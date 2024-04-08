const balluff007 = require('@autexis/balluff-007-protocol');

const simpleDeepEqual = ({ a, b }) => {
    if (a === undefined || b === undefined) {
        return a === b;
    }

    const keysA = Object.keys(a);
    const keysB = Object.keys(b);

    if (keysA.length !== keysB.length) {
        return false;
    }

    return keysA.every((key) => {
        return a[key] === b[key];
    });
};

const promiseQueue = () => {
    let queue = [];

    let running = false;

    const maybeNext = () => {
        if (running) {
            return;
        }

        if (queue.length === 0) {
            return;
        }

        running = true;

        const { f, resolve, reject } = queue[0];
        queue = queue.slice(1);

        Promise.resolve().then(() => {
            return f();
        }).then((result) => {
            running = false;
            resolve(result);
            maybeNext();
        }, (err) => {
            running = false;
            reject(err);
            maybeNext();
        });
    };

    const add = (f) => {
        return new Promise((resolve, reject) => {
            queue = [
                ...queue,
                { f, resolve, reject }
            ];

            maybeNext();
        });
    };

    return {
        add
    };
};

const connect = ({ ipAddress, port, onConnect, onError }) => {

    let closed = false;
    let errored = false;

    const fail = ({ error }) => {
        if (closed) {
            throw Error("already closed");
        }

        if (errored) {
            throw Error("already errored");
        }

        // if we still have a connection, close it
        if (conn !== undefined) {
            conn.close();
        }

        errored = true;
        onError(error);
    };

    let conn = balluff007.connect({
        ipAddress,
        port,

        onConnect: () => {

            try {
                onConnect();
            } catch (ex) {
                console.error("listener exception", ex);
            }

            requestScanSlot(async () => {
                let error = undefined;

                try {
                    const { statusCode: sc1 } = await conn.requestChangeAsyncOperationParameter({
                        antennaNumber: 0,
                        asynchronous: false,
                        comesMessage: false,
                        goesMessage: false,
                        cumulate: false
                    });

                    if (sc1 !== balluff007.statusCodes.OK) {
                        error = balluff007.errorFromStatusCode({ statusCode: sc1 });
                    }

                } catch (err) {
                    error = err;
                }

                if (closed || errored) {
                    return;
                }

                if (error !== undefined) {
                    console.error("initial scan stop failed", error);
                }
            });
        },

        onError: (error) => {
            conn = undefined;
            fail({ error });
        }
    });

    let claimedDigitalInputPins = [];
    let claimedIoLink = undefined;

    let activeIoLinkMasterConfig = undefined;

    const determineRequiredIoLinkMasterConfig = () => {
        // for now, we always use normally open input
        const pin2Mode = 0;

        let pin4Mode = claimedIoLink ? 4 : 0;

        const wantsPin4AsDigitalInput = claimedDigitalInputPins.some((p) => {
            return p.pinNumber === 4;
        });

        if (wantsPin4AsDigitalInput) {
            // always configure normally open
            pin4Mode = 0;
        }

        const ioLinkDefaults = {
            cycleTimeBase: 0,
            cycleTime: 0,
            safeState: 0,
            validationMode: 0,
            vendorId: 0,
            deviceId: 0,
            outputLength: 0,
            inputLength: 0,
            serialNumber: "0000000000000000"
        };

        const ioLink = claimedIoLink || ioLinkDefaults;

        return {
            ...ioLink,
            pin4Mode,
            pin2Mode,
            parameterServerMode: 0,
            parameterUploadEnabled: false,
            parameterDownloadEnabled: false,
            serialNumber: "0000000000000000"
        };
    };

    const configQueue = promiseQueue();

    const maybeReconfigure = () => {
        return configQueue.add(async () => {
            if (closed) {
                return {};
            }

            let requiredIoLinkMasterConfig = determineRequiredIoLinkMasterConfig();
            let correctConfigActive = simpleDeepEqual({ a: activeIoLinkMasterConfig, b: requiredIoLinkMasterConfig });

            if (correctConfigActive) {
                return {}
            }

            // if we are going to reconfigure, we wait a bit
            // in order to avoid reconfiguring more than once
            await new Promise((resolve) => setTimeout(resolve, 200));

            if (closed) {
                return {};
            }

            // as config writes causes reconfiguration, we only write the config
            // if it actually changed, so we read the current config first
            const { statusCode: sc1, ioLinkMasterConfig: lowerActiveIoLinkMasterConfig } = await conn.readIoLinkMasterConfig();
            if (sc1 !== balluff007.statusCodes.OK) {
                return {
                    error: Error(`failed to read io link master config`, { cause: balluff007.errorFromStatusCode({ statusCode: sc1 }) })
                };
            }

            activeIoLinkMasterConfig = lowerActiveIoLinkMasterConfig;

            // redetermine the required config, as it might have changed
            requiredIoLinkMasterConfig = determineRequiredIoLinkMasterConfig();
            correctConfigActive = simpleDeepEqual({ a: activeIoLinkMasterConfig, b: requiredIoLinkMasterConfig });

            if (correctConfigActive) {
                console.log("lower active config is correct", { activeIoLinkMasterConfig, requiredIoLinkMasterConfig });

                return {};
            }

            console.log("lower active config differs, writing new config", { activeIoLinkMasterConfig, requiredIoLinkMasterConfig });

            const { statusCode: sc2 } = await conn.writeIoLinkMasterConfig({ ioLinkMasterConfig: requiredIoLinkMasterConfig });

            if (closed) {
                return {};
            }

            // HACK: workaround, as sometimes connection always returns ACCESS_DENIED
            // in this case we close the underlying connection and report an error
            if (sc2 === balluff007.statusCodes.ACCESS_DENIED) {
                const error = Error("connection returns ACCESS_DENIED, closing connection");
                fail({ error });

                return {
                    error
                };
            } else if (sc2 !== balluff007.statusCodes.OK) {
                return {
                    error: Error(`failed to write io link master config`, { cause: balluff007.errorFromStatusCode({ statusCode: sc2 }) })
                };
            }

            activeIoLinkMasterConfig = requiredIoLinkMasterConfig;

            return {};
        });
    };

    const maybeTriggerReconfiguration = () => {
        maybeReconfigure().then(({ error }) => {
            return { error };
        }, (err) => {
            return { error: err };
        }).then(({ error }) => {
            if (closed || errored) {
                return;
            }

            // only log and ignore
            if (error !== undefined) {
                console.error("configuration failed", error);
            }
        });
    };

    const claimPinAsDigitalInput = ({ pinNumber }) => {

        if (pinNumber === 4 && claimIoLink !== undefined) {
            throw new Error("pin 4 already claimed as io link");
        }

        const handle = {
            pinNumber
        };

        claimedDigitalInputPins = [
            ...claimedDigitalInputPins,
            handle
        ];

        maybeTriggerReconfiguration();

        const poll = ({ onRead, pollIntervalMs }) => {

            let stopped = false;
            let pollIntervalHandle = undefined;

            const next = () => {
                if (closed) {
                    return;
                }

                if (stopped) {
                    return;
                }

                Promise.resolve().then(async () => {
                    const { error: e1 } = await maybeReconfigure();

                    if (closed || stopped) {
                        return {};
                    }

                    if (e1) {
                        return { error: e1 };
                    }

                    const { statusCode: sc2, value } = await conn.readDigitalInputPin({ pinNumber });

                    if (closed || stopped) {
                        return {};
                    }

                    if (sc2 !== balluff007.statusCodes.OK) {
                        return {
                            error: balluff007.errorFromStatusCode({ statusCode: sc2 })
                        };
                    }

                    return {
                        value
                    };

                }).then(({ error, value }) => {
                    return { error, value };
                }, (err) => {
                    return { error: err };
                }).then(({ error, value }) => {
                    if (closed || stopped) {
                        return;
                    }

                    try {
                        onRead({ error, value });
                    } catch (ex) {
                        console.error("listener execption", ex);
                    }

                    pollIntervalHandle = setTimeout(() => {
                        next();
                    }, pollIntervalMs);
                });
            };

            pollIntervalHandle = setTimeout(() => {
                next();
            }, 0);

            const stop = () => {
                stopped = true;

                clearTimeout(pollIntervalHandle);
                pollIntervalHandle = undefined;
            };

            return {
                stop
            };
        };

        const release = () => {
            claimedDigitalInputPins = claimedDigitalInputPins.filter((p) => p !== handle);

            maybeTriggerReconfiguration();
        };

        return {
            poll,

            release
        };
    };

    const claimIoLink = ({
        cycleTimeBase,
        cycleTime,
        safeState,
        validationMode,
        vendorId,
        deviceId,
        outputLength,
        inputLength,
    }) => {

        const wantsPin4AsDigitalInput = claimedDigitalInputPins.some((p) => {
            return p.pinNumber === 4;
        });

        if (wantsPin4AsDigitalInput) {
            throw new Error("pin 4 already claimed as digital input");
        }

        if (claimedIoLink !== undefined) {
            throw new Error("io link already claimed");
        }

        const handle = {
            cycleTimeBase,
            cycleTime,
            safeState,
            validationMode,
            vendorId,
            deviceId,
            outputLength,
            inputLength,
        };

        claimedIoLink = handle;
        maybeTriggerReconfiguration();

        const writeCyclicProcessData = async ({ offset, data }) => {
            if (claimedIoLink !== handle) {
                throw new Error("io link already released");
            }

            if (data.length !== outputLength) {
                throw new Error("invalid data length");
            }

            const { error: e1 } = await maybeReconfigure();
            if (e1 !== undefined) {
                return {
                    error: Error("configuration failed", { cause: e1 })
                };
            }

            const { statusCode: sc1 } = await conn.writeIoLinkCyclicProcessData({
                offset,
                data
            });

            if (sc1 !== balluff007.statusCodes.OK) {
                return {
                    error: Error("write io link process data failed", { cause: balluff007.errorFromStatusCode({ statusCode: sc1 }) })
                };
            }

            return {};
        };

        const release = () => {
            claimedIoLink = undefined;
            maybeTriggerReconfiguration();
        };

        return {
            writeCyclicProcessData,

            release
        };
    };

    let scanInProgress = false;

    const requestScanSlot = async (fn) => {
        if (conn === undefined) {
            throw Error("not connected");
        }

        if (scanInProgress) {
            throw new Error("scan already in progress");
        }

        scanInProgress = true;

        try {
            return await fn();
        } finally {
            scanInProgress = false;
        }
    };

    const detectOrGetCarriers = async ({ dataType, maxNumberCarriers = 999 }) => {
        if (conn === undefined) {
            throw Error("not connected");
        }

        const { statusCode, detectedCarriers } = await conn.detectDataCarriersExtended({
            antennaNumber: 0,
            dataType,
            maxNumberCarriers,
            onlySelected: false
        });

        if (statusCode === balluff007.statusCodes.NO_CARRIER_IN_RANGE) {
            return {
                detectedCarriers: []
            };
        } else if (statusCode !== balluff007.statusCodes.OK) {
            return {
                error: balluff007.errorFromStatusCode({ statusCode })
            }
        }

        return {
            detectedCarriers
        };
    };

    const requestCumulatedModeAndFlush = async () => {
        const { statusCode: sc1 } = await conn.requestChangeAsyncOperationParameter({
            antennaNumber: 0,
            asynchronous: false,
            comesMessage: false,
            goesMessage: false,
            cumulate: false
        });

        if (sc1 !== balluff007.statusCodes.OK) {
            return {
                error: Error(`flushing list / going to live mode failed`, { cause: balluff007.errorFromStatusCode({ statusCode: sc1 }) })
            };
        }

        const { statusCode: sc2 } = await conn.requestChangeAsyncOperationParameter({
            antennaNumber: 0,
            asynchronous: true,
            comesMessage: false,
            goesMessage: false,
            cumulate: true
        });

        if (sc2 !== balluff007.statusCodes.OK) {
            return {
                error: Error(`going to cumulated mode failed`, { cause: balluff007.errorFromStatusCode({ statusCode: sc2 }) })
            };
        }

        return {};
    };

    const requestLiveScanMode = async () => {
        const { statusCode: sc1 } = await conn.requestChangeAsyncOperationParameter({
            antennaNumber: 0,
            asynchronous: false,
            comesMessage: false,
            goesMessage: false,
            cumulate: false
        });

        if (sc1 !== balluff007.statusCodes.OK) {
            return {
                error: balluff007.errorFromStatusCode({ statusCode: sc1 })
            };
        }

        return {};
    };

    const detectCarriersLive = async ({ dataType }) => {
        return await requestScanSlot(async () => {
            const { error: e1 } = requestLiveScanMode();
            if (e1 !== undefined) {
                return {
                    error: Error("failed to request live scan mode", { cause: e1 })
                };
            }

            const { error: e2, detectedCarriers } = await detectOrGetCarriers({ dataType });
            if (e2 !== undefined) {
                return {
                    error: Error("failed to detect carriers", { cause: e2 })
                };
            }

            return {
                carriers: detectedCarriers
            };
        });
    };

    const detectCarriersCumulated = async ({ dataType, scanTimeMs }) => {
        return await requestScanSlot(async () => {

            const { error: e1 } = await requestCumulatedModeAndFlush();
            if (e1 !== undefined) {
                return {
                    error: Error("failed to request cumulated mode and flush", { cause: e1 })
                };
            }

            await new Promise((resolve) => {
                setTimeout(() => {
                    resolve();
                }, scanTimeMs);
            });

            const { error: e2, detectedCarriers } = await detectOrGetCarriers({ dataType });
            if (e2 !== undefined) {
                return {
                    error: Error("failed to get carriers", { cause: e2 })
                }
            }

            const { error: e3 } = await requestLiveScanMode();
            if (e3 !== undefined) {
                return {
                    error: Error("failed to request live scan mode (reset)", { cause: e3 })
                };
            }

            return {
                carriers: detectedCarriers
            };
        });
    };

    const scanCarriersCumulated = ({ dataType, maxCarriersPerRequest = 12, requestIntervalMs = 500, onScan, onError }) => {

        if (scanInProgress) {
            throw new Error("scan already in progress");
        }

        scanInProgress = true;

        let stopped = false;
        let scanErrored = false;

        const main = async() => {
            const { error: e1 } = await requestCumulatedModeAndFlush();
            if (e1 !== undefined) {
                return {
                    error: Error("failed to request cumulated mode and flush", { cause: e1 })
                };
            }

            let error = undefined;

            while (!stopped && error === undefined) {
                await new Promise((resolve) => setTimeout(resolve, requestIntervalMs));

                if (stopped) {
                    continue;
                }

                const { error: e2, detectedCarriers } = await detectOrGetCarriers({ dataType, maxNumberCarriers: maxCarriersPerRequest });
                if (e2 !== undefined) {
                    error = Error("failed to get carriers", { cause: e2 });
                    continue;
                }

                if (stopped) {
                    continue;
                }

                try {
                    onScan({ carriers: detectedCarriers });
                } catch (ex) {
                    console.error("listener exception", ex);
                }
            }

            const { error: e3 } = await requestLiveScanMode();
            if (e3 !== undefined) {
                const liveScanModeError = Error("failed to request live scan mode (reset)", { cause: e3 });

                if (error) {
                    // if we already have an error, only log
                    // this cleanup error but not return it as main error
                    console.error(liveScanModeError);
                } else {
                    error = liveScanModeError;
                }
            }

            return {
                error
            };
        };

        main().then(({ error }) => {
            return { error };
        }, (error) => {
            return { error };
        }).then(({ error }) => {
            scanInProgress = false;

            if (error !== undefined && !stopped) {
                scanErrored = true;
                onError(error);
            }
        });

        const stop = () => {
            if (stopped) {
                throw Error("already stopped");
            }

            if (scanErrored) {
                throw Error("already errored");
            }

            stopped = true;
        };

        return {
            stop
        };
    };

    const close = () => {
        closed = true;

        conn?.close();
        conn = undefined;
    };

    return {
        claimPinAsDigitalInput,
        claimIoLink,

        detectCarriersLive,
        detectCarriersCumulated,
        scanCarriersCumulated,

        close
    };
};

module.exports = {
    connect,
    statusCodes: balluff007.statusCodes,
    errorFromStatusCode: balluff007.errorFromStatusCode
};
