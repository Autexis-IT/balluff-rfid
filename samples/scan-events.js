const balluffRfid = require("../lib/index.js");

const conn = balluffRfid.connect({
    ipAddress: "192.168.10.2",
    port: 10003,

    onConnect: () => {
        console.log("connected!");

        setTimeout(() => {
            console.log("starting scan...");

            const scanHandle = conn.scanCarriersCumulated({
                dataType: "E",
                maxCarriersPerRequest: 999,
                requestIntervalMs: 10,

                onScan: ({ carriers }) => {
                    console.log("scan event", carriers);
                },

                onError: (error) => {
                    console.error("scan error", error);
                    clearTimeout(stopTimeoutHandle);
                }
            });

            const stopTimeoutHandle = setTimeout(() => {
                scanHandle.stop();
                conn.close();
            }, 2000);
        }, 1000);
    },

    onError: (err) => {
        console.error("balluff error", err);
        conn.close();
    }
});
