const balluffRfid = require("../lib/index.js");

const conn = balluffRfid.connect({
    ipAddress: "192.168.10.2",
    port: 10003,

    onConnect: () => {
        console.log("connected!");

        const claimedPin = conn.claimPinAsDigitalInput({ pinNumber: 2 });
        const pollHandle = claimedPin.poll({
            onRead: ({ error, value }) => {
                console.log("read digital input", { error, value });
            },
            pollIntervalMs: 1000
        });

        setTimeout(() => {
            console.log("closing after 10s");
            pollHandle.stop();
            claimedPin.release();
            conn.close();
        }, 10000);
    },

    onError: (err) => {
        console.error("balluff error", err);
        conn.close();
    }
});
