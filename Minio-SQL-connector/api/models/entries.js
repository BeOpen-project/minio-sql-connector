const mongoose = require("mongoose");

const key = mongoose.Schema(
    {
        key: { type: String, required: true },
        value: { type: mongoose.Schema.Types.Mixed, required: true },
        name: { type: String, required: true }//,
        //visibility: { type: String, required: true },
        //group: { type: String, required: true }
    },
    { versionKey: false }
);

module.exports = mongoose.model("entries", key);