const mongoose = require("mongoose");

const value = mongoose.Schema(
    {
        value: { type: mongoose.Schema.Types.Mixed, required: true },
        name: { type: String, required: true }//,
        //visibility: { type: String, required: true },
        //group: { type: String, required: true }
    },
    { versionKey: false }
);

module.exports = mongoose.model("value", value);