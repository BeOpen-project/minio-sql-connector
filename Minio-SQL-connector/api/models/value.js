const mongoose = require("mongoose");

const value = mongoose.Schema({
    value: { type: mongoose.Schema.Types.Mixed, required: true}
}, { versionKey: false });   

module.exports = mongoose.model("value", value);