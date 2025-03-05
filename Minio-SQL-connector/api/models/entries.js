const mongoose = require("mongoose");

const key = mongoose.Schema({
    key: { type: String, required: true},
    value: { type: mongoose.Schema.Types.Mixed, required: true},
}, { versionKey: false });   

module.exports = mongoose.model("entries", key);