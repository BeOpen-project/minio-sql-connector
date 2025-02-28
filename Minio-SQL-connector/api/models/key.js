const mongoose = require("mongoose");

const key = mongoose.Schema({}, { strict: false });   

module.exports = mongoose.model("key", key);