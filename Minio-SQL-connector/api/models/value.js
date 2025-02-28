const mongoose = require("mongoose");

const value = mongoose.Schema({}, { strict: false });   

module.exports = mongoose.model("value", value);