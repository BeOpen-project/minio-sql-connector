const mongoose = require("mongoose");

const source = mongoose.Schema({}, { strict: false });   

module.exports = mongoose.model("source", source);