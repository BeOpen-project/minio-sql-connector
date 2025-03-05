const mongoose = require("mongoose");

const key = mongoose.Schema({
    key: { type: String, required: true}}, {versionKey: false }
  ); 

module.exports = mongoose.model("key", key);