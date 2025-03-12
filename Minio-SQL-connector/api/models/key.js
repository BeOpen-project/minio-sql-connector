const mongoose = require("mongoose");

const key = mongoose.Schema(
  {
    key: { type: String, required: true },
    //name: { type: String, required: true }//,
    visibility: { type: Array, required: true },
    //group: { type: String, required: true }
  },
  { versionKey: false }
);

module.exports = mongoose.model("key", key);