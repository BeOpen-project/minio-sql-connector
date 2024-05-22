// A global reset config middleware is configured for all endpoints in server.js

const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
//const multer = require('multer');
//const upload = multer();
const { auth } = require("../middlewares/auth.js")

router.post(encodeURI("/query"), auth, controller.querySQL)
router.get(encodeURI("/query"), auth, controller.queryMongo)

module.exports = router
