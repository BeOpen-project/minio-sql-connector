// A global reset config middleware is configured for all endpoints in server.js

const express = require("express")
const controller = require("../controllers/controller.js")
const router = express.Router()
//const multer = require('multer');
//const upload = multer();
const { auth } = require("../middlewares/auth.js")
const {bodyCheck} = require('../../utils/common.js')

router.post(encodeURI("/query"), auth, bodyCheck, controller.query)//, controller.queryMongo)
router.get(encodeURI("/query"), auth, controller.queryMongo)
router.get(encodeURI("/minio/listObjects"), auth, controller.minioListObjects)
router.put(encodeURI("/query"), auth, controller.sync)

module.exports = router
