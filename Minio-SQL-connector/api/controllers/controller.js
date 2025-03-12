const service = require("../services/service.js")
const common = require("../../utils/common.js")
const Log = require('../../utils/logger.js')//.app(module);
const { Logger } = Log
const logger = new Logger("controller")


const queryMongo = async (req, res) => {
    if (req.headers.israwquery)
        return await res.send(await service.rawQuery(req.query, req.body.prefix, req.body.bucketName, req.headers.visibility)) && logger.log("Raw query finished")
    logger.log("Query mongo")
    logger.debug("format ", req.query.format)
    if (req.query.format == "JSON") {
        let objectQuerySet = JSON.parse(JSON.stringify(req.body.mongoQuery || req.query))
        objectQuerySet.format = "Object"
        let JSONQuerySet = JSON.parse(JSON.stringify(req.body.mongoQuery || req.query))
        JSONQuerySet.format = "JSON"
        let objectQuery = await service.mongoQuery(objectQuerySet, req.body.prefix, req.body.bucketName, req.headers.visibility)
        if (objectQuery && !Array.isArray(objectQuery))
            objectQuery = [objectQuery]
        let JSONQuery = await service.mongoQuery(JSONQuerySet, req.body.prefix, req.body.bucketName, req.headers.visibility)
        if (JSONQuery && !Array.isArray(JSONQuery))
            JSONQuery = [JSONQuery]
        if (JSONQuery && objectQuery)
            res.send(JSONQuery.concat(objectQuery))
        else
            res.send(JSONQuery || objectQuery)
    }
    else
        res.send(await service.mongoQuery({ ...req.body.mongoQuery, ...req.query }, req.body.prefix, req.body.bucketName, req.headers.visibility))
    logger.log("Query mongo finished")

}

const querySQL = async (req, res) => {
    logger.log("Query sql")
    if (!req.body.query)
        return await res.status(400).send("Missing query")
    logger.log("Query : ", req.body.query)
    service.querySQL(res, req.body.query, req.body.prefix, req.body.bucketName, req.headers.visibility)
}

module.exports = {

    queryMongo, querySQL,

    query: async (req, res) => {
        logger.log("Query: \n", req.query, "\n","Body : \n", req.body)
        if (req.body.mongoQuery)
            return await queryMongo(req, res)
        querySQL(req, res)
    },

    getValues: async (req, res) => {
        logger.log("values")
        try {
            res.send(await service.getValues())
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    getEntries: async (req, res) => {
        logger.log("values")
        try {
            res.send(await service.getEntries())
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    getKeys: async (req, res) => {
        logger.log("keys")
        try {
            res.send(await service.getKeys())
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },

    sync: async (req, res) => {
        logger.log("Sync")
        return await res.send(await service.sync())
    },

    minioListObjects: async (req, res) => {
        try {
            res.send(await service.minioListObjects(req.params.bucketName || req.query.bucketName))
        }
        catch (error) {
            logger.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },
}