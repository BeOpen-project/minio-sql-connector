const service = require("../services/service.js")
const common = require("../../utils/common.js")
module.exports = {

    querySQL: async (req, res) => {
        console.log("Query sql")
        const requestData = req.body.query;
        console.log(requestData)
        service.querySQL(res, req.body.query, req.body.prefix, req.body.bucketName, req.headers.visibility)
    },

    queryMongo: async (req, res) => {
        console.log("Query mongo")
        console.log(req.query, req.headers.visibility)
        //if (common.isRawQuery(req.query))
        //return res.send(await service.rawQuery(req.query, req.body.prefix, req.body.bucketName, req.headers.visibility))
        let queryRaw = JSON.parse(JSON.stringify(req.query))
        queryRaw.format = "Object"
        let queryMongo = JSON.parse(JSON.stringify(req.query))
        queryMongo.format = "JSON"
        let rawQuery = await service.rawQuery(queryRaw, req.body.prefix, req.body.bucketName, req.headers.visibility)
        if (rawQuery && !Array.isArray(rawQuery))
            rawQuery = [rawQuery]
        let mongoQuery = await service.mongoQuery(queryMongo, req.body.prefix, req.body.bucketName, req.headers.visibility)
        if (mongoQuery && !Array.isArray(mongoQuery))
            mongoQuery = [mongoQuery]
        if (rawQuery && mongoQuery)
            res.send(rawQuery.concat(mongoQuery))
        else
            res.send(rawQuery || mongoQuery)
        console.log("Query mongo finished")
    },

    sync: async (req, res) => {
        console.log("Sync")
        return res.send(await service.sync())
    },

    minioListObjects: async (req, res) => {
        try {
            res.send(await service.minioListObjects(req.params.bucketName || req.query.bucketName))
        }
        catch (error) {
            console.error(error)
            res.status(500).send(error.toString() == "[object Object]" ? error : error.toString())
        }
    },
}