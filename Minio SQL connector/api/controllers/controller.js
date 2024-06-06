const service = require("../services/service.js")
const common = require("../../utils/common.js")
module.exports = {

    querySQL: async (req, res) => {

        const requestData = req.body.query;
        console.log(requestData)
        service.querySQL(res, req.body.query, req.body.prefix, req.body.bucketName)
    },

    queryMongo: async (req, res) => {
        console.log(req.query, req.headers.visibility)
        if (common.isRawQuery(req.query))
            return res.send(await service.rawQuery(req.query, req.body.prefix, req.body.bucketName))
        res.send(await service.mongoQuery(req.query, req.body.prefix, req.body.bucketName))
    }
}