const service = require("../services/service.js")
module.exports = {

    querySQL: async (req, res) => {

        const requestData = req.body.query;
        console.log(requestData)
        service.querySQL(res, req.body.query, req.body.prefix)
    },

    queryMongo: async (req, res) => {
        console.log(req.query)
        res.send(await service.mongoQuery(req.query, req.body.prefix))
    }
}