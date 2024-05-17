const Source = require('../source')
const common = require('../common')

module.exports = {

    async exampleQueryCSV(query) {

        return await Source.find({
            "csv": {
                $elemMatch: query
            }
        })
    },

    async exampleQueryJson(query) {

        return await Source.find({
            "json": {
                $elemMatch: query
            }
        })
    },

    async exampleQueryGeoJson(query) {

        let key

        for (let k in query)
            key = k

        console.debug(query[key])

        return await Source.find({
            "features": {
                $elemMatch: {
                    //"a": {
                    "geometry.coordinates": {
                        $elemMatch: {
                            $elemMatch: {

                                $elemMatch: {
                                    $elemMatch: { $eq: Number(query[key]) }
                                }
                            }
                        }
                    }
                }
            }
        })
    },

    async mongoQuery(query) {
        let format = query.format?.toLowerCase()
        if (format)
            delete query["format"]
        switch (format) {
            case "geojson": return await this.exampleQueryGeoJson(query)
            case "csv": return await this.exampleQueryCSV(query)
            case "json": return this.exampleQueryJson(query)
            case "object": return await Source.find(query)
        }
    }
}