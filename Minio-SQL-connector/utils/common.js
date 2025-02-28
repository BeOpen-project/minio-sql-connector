//const Papa = require("papaparse");
const Log = require('./logger')//.app(module);
const { Logger } = Log
const logger = new Logger("miniowriter")
const log = logger.info

function objectCheck(objs) {
  for (let obj of objs)
    for (let key in obj)
      try {
        let valueParsed = JSON.parse(obj[key])
        obj[key] = valueParsed
      }
      catch (error) {
        console.error(error)
      }

}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function convertCSVtoJSON(csvData) {
  console.debug(csvData)
  const lines = csvData.split('\r\n');
  //console.debug(this.minify(lines))
  const possibleHeaders = [
    lines[0].trim().split(','),
    lines[0].trim().split(';')
  ]
  //console.debug(this.minify(possibleHeaders))
  const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
  const results = [];
  //console.debug(this.minify(headers))

  //for (let i = 1; i < lines.length - 1 || i < 2 ; i++) {
  for (let i = 1; i < lines.length; i++) {
    const obj = {};
    const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
    //console.debug(this.minify(currentLine))
    for (let j = 0; j < headers.length; j++)
      obj[this.deleteSpaces(headers[j].replaceAll(/['"]/g, ''))] = this.deleteSpaces(currentLine[j]?.replaceAll(/['"]/g, ''));
    results.push(obj);
    //console.debug(this.minify(obj))
  }
  //return results
  //console.debug("convert csv to json")
  //console.debug(this.minify(results))
  return JSON.stringify(results);
}

module.exports = {

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  },

  minify(obj) {
    try {
      if (typeof obj == "string")
        return obj.substring(0, 10).concat(" ...")
      else if (Array.isArray(obj) || typeof obj == "object")
        return JSON.stringify(obj).substring(0, 10).concat(" ...")
      return obj
    }
    catch (error) {
      console.error(error.toString())
      return obj
    }
  },

  async getEntries(obj, type) {// csv, jsonArray, json
    let csvParsed, logCounterFlag
    let delays = 10
    if (!obj[0].csv && Array.isArray(obj[0].json) && type != "jsonArray")
      type = "jsonArray" //throw new Error("obj is a jsonArray and not " + type)
    else if ((!obj[0].csv && !Array.isArray(obj[0].json) && typeof obj == "object") && type != "json")
      //if (obj[0].features)
      type = "json" //throw new Error("obj is a json and not " + type)
    else if (obj[0].csv && type != "csv")
      type = "csv"//throw new Error("obj is a csv and not " + type)
    if (type == "json") {
      if (obj[0].features)
        obj = [{ json: obj[0].features }]
      else{
        logger.debug(obj[0])
        return Object.entries(obj[0]).map(arr => ({key : arr[0], value : arr[1]}))
      }
      /*return {
        keys: Object.keys(obj[0]).map(k => ({ key: k })),
        values: Object.values(obj[0]).map(v => ({ value: v }))
      }*/
      logger.debug("so it was a geojson")
      //await sleep(100)
    }
    logger.debug("Here's obj before flatmap")
    logger.debug(JSON.stringify(obj).substring(0, 30))
    //await sleep(100)
    obj = obj[0].json || obj[0].csv
    if (obj[0].properties)
      obj = obj.map(o => o.properties)
    let entries = []
    for (let o of obj)
      entries.push(...Object.entries(o))

    /*
    obj = obj[0].json.flatMap(o => 
      Object.entries(o.properties).map(([key, value]) => ({ [key]: value }))
    );*/


    logger.debug("Here's obj after flatmap or custom cose")
    logger.debug(JSON.stringify(obj).substring(0, 30))
    //await sleep(100)
    logger.debug()
    return entries.map(arr => ({key : arr[0], value : arr[1]}))
    /*return {
      keys: [...new Set(obj[0].json.flatMap(o => Object.keys(o)))].map(k => ({ key: k })),
      values: [...new Set(obj[0].json.flatMap(o => Object.keys(o)))].map(v => ({ value: v }))
    }*/
  },

  async setType(extension, jsonParsed) {
    //extension == "csv", Array.isArray(jsonParsed), typeof jsonParsed == "object"
    console.debug("csv ", extension == "csv", " array ", Array.isArray(jsonParsed), " object ", typeof jsonParsed == "object", " jsonparsed ", jsonParsed)
    //await sleep(100)
    return extension == "csv" ?
      "csv" :
      Array.isArray(jsonParsed) ?
        "jsonArray" :
        typeof jsonParsed == "object" ?
          "json" :
          "raw"
  },

  json2csv(obj) {
    return JSON.stringify([obj])
    console.debug("json2csv")
    let csv = ""

    for (let key in obj)
      csv = csv + key + ";"
    csv = [csv.substring(0, csv.length - 1)]

    csv[1] = ""

    for (let key in obj)
      csv[1] = csv[1].toString() + obj[key].toString() + ";"
    csv[1] = csv[1].substring(0, csv.length - 1)

    return csv

    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(csv);
        controller.close();
      }
    });

    return stream
  },

  parseJwt(token) {
    return JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
  },

  urlEncode(bucket) {
    return bucket.replaceAll("-", "")
  },

  deleteSpaces(obj) {
    if (obj) {
      while (obj[0] == " ")
        obj = obj.substring(1)
      while (obj[obj.length - 1] == " ")
        obj = obj.substring(0, obj.length - 1)
    }
    return obj
  },

  convertCSVtoJSON(csvData) {
    //console.debug(this.minify(csvData))
    const lines = csvData.split('\r\n');
    //console.debug(this.minify(lines))
    const possibleHeaders = [
      lines[0].trim().split(','),
      lines[0].trim().split(';')
    ]
    //console.debug(this.minify(possibleHeaders))
    const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
    const results = [];
    //console.debug(this.minify(headers))

    //for (let i = 1; i < lines.length - 1 || i < 2 ; i++) {
    for (let i = 1; i < lines.length; i++) {
      const obj = {};
      const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
      //console.debug(this.minify(currentLine))
      for (let j = 0; j < headers.length; j++)
        obj[this.deleteSpaces(headers[j].replaceAll(/['"]/g, ''))] = this.deleteSpaces(currentLine[j]?.replaceAll(/['"]/g, ''));
      results.push(obj);
      //console.debug(this.minify(obj))
    }
    //return results
    //console.debug("convert csv to json")
    //console.debug(this.minify(results))
    return JSON.stringify(results);
  },

  cleaned(obj) {
    //console.log(typeof obj != "string" ? JSON.stringify(obj) : obj)
    //return obj
    //return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['"\r\n\s]/g, ''); /['"\r\n]/g /['"]/g
    //return JSON.stringify(JSON.parse((typeof obj != "string" ? JSON.stringify(obj) : obj).replace( /['\r\n]/g, '')));
    return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['\r\n]/g, '')//.replaceAll('"\"a\""', '"a"') //.replaceAll('"\\"', '"').replaceAll('\\""', '"');
  },

  /*
  isRawQuery(obj) {
    const keys = Object.keys(obj);
    if (keys.length !== 1)
      return false;
    return obj.value;
  },
  */

  checkConfig(configIn) {

    if (!configIn.queryAllowedExtensions) configIn.queryAllowedExtensions = ["csv", "json", "geojson"]
    return configIn
  },

  bodyCheck: async (req, res, next) => {
    if (req?.body?.mongoQuery && (
      Object.keys(req?.body?.mongoQuery).length == 1 && req.body.mongoQuery[''] == '' ||
      req.body.mongoQuery[''] == '{"$gte":null,"$lte":null}') || !req.body || !req.body.mongoQuery)
      req.body.mongoQuery = req.query
    objectCheck([req.body.mongoQuery, req.query])
    next()
  }
}