﻿//const Papa = require("papaparse");
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
        logger.error(error)
      }

}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function stringify(item) {
  if (typeof item != "string")
    return JSON.stringify(item)
  return item
}

function convertCSVtoJSON(csvData) {
  logger.debug(csvData)
  const lines = csvData.split('\r\n');
  //logger.debug(this.minify(lines))
  const possibleHeaders = [
    lines[0].trim().split(','),
    lines[0].trim().split(';')
  ]
  //logger.debug(this.minify(possibleHeaders))
  const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
  const results = [];
  //logger.debug(this.minify(headers))

  //for (let i = 1; i < lines.length - 1 || i < 2 ; i++) {
  for (let i = 1; i < lines.length; i++) {
    const obj = {};
    const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
    //logger.debug(this.minify(currentLine))
    for (let j = 0; j < headers.length; j++)
      obj[this.deleteSpaces(headers[j].replaceAll(/['"]/g, ''))] = this.deleteSpaces(currentLine[j]?.replaceAll(/['"]/g, ''));
    results.push(obj);
    //logger.debug(this.minify(obj))
  }
  //return results
  //logger.debug("convert csv to json")
  //logger.debug(this.minify(results))
  return JSON.stringify(results);
}

function getVisibility(name) {

  name = name.split("/")
  if (name[0].includes("@") || (name[0].toLowerCase().includes("shared data")))
    return name[0]
  return "public-data"
}

function syncEntries(obj, visibility, entries) {
  for (let key in obj)
    if (!entries[key])
      entries[key] = { [stringify(obj[key])]: [visibility] }
    else if (!entries[key][stringify(obj[key])])
      entries[key][stringify(obj[key])] = [visibility]
    else if (!entries[key][stringify(obj[key])].includes(visibility))
      entries[key][stringify(obj[key])].push(visibility)
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
      logger.error(error.toString())
      return obj
    }
  },

  async getEntries(obj, type, name, entries) {// csv, jsonArray, json
    let csvParsed, logCounterFlag
    let delays = 10
    let visibility = getVisibility(name)
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
      else {
        logger.trace(obj[0])
        syncEntries(obj[0], visibility, entries)
        /*
        for (let key in obj[0])
          if (!entries[key]) {
            entries[key] = {}
            entries[key][stringify(obj[0][key])] = [visibility]
          }
          else
            entries[key][stringify(obj[0][key])].push(visibility)*/
        return
        //return Object.entries(obj[0]).map(arr => ({ key: arr[0], value: arr[1], visibility }))
      }
      /*return {
        keys: Object.keys(obj[0]).map(k => ({ key: k })),
        values: Object.values(obj[0]).map(v => ({ value: v }))
      }*/
      logger.trace("so it was a geojson")
      //await sleep(100)
    }
    logger.trace("Here's obj before flatmap")
    logger.trace(JSON.stringify(obj).substring(0, 30))
    //await sleep(100)
    obj = obj[0].json || obj[0].csv
    if (obj[0] && obj[0].properties)
      obj = obj.map(o => o.properties)
    //let entries = []
    for (let o of obj)
      syncEntries(o, visibility, entries)
    /*
    for (let key in o)
      if (!entries[key])
        entries[key] = { [stringify(o[key])]: [visibility] }
      else if (!entries[key][stringify(o[key])])
        entries[key][stringify(o[key])] = [visibility]
      else
        entries[key][stringify(o[key])].push(visibility)*/
    return

    //entries.push(...Object.entries(o))

    /*
    obj = obj[0].json.flatMap(o => 
      Object.entries(o.properties).map(([key, value]) => ({ [key]: value }))
    );*/


    logger.trace("Here's obj after flatmap or custom cose")
    logger.trace(JSON.stringify(obj).substring(0, 30))
    //await sleep(100)

    return entries.map(arr => ({ key: arr[0], value: arr[1], visibility }))
    /*return {
      keys: [...new Set(obj[0].json.flatMap(o => Object.keys(o)))].map(k => ({ key: k })),
      values: [...new Set(obj[0].json.flatMap(o => Object.keys(o)))].map(v => ({ value: v }))
    }*/
  },

  async setType(extension, jsonParsed) {
    //extension == "csv", Array.isArray(jsonParsed), typeof jsonParsed == "object"
    logger.debug("csv ", extension == "csv", " array ", Array.isArray(jsonParsed), " object ", typeof jsonParsed == "object", " jsonparsed ", jsonParsed)
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
    logger.debug("json2csv")
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
    //logger.debug(this.minify(csvData))
    const lines = csvData.split('\r\n');
    //logger.debug(this.minify(lines))
    const possibleHeaders = [
      lines[0].trim().split(','),
      lines[0].trim().split(';')
    ]
    //logger.debug(this.minify(possibleHeaders))
    const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
    const results = [];
    //logger.debug(this.minify(headers))

    //for (let i = 1; i < lines.length - 1 || i < 2 ; i++) {
    for (let i = 1; i < lines.length; i++) {
      const obj = {};
      const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
      //logger.debug(this.minify(currentLine))
      for (let j = 0; j < headers.length; j++)
        obj[this.deleteSpaces(headers[j].replaceAll(/['"]/g, ''))] = this.deleteSpaces(currentLine[j]?.replaceAll(/['"]/g, ''));
      results.push(obj);
      //logger.debug(this.minify(obj))
    }
    //return results
    //logger.debug("convert csv to json")
    //logger.debug(this.minify(results))
    return JSON.stringify(results);
  },

  cleaned(obj) {
    //logger.info(typeof obj != "string" ? JSON.stringify(obj) : obj)
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
    if(req?.body?.mongoQuery && req.body.mongoQuery[''] == '{"$gte":null,"$lte":null}')
      delete req.body.mongoQuery['']
    if (!req.body.query && req?.body?.mongoQuery && !(Object.keys(req?.body?.mongoQuery).length == 1 && req.body.mongoQuery[''] == ''))
      //req.body ? req.body.mongoQuery = req.query : req.body = { mongoQuery: req.query }
    objectCheck([req.body.mongoQuery, req.query])
    next()
  }
}