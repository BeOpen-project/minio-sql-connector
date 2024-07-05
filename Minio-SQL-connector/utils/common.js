
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

  json2csv(obj) {
    console.debug("json2csv")
    let csv = ""

    for (let key in obj)
      csv = csv + key + ";"
    csv = [csv.substring(0, csv.length - 1)] 

    for (let key in obj)
      csv[1] = csv[1] + obj[key] + ";"
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

  isRawQuery(obj) {
    const keys = Object.keys(obj);
    if (keys.length !== 1)
      return false;
    return obj.value;
  },

  checkConfig(configIn) {

    if (!configIn.queryAllowedExtensions) configIn.queryAllowedExtensions = ["csv", "json", "geojson"]
    return configIn
  }
}