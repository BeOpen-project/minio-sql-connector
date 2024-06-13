
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

  urlEncode(bucket){
    return bucket.replaceAll("-", "")
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
        obj[headers[j]] = currentLine[j]?.replace(/['"]/g, '');
      results.push(obj);
      //console.debug(this.minify(obj))
    }
    //return results
    //console.debug("convert csv to json")
    //console.debug(this.minify(results))
    return JSON.stringify(results);
  },

  setQuery(query) {

  },

  cleaned(obj) {
    //console.debug("CLEAN")
    //console.debug(this.minify(obj))
    //return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['"\r\n\s]/g, '');

    return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['"]/g, '');
  },

  isRawQuery(obj) {
    const keys = Object.keys(obj);

    //console.debug(keys.length)

    // Verifica che ci sia una sola chiave
    if (keys.length !== 1) {
      return false;
    }

    // Verifica che il valore della chiave sia uguale a "value"
    //console.debug(obj[keys[0]], obj[keys[0]] == "value")
    return obj.value;
  },

  checkConfig(configIn){
    
    if (!configIn.queryAllowedExtensions) configIn.queryAllowedExtensions = ["csv", "json", "geojson"]
    return configIn
  }
}