
module.exports = {
  
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    },

    convertCSVtoJSON(csvData) {
      console.debug(csvData)
        const lines = csvData.split('\r\n');
        console.debug(lines)
        const possibleHeaders = [
          lines[0].trim().split(','),
          lines[0].trim().split(';')
        ]
        console.debug(possibleHeaders)
        const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
        const results = [];
        console.debug(headers)
      
        //for (let i = 1; i < lines.length - 1 || i < 2 ; i++) {
        for (let i = 1; i < lines.length ; i++) {
          const obj = {};
          const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
          console.debug(currentLine)
          for (let j = 0; j < headers.length; j++) 
            obj[headers[j]] = currentLine[j]?.replace(/['"]/g, '');
          results.push(obj);
          console.debug(obj)
        }
        //return results
        console.debug("convert csv to json")
        console.debug(JSON.stringify(results))
        return JSON.stringify(results);
      },

      setQuery(query){

      },

      cleaned (obj){
        console.debug("CLEAN")
        console.debug(typeof obj != "string" ? JSON.stringify(obj) : obj)
        //return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['"\r\n\s]/g, '');

        return (typeof obj != "string" ? JSON.stringify(obj) : obj).replace(/['"]/g, '');
      },

      isRawQuery(obj) {
        const keys = Object.keys(obj);

        console.debug(keys.length)
    
        // Verifica che ci sia una sola chiave
        if (keys.length !== 1) {
            return false;
        }
    
        // Verifica che il valore della chiave sia uguale a "value"
        console.debug(obj[keys[0]], obj[keys[0]] == "value")
        return obj.value;
    }
}