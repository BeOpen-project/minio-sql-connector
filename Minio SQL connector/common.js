

module.exports = {
  
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    },

    convertCSVtoJSON(csvData) {
        const lines = csvData.split('\r\n');
        const possibleHeaders = [
          lines[0].trim().split(','),
          lines[0].trim().split(';')
        ]
        const headers = possibleHeaders[0].length > possibleHeaders[1].length ? possibleHeaders[0] : possibleHeaders[1]
        const results = [];
      
        for (let i = 1; i < lines.length - 1; i++) {
          const obj = {};
          const currentLine = lines[i].trim().split(possibleHeaders[0].length > possibleHeaders[1].length ? "," : ";");
          for (let j = 0; j < headers.length; j++) 
            obj[headers[j]] = currentLine[j]?.replace(/['"]/g, '');
          results.push(obj);
        }
        //return results
        return JSON.stringify(results);
      },

      setQuery(query){

      },

      cleaned (obj){
        return obj.replace(/['"]/g, '')
      }
}