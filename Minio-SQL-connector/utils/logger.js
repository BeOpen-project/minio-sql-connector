const config = require("../config");
const path = require("path");
const fs = require("fs");
let registredDay = 0;
const logPath = config.logPath || "logs/"
if (!fs.existsSync(logPath))
  fs.mkdirSync(logPath, { recursive: true });
let logStream = fs.createWriteStream(setLogDate(), { flags: "a" });
const { inspect } = require('util')
let thisFilename

setInterval(checkDate, 6000);
setInterval(deleteOldLogs, 24 * 60 * 60 * 1000);
deleteOldLogs()

function setLogDate() {
  const date = new Date();
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  // Aggiungiamo 1 a month perché gennaio è 0
  const day = String(date.getDate()).padStart(2, "0");
  registredDay = day;

  return logPath + `${year}-${month}-${day}` + ".txt";
}

function checkDate() {
  if (registredDay !== String(new Date().getDate()).padStart(2, "0")) {
    logStream.close();
    logStream = fs.createWriteStream(setLogDate(), { flags: "a" });
  }
}

// Funzione per controllare l'età di un file
function isOlderThan30Days(filePath) {
  try {
    const fileStats = fs.statSync(filePath);
    const { mtimeMs, birthtimeMs } = fileStats
    //console.debug(fileStats)
    const fileTime = (birthtimeMs > mtimeMs) && birthtimeMs || (mtimeMs > birthtimeMs) && mtimeMs
    //console.debug(fileTime)
    const currentDate = Date.now();
    //console.debug(currentDate)
    const thirtyDaysInMilliseconds = 30 * 24 * 60 * 60 * 1000;
    //console.debug(currentDate - fileTime)
    //console.debug(thirtyDaysInMilliseconds)
    //console.debug(currentDate - fileTime > thirtyDaysInMilliseconds)
    return currentDate - fileTime > thirtyDaysInMilliseconds;
  } catch (err) {
    if (err.code === "EPERM") {
      console.error("Errore di permessi:", err);
    } else {
      console.error("Errore durante l'ottenimento delle statistiche:", err);
    }
  }
}

// Funzione per eliminare i file
function deleteOldLogs() {
  fs.readdir(logPath + "", (err, files) => {
    if (err) {
      console.error("Errore durante la lettura della directory:", err);
      return;
    }

    files.forEach((file) => {
      const filePath = path.join(logPath, file);
      if (isOlderThan30Days(filePath)) {
        fs.unlinkSync(filePath, (err) => {
          if (err) {
            console.error(
              `Errore durante l'eliminazione del file ${file}:`,
              err
            );
          } else {
            console.log(`File ${file} eliminato.`);
          }
        });
      }
    });
  });
}

const LEVEL = process.env.LEVEL?.toLowerCase() || config.logLevel || "trace";

function saveLog(log) {
  if (config.saveLog)
    try {
      if (log && (Array.isArray(log) || typeof log == "object"))
        try {
          logStream.write(JSON.stringify(log) + "\n");
        }
        catch (error) {
          logStream.write(JSON.stringify(inspect(log)) + "\n");
        }
      else logStream.write(log + "\n");
    } catch (error) {
      console.log("Logs saving fail");
      console.error(error);
    }
}

function customLogger(level, fileName) {
  if (fileName.includes("backend")) fileName = fileName.split("backend")[1];
  const currentDate = new Date().toISOString();
  const line = new Error().stack.split("\n")[3].split("  at").pop()
  const log = `[${currentDate}] [${line}] [${level}]\n`;
  //const log = `[${currentDate}] [${fileName}] [${level}]`;
  return log;
}

function logBackup(...messages) {
  console.log(...messages);
  for (let m of messages) if (m != " ") saveLog(m);
}

function debugBackup(...messages) {
  console.debug(...messages);
  for (let m of messages) if (m != " ") saveLog(m);
}

function errorBackup(...messages) {
  console.error(...messages);
  for (let m of messages) if (m != " ") saveLog(m);
}

function warnBackup(...messages) {
  console.warn(...messages);
  for (let m of messages) if (m != " ") saveLog(m);
}

function infoBackup(...messages) {
  console.info(...messages);
  for (let m of messages) if (m != " ") saveLog(m);
}

function minifyMessages(...messages) {
  let messagesArray = [...messages]
  for (let i = 0; i < messagesArray.length; i++) {
    messagesArray[i] = minifyMessage(messagesArray[i]);
    //messagesArray[i] = minifyMessage(...messagesArray[i]);
  }
  return messagesArray
}

function minifyMessage(messag) {
  let message = messag//[0]
  let start = 500, end = 800
  try {
    if (message && (Array.isArray(message) || typeof message == "object"))
      try {
        if (JSON.stringify(message).length < (start + end))
          return message
        return JSON.stringify(message).substring(0, start).concat("...").concat(JSON.stringify(message).substring(JSON.stringify(message).length - end))//, JSON.stringify(message).length - 1))
      }
      catch (error) {
        if (JSON.stringify(inspect(message)).length < 400)
          return message
        return JSON.stringify(inspect(message)).substring(0, start).concat("...").concat(JSON.stringify(inspect(message)).substring(JSON.stringify(inspect(message)) - end))//, JSON.stringify(inspect(message)).length - 1))
      }
    if (typeof message == "string")
      if (message.length < 400)
        return message
      else
        return message.substring(0, start).concat("...").concat(message.length - end)//, message.length - 1)
    return message
  } catch (error) {
    console.log("Logs minifying fail", error, message);
    return message
  }
}

class Logger {
  constructor(fileName) {
    thisFilename = fileName;
  }

  customLogger(level) {
    if (thisFilename.includes("backend"))
      thisFilename = thisFilename.split("backend")[1];
    const currentDate = new Date().toISOString();
    const log = `[${currentDate}] [${thisFilename}] [${level}]`;
    return log;
  }

  truncate = {
    trace: (...message) => {
      if (LEVEL == "trace")
        logBackup(customLogger("trace", thisFilename), " ", ...minifyMessages(message));
    },
    debug: (...message) => {
      if (LEVEL == "trace" || LEVEL == "debug")
        debugBackup(customLogger("debug", thisFilename), " ", ...minifyMessages(message));
    },
    info: (...message) => {
      if (LEVEL == "trace" || LEVEL == "debug" || LEVEL == "info")
        infoBackup(customLogger("info", thisFilename), " ", ...minifyMessages(message));
    },
    warn: (...message) => {
      if (
        LEVEL == "trace" ||
        LEVEL == "debug" ||
        LEVEL == "info" ||
        LEVEL == "warn"
      )
        warnBackup(customLogger("warn", thisFilename), " ", ...minifyMessages(message));
    },
    error: (...message) => {
      if (
        LEVEL == "trace" ||
        LEVEL == "debug" ||
        LEVEL == "info" ||
        LEVEL == "warn" ||
        LEVEL == "error"
      )
        errorBackup(customLogger("error", thisFilename), " ", ...minifyMessages(message));
    },
    err: (...message) => {
      if (
        LEVEL == "trace" ||
        LEVEL == "debug" ||
        LEVEL == "info" ||
        LEVEL == "warn" ||
        LEVEL == "error"
      )
        errorBackup(customLogger("error", thisFilename), " ", ...minifyMessages(message));
    }
  }

  trace(...message) {
    if (LEVEL == "trace")
      if (config.truncateLogs)
        logBackup(customLogger("trace", thisFilename), " ", ...minifyMessages(message));
      else
        logBackup(customLogger("trace", thisFilename), " ", ...message);
  }
  debug(...message) {
    if (LEVEL == "trace" || LEVEL == "debug")
      if (config.truncateLogs)
        debugBackup(customLogger("debug", thisFilename), " ", ...minifyMessages(message));
      else
        debugBackup(customLogger("debug", thisFilename), " ", ...message);
  }
  info(...message) {
    if (LEVEL == "trace" || LEVEL == "debug" || LEVEL == "info")
      if (config.truncateLogs)
        infoBackup(customLogger("info", thisFilename), " ", ...minifyMessages(message));
      else
        infoBackup(customLogger("info", thisFilename), " ", ...message);

  }
  warn(...message) {
    if (
      LEVEL == "trace" ||
      LEVEL == "debug" ||
      LEVEL == "info" ||
      LEVEL == "warn"
    )
      if (config.truncateLogs)
        warnBackup(customLogger("warn", thisFilename), " ", ...minifyMessages(message));
      else
        warnBackup(customLogger("warn", thisFilename), " ", ...message);
  }
  error(...message) {
    if (
      LEVEL == "trace" ||
      LEVEL == "debug" ||
      LEVEL == "info" ||
      LEVEL == "warn" ||
      LEVEL == "error"
    )
      if (config.truncateLogs)
        errorBackup(customLogger("error", thisFilename), " ", ...minifyMessages(message));
      else
        errorBackup(customLogger("error", thisFilename), " ", ...message);
  }
  err(...message) {
    if (
      LEVEL == "trace" ||
      LEVEL == "debug" ||
      LEVEL == "info" ||
      LEVEL == "warn" ||
      LEVEL == "error"
    )
      if (config.truncateLogs)
        errorBackup(customLogger("error", thisFilename), " ", ...minifyMessages(message));
      else
        errorBackup(customLogger("error", thisFilename), " ", ...message);
  }
}

module.exports = {
  Logger: Logger
};
