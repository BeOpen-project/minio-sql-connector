
const config = require('./config')
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = 3000;
const mongoose = require("mongoose");
const cors = require('cors');
const routes = require ("./api/routes/router")

app.use(cors());
app.use(express.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(config.basePath || "/api", routes);
app.listen(port, () => {console.log(`Server listens on http://localhost:${port}`);});
mongoose.connect(config.mongo, { useNewUrlParser: true }).then(() => {console.log("Connected to mongo")})