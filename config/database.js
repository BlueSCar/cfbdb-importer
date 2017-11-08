const promise = require('bluebird');
const pgp = require('pg-promise');

const user = process.env.USER;
const password = process.env.PASSWORD;
const host = process.env.HOST;
const port = process.env.PORT;
const dbName = process.env.DATABASE;

const connectionString = `postgres://${user}:${password}@${host}:${port}/${dbName}`;

const dbCreator = pgp({
    promiseLib: promise
});

const db = dbCreator(connectionString);

module.exports = db;