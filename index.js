(async() => {
    const dotEnv = require('dotenv');

    dotEnv.config();

    const db = require('./config/database');

    const games = require('./lib/games')(db);
    const athletes = require('./lib/athletes')(db);

    module.exports = {
        games: games,
        athletes: athletes
    };
})();