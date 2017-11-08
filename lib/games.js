module.exports = (db) => {
    const cfb = require('cfb-data');

    const importGames = async(season, week) => {
        let now = new Date();
        console.log(`${now.getHours()}:${now.getMinutes()}:${now.getSeconds()} - Importing data for ${season} week ${week}`); // eslint-disable-line

        const scoreboard = await cfb.scoreboard.getScoreboard({
            groups: 80,
            year: season,
            week: week
        });

        for (let game of scoreboard.events) {
            await importGame(game.id);
        }
    };

    const importGame = async(id) => {
        let game = await cfb.games.getSummary(id);

        if (!isGameFinal(game)) {
            return;
        }

        let venueInsert = createVenueInsert(game);
        let gameInsert = createGameInsert(game);
        let gameTeamInserts = createGameTeamInserts(game);
        let driveInserts = createDriveInserts(game);
        let playInserts = createPlayInserts(game);

        if (venueInsert) {
            await db.tx(t => {
                    return t.none(venueInsert.sql, venueInsert.data);
                })
                .catch(err => {
                    console.error(err); // eslint-disable-line
                });
        }

        await db.tx(async t => {
                let gameCommand = t.none(gameInsert.sql, gameInsert.data);
                let gameTeamCommands = gameTeamInserts.map(gt => t.one(gt.sql, gt.data));
                let driveCommands = driveInserts.map(d => t.none(d.sql, d.data));
                let playCommands = playInserts.map(p => t.none(p.sql, p.data));

                let batchResult = await t.batch([
                    gameCommand,
                    ...gameTeamCommands,
                    ...driveCommands,
                    ...playCommands
                ]);

                let teamGameResults = batchResult.filter(r => r !== null);

                let athleteInserts = createAthleteInserts(game);
                let teamStatInserts = createTeamStatInserts(game, teamGameResults);
                let playerStatInserts = createPlayerStatInserts(game, teamGameResults);

                let athleteCommands = athleteInserts.map(a => t.none(a.sql, a.data));
                let teamCommands = teamStatInserts.map(s => t.none(s.sql, s.data));
                let playerCommands = playerStatInserts.map(s => t.none(s.sql, s.data));

                await t.batch([
                    ...athleteCommands,
                    ...teamCommands,
                    ...playerCommands
                ]);

                return Promise.resolve();
            })
            .catch(err => {
                console.error(err); // eslint-disable-line
            });
    }

    const isGameFinal = (game) => {
        return game.header.competitions[0].status.type.state == 'post' && game.header.competitions[0].status.type.description == 'Final';
    }

    const createVenueInsert = (game) => {
        const venueSql = "INSERT INTO venue(id, name, capacity, grass, city, state, zip) SELECT $1, $2, $3, $4, $5, $6, $7 WHERE NOT EXISTS (SELECT id FROM venue WHERE id = $1)";

        if (!game.gameInfo.venue) {
            return null;
        }

        return {
            sql: venueSql,
            data: [
                game.gameInfo.venue.id,
                game.gameInfo.venue.fullName,
                game.gameInfo.venue.capacity,
                game.gameInfo.venue.grass,
                game.gameInfo.venue.address.city,
                game.gameInfo.venue.address.state,
                game.gameInfo.venue.address.zipCode
            ]
        };
    };

    const createAthleteInserts = (game) => {
        const athleteSql = "INSERT INTO athlete(id, team_id, name, first_name, last_name) SELECT $1, $2, $3, $4, $5 WHERE NOT EXISTS (SELECT id FROM athlete WHERE id = $1)";
        let players = [];

        for (let info of game.boxscore.players) {
            let team = info.team;

            for (let stat of info.statistics) {
                for (let athlete of stat.athletes) {
                    if (!(players.find(p => p.player.id === athlete.athlete.id))) {
                        players.push({
                            teamId: team.id,
                            player: athlete.athlete
                        });
                    }
                }
            }
        }

        return players.filter(p => p.player && p.player.id).map(p => {
            let nameParts = splitName(p.player.displayName);

            return {
                sql: athleteSql,
                data: [
                    p.player.id,
                    p.teamId,
                    p.player.displayName,
                    ...nameParts
                ]
            }
        });
    };

    const splitName = (name) => {
        // This is very rudimentary and will need a more robust implementation
        let firstSpace = name.indexOf(" ");

        return [
            name.substr(0, firstSpace),
            name.substr(firstSpace + 1)
        ]
    }

    const createGameInsert = (game) => {
        const gameSql = "INSERT INTO game(id, season, week, season_type, start_date, neutral_site, conference_game, attendance, venue_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

        return {
            sql: gameSql,
            data: [
                game.header.id,
                game.header.season.year,
                game.header.week,
                getSeasonType(game.header.season.type),
                new Date(game.header.competitions[0].date),
                game.header.competitions[0].neutralSite,
                game.header.competitions[0].conferenceCompetition,
                game.gameInfo.attendance,
                game.gameInfo.venue ? game.gameInfo.venue.id : null
            ]
        };
    };

    const createGameTeamInserts = (game) => {
        const teamSql = "INSERT INTO game_team(game_id, team_id, home_away, points, winner, line_scores) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, team_id";

        return game.header.competitions[0].competitors.map(c => {
            return {
                sql: teamSql,
                data: [
                    game.header.id,
                    c.id,
                    c.homeAway,
                    c.score,
                    c.winner,
                    c.linescores ? c.linescores.map(ls => ls.displayValue * 1.0) : []
                ]
            };
        });
    };

    const createDriveInserts = (game) => {
        const driveSql = "INSERT INTO drive(id, game_id, offense_id, defense_id, scoring, start_period, start_yardline, start_time, end_period, end_yardline, end_time, elapsed, plays, yards, result_id) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 FROM drive_result dr WHERE dr.name = $15";

        if (!game.drives) {
            return [];
        }

        return game
            .drives
            .previous
            .filter(d => d.team != null)
            .map(d => {
                if (!d.result) {
                    d;
                }

                return {
                    sql: driveSql,
                    data: [
                        d.id,
                        game.header.id,
                        game.header.competitions[0].competitors.find(t => t.team.abbreviation === d.team.abbreviation).id,
                        game.header.competitions[0].competitors.find(t => t.team.abbreviation !== d.team.abbreviation).id,
                        d.isScore,
                        d.start.period.number,
                        d.start.yardLine,
                        `00:${d.start.clock ? d.start.clock.displayValue : "00:00"}`,
                        d.end ? d.end.period.number : d.start.period.number,
                        d.end ? d.end.yardLine : d.start.yardLine + d.yards,
                        `00:${d.end && d.end.clock ? d.end.clock.displayValue : "00:00"}`,
                        `00:${d.timeElapsed ? getValidInterval(d.timeElapsed.displayValue, d.description) : "00:00"}`,
                        d.offensivePlays,
                        d.yards,
                        d.result
                    ]
                };
            });
    };

    const createPlayInserts = (game) => {
        const playSql = "INSERT INTO play(id, drive_id, offense_id, defense_id, home_score, away_score, period, clock, yard_line, down, distance, yards_gained, scoring, play_type_id, play_text) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        let plays = [];

        if (!game.drives) {
            return plays;
        }

        for (let drive of game.drives.previous.filter(d => d.team != null)) {
            plays = plays.concat(drive.plays.map(p => {
                return {
                    sql: playSql,
                    data: [
                        p.id,
                        drive.id,
                        p.start.team ? p.start.team.id : game.header.competitions[0].competitors.find(t => t.team.abbreviation === drive.team.abbreviation).id,
                        p.start.team ? game.header.competitions[0].competitors.find(t => t.id !== p.start.team.id).id : game.header.competitions[0].competitors.find(t => t.team.abbreviation !== drive.team.abbreviation).id,
                        p.homeScore,
                        p.awayScore,
                        p.period.number,
                        `00:${p.clock ? p.clock.displayValue : '00:00'}`,
                        p.start.yardLine,
                        p.start.down,
                        p.start.distance,
                        p.statYardage,
                        p.scoringPlay,
                        p.type ? p.type.id : null,
                        p.text
                    ]
                };
            }));
        }

        return plays;
    };

    const createTeamStatInserts = (game, teamMaps) => {
        const teamStatSql = "INSERT INTO game_team_stat(game_team_id, type_id, stat) SELECT $1, id, $3 FROM team_stat_type WHERE name = $2";
        let stats = [];

        for (let team of game.boxscore.teams) {
            let gameTeamId = teamMaps.find(m => m.team_id == team.team.id).id;
            for (let stat of team.statistics) {
                stats.push({
                    sql: teamStatSql,
                    data: [
                        gameTeamId,
                        stat.name,
                        stat.displayValue
                    ]
                });
            }
        }

        return stats;
    }

    const createPlayerStatInserts = (game, teamMaps) => {
        const playerStatSql = "INSERT INTO game_player_stat(game_team_id, athlete_id, category_id, type_id, stat) SELECT $1, $2, s_cat.id, s_type.id, $5 FROM player_stat_category s_cat INNER JOIN player_stat_type s_type ON s_type.name = $4 WHERE s_cat.name = $3";
        let stats = [];

        for (let team of game.boxscore.players) {
            let gameTeamId = teamMaps.find(m => m.team_id == team.team.id).id;

            for (let category of team.statistics) {
                let categoryName = category.name;

                for (var i = 0; i < category.labels.length; i++) {
                    let label = category.labels[i];

                    for (let athlete of category.athletes) {
                        if (athlete.athlete && athlete.athlete.id) {
                            stats.push({
                                sql: playerStatSql,
                                data: [
                                    gameTeamId,
                                    athlete.athlete.id,
                                    categoryName,
                                    label,
                                    athlete.stats[i]
                                ]
                            });
                        }
                    }
                }
            }
        }

        return stats;
    }

    const getSeasonType = (id) => {
        switch (id) {
            case 1:
                return "preseason";
            case 2:
                return "regular";
            case 3:
                return "postseason";
            case 4:
                return "allstar";
            default:
                return "regular";
        }
    };

    const getValidInterval = (duration, description) => {
        const anchoredRegex = /^\d{1,2}:\d{2}$/g;
        const unanchoredRegex = /\d{1,2}:\d{2}$/g;

        if (!duration.match(anchoredRegex) && description) {
            let match = description.match(unanchoredRegex);

            return match && match.length > 0 ? match[0] : '00:00';
        } else if (!description) {
            return '00:00';
        }

        return duration;
    };

    return {
        importGame: importGame,
        importGames: importGames
    }
}