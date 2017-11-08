module.exports = async(db) => {
    const importPlayers = async(players) => {
        for (let player of players) {
            await importPlayer(player);
        }
    }

    const importPlayer = async(player) => {
        let athlete = await db.any("SELECT * FROM athlete WHERE id = $1", [player.id]);
        let hometownIds = await db.any("SELECT id FROM hometown WHERE city = $1 AND state = $2 AND country = $3", [
            player.birthPlace.city,
            player.birthPlace.state,
            player.birthPlace.country
        ]);

        let hometownId;

        if (!hometownIds.length) {
            hometownId = await db.one("INSERT $1, $2, $3 INTO hometown RETURNING id");
        } else {
            hometownId = hometownIds[0];
        }

        if (athlete.length) {
            await db.none("UPDATE athlete SET first_name = $1, last_name = $2, weight = $3, height = $4, jersey = $5, hometown_id = $7, position_id = $7 WHERE id = $8", [
                player.firstName,
                player.lastName,
                player.weight,
                player.height,
                player.jersey,
                hometownId,
                player.position.id,
                player.id
            ]);
        } else {
            await db.none("INSERT INTO athlete(id, team_id, name, first_name, last_name, weight, height, jersey, hometown_id, position_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", [
                player.id,
                player.teamId,
                player.displayName,
                player.firstName,
                player.lastName,
                player.weight,
                player.height,
                player.jersey,
                hometownId,
                player.position.id
            ]);
        }
    }

    return {
        importPlayer: importPlayer,
        importPlayers: importPlayers
    }
}