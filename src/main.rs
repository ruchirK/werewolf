use rand::thread_rng;
use rand::seq::SliceRandom;

mod config;
mod error;
mod kafka_client;

fn main() {

    let mut test = vec!["ruchir", "bbarg", "bcaine", "ruchir"];
    if let Err(e) = deal(&mut test) {
        println!("ERROR: {}", e);
    }
}

fn deal(players: &mut Vec<&str>) -> Result<(), &'static str> {
    let mut roles = vec!["werewolf", "werewolf", "seer", "robber", "troublemaker", "villager"];

    let num_players = players.len();

    if num_players < 3 {
        return Err("cannot play with less than three players");
    }

    if num_players > 10 {
        return Err("cannot play with more than ten players");
    }

    players.sort();
    players.dedup();

    if players.len() != num_players {
        return Err("cannot have duplicate in player names");
    }

    if num_players >= 4 {
        roles.push("villager");
    }

    if num_players >= 5 {
        roles.push("villager");
    }

    if num_players > 5 {
        let mut additional_roles = vec!["drunk", "mason", "mason", "insomniac", "minion"];
        additional_roles.shuffle(&mut thread_rng());
        additional_roles.truncate(num_players - 5);
        roles.append(&mut additional_roles);
    }

    assert!(roles.len() == (players.len() + 3));
    players.append(&mut vec!["center", "center", "center"]);

    roles.shuffle(&mut thread_rng());

    roles.iter().zip(players.iter()).for_each(|(role, player)| println!("{} {}", role, player));

    Ok(())
}
