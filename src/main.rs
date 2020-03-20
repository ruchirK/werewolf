use std::error::Error as _;

use avro_rs::schema::Schema;
use futures::executor::block_on;
use rand::seq::SliceRandom;
use rand::thread_rng;
use structopt::StructOpt;

use crate::avro::{encode_insert, encode_delete};
use crate::config::{Args, KafkaConfig, MzConfig};
use crate::error::Result;
use crate::kafka_client::KafkaClient;

mod avro;
mod config;
mod error;
mod kafka_client;

fn main() {
    if let Err(e) = run() {
        println!("ERROR: {}", e);
    }
}

fn run() -> Result<()> {
    let config = Args::from_args();
    let kafka_config = config.kafka_config();
    //let mut test = vec!["ruchir", "bbarg", "bcaine", "adam"];
    //deal(&mut test, &kafka_config)?;

    swap(&kafka_config, "gabe", "villager", "chae", "werewolf");

    Ok(())
}

fn deal(players: &mut Vec<&str>, config: &KafkaConfig) -> Result<()> {
    let mut roles = vec![
        "werewolf",
        "werewolf",
        "seer",
        "robber",
        "troublemaker",
        "villager",
    ];

    let num_players = players.len();

    if num_players < 3 {
        return Err("cannot play with less than three players".into());
    }

    if num_players > 10 {
        return Err("cannot play with more than ten players".into());
    }

    players.sort();
    players.dedup();

    if players.len() != num_players {
        return Err("cannot have duplicate in player names".into());
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

    let mut client = kafka_client::KafkaClient::new(&config.url, &config.group_id)?;

    for (role, player) in roles.iter().zip(players.iter()) {
        send_player(&config, &mut client, player, role, true);
    }

    Ok(())
}

fn send_player(config: &KafkaConfig, client: &mut KafkaClient, player: &str, role: &str, insert: bool) -> Result<()> {
    let encoded = if insert == true {
        encode_insert(player, role)?
    } else {
        encode_delete(player, role)?
    };

    let future = client.send(&config.topic, &encoded);
    block_on(future);

    Ok(())
}

fn swap(config:&KafkaConfig, player1: &str, role1: &str, player2: &str, role2: &str) -> Result<()> {
    let mut client = kafka_client::KafkaClient::new(&config.url, &config.group_id)?;
    
//    send_player(config, &mut client, player1, role1, false);
//    send_player(config, &mut client, player2, role2, false);
    send_player(config, &mut client, player1, role2, true);
    send_player(config, &mut client, player2, role1, true);

    Ok(())
}
