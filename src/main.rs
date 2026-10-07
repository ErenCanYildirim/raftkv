
//! Command-line entry point: run a node, or talk to a cluster as a client.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used)]
 
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;
 
use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
 
use raftkv::client;
use raftkv::node::{self, NodeAddrs, NodeConfig, parse_member};
use raftkv::types::NodeId;
 
/// A Raft-replicated key-value store.
#[derive(Parser, Debug)]
#[command(name = "raftkv", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
 

 
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a cluster node.
    ///
    /// Every node is started with the full member list, for example:
    ///
    ///   raftkv node --id 1 --data-dir data/1 \
    ///     --member 1=127.0.0.1:7001,127.0.0.1:8001 \
    ///     --member 2=127.0.0.1:7002,127.0.0.1:8002 \
    ///     --member 3=127.0.0.1:7003,127.0.0.1:8003
    Node {
        /// This node's id (must appear in the member list).
        #[arg(long)]
        id: NodeId,
        /// A cluster member as `ID=RAFT_ADDR,CLIENT_ADDR`. Repeat for every member.
        #[arg(long = "member", value_parser = parse_member, required = true)]
        members: Vec<(NodeId, NodeAddrs)>,
        /// Directory for this node's durable state.
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Send a command to the cluster: get KEY, set KEY VALUE, del KEY, or status.
    ///
    /// `status` asks every listed node for its local view.
    Client {
        /// Client addresses of the nodes, comma-separated.
        #[arg(long, value_delimiter = ',', required = true)]
        nodes: Vec<String>,
        /// Give up after this many seconds.
        #[arg(long, default_value_t = 10)]
        timeout_secs: u64,
        /// The command, e.g. `set greeting hello world`.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        command: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let default_level = match cli.command {
        Commands::Node { .. } => "info",
        Commands::Client { .. } => "warn",
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level)),
        )
        .with_writer(std::io::stderr)
        .init();
    
    match run(cli.command).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(command: Commands) -> anyhow::Result<ExitCode> {
    match command {
        Commands::Node {
            id,
            members,
            data_dir, 
        } => {
            let mut map = BTreeMap::new();
            for (member_id, addrs) in members {
                if map.insert(member_id, addrs).is_some() {
                    bail!("member {member_id} is listed more than once");
                }
            }
            node::run(NodeConfig {
                id,
                members: map,
                data_dir,
            })
            .await 
            .context("node failed")?;
            Ok(ExitCode::SUCCESS)
        }
        Commands::Client {
            nodes,
            timeout_secs,
            command,
        } => {
            let line = command.join(" ");
            if line.trim().eq_ignore_ascii_case("status") {
                return Ok(print_status(&nodes).await);
            }
            let reply = client::execute(&nodes, &line, Duration::from_secs(timeout_secs))
                .await
                .map_err(anyhow::Error::msg)?;
            Ok(print_reply(&reply))
        }
    }
}

fn print_reply(reply: &str) -> ExitCode {
    if let Some(message) = reply.strip_prefix("ERR ") {
        eprintln!("error: {message}");
        return ExitCode::FAILURE;
    }
    if let Some(value) = reply.strip_prefix("VALUE ") {
        println!("{value}");
    } else if reply == "NIL" {
        println!("(nil)");
    } else if let Some(existed) = reply.strip_prefix("DELETED ") {
        println!("{existed}");
    } else {
        println!("{reply}");
    }
    ExitCode::SUCCESS
}
 
async fn print_status(nodes: &[String]) -> ExitCode {
    let mut all_ok = true;
    for addr in nodes {
        match client::request(addr, "STATUS").await {
            Ok(reply) => println!("{addr}: {}", reply.trim_start_matches("STATUS ")),
            Err(e) => {
                all_ok = false;
                println!("{addr}: unreachable ({e})");
            }
        }
    }
    if all_ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
