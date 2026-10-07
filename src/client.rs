//! A client that finds the leader on its own: it follows `REDIRECT` replies,
//! and retries other nodes while no leader is known or a node is down.

use std::io;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use crate::client_api::CLIENT_REQUEST_TIMEOUT;
use crate::protocol::NO_LEADER;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(200);

/// Send one request line to the cluster and return the reply line.
///
/// Starts with the first address in `nodes`, follows redirects to the
/// leader, and keeps retrying until a definitive reply arrives or `deadline`
/// passes.
///
/// # Errors
/// Returns the last problem seen if no definitive reply arrived in time.
pub async fn execute(nodes: &[String], line: &str, deadline: Duration) -> Result<String, String> {
    if nodes.is_empty() {
        return Err("no node addresses given".to_string());
    }
    let started = Instant::now();
    let mut next_node = 0;
    let mut target = nodes[0].clone();
    let mut last_problem = String::from("no attempt made");

    while started.elapsed() < deadline {
        match request(&target, line).await {
            Ok(reply) => {
                if let Some(leader) = reply.strip_prefix("REDIRECT ") {
                    target = leader.to_string();
                    continue;
                }
                if reply.strip_prefix("ERR ") != Some(NO_LEADER) {
                    return Ok(reply);
                }
                last_problem = format!("{target}: {NO_LEADER}");
            }
            Err(e) => last_problem = format!("{target}: {e}"),
        }
        // Retryable: try the next node after a short pause.
        next_node = (next_node + 1) % nodes.len();
        target = nodes[next_node].clone();
        sleep(RETRY_DELAY).await;
    }
    Err(format!("gave up after {deadline:?}; last problem: {last_problem}"))
}

/// Send one request line to one node and read one reply line.
///
/// # Errors
/// Fails on connection problems or timeouts.
pub async fn request(addr: &str, line: &str) -> io::Result<String> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
    let (reader, mut writer) = stream.into_split();
    writer.write_all(format!("{line}\n").as_bytes()).await?;

    let mut reply = String::new();
    // Slightly longer than the server's own timeout, so its error arrives first.
    let wait = CLIENT_REQUEST_TIMEOUT + Duration::from_secs(1);
    let read = timeout(wait, BufReader::new(reader).read_line(&mut reply))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no reply"))??;
    if read == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed without a reply",
        ));
    }
    Ok(reply.trim_end().to_string())
}