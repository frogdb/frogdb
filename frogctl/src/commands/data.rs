use anyhow::Result;
use clap::Subcommand;

use crate::connection::ConnectionContext;

#[derive(Subcommand, Debug)]
pub enum DataCommand {
    /// Keyspace summary: key count by type, memory distribution, expiry stats
    Keyspace {
        /// Sample size for memory estimation
        #[arg(long, default_value_t = 10000)]
        samples: u64,
    },

    /// Pipe raw RESP commands from stdin
    Pipe {
        /// Pipeline batch size
        #[arg(long, default_value_t = 1000)]
        batch: u64,
    },

    /// Show which hash slot and node a key maps to
    Slot {
        /// Key to look up
        key: String,

        /// Show internal shard via DEBUG HASHING
        #[arg(long)]
        internal: bool,
    },
}

pub async fn run(cmd: &DataCommand, ctx: &mut ConnectionContext) -> Result<i32> {
    match cmd {
        DataCommand::Keyspace { .. } => {
            anyhow::bail!("frogctl data keyspace: not yet implemented")
        }
        DataCommand::Pipe { .. } => {
            anyhow::bail!("frogctl data pipe: not yet implemented")
        }
        DataCommand::Slot { key, internal } => run_slot(key, *internal, ctx).await,
    }
}

async fn run_slot(key: &str, internal: bool, ctx: &mut ConnectionContext) -> Result<i32> {
    let slot = crc16_slot(key);
    println!("Key: {key}");
    println!("Hash Slot: {slot}");

    if internal {
        match ctx.cmd("DEBUG", &["HASHING", key]).await {
            Ok(result) => println!("Internal: {result}"),
            Err(_) => println!("Internal: (DEBUG HASHING not available)"),
        }
    }

    Ok(0)
}

/// CRC16-CCITT for Redis hash slot calculation.
fn crc16_slot(key: &str) -> u16 {
    let key_bytes = hash_tag_content(key.as_bytes());
    crc16(key_bytes) % 16384
}

/// Extract hash tag content: if key contains `{...}` with non-empty content, use that.
fn hash_tag_content(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{')
        && let Some(end) = key[start + 1..].iter().position(|&b| b == b'}')
        && end > 0
    {
        return &key[start + 1..start + 1 + end];
    }
    key
}

/// CRC16-CCITT (polynomial 0x1021, init 0x0000) — matches Redis.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_slot() {
        // Well-known Redis hash slot values
        assert_eq!(crc16_slot("foo"), 12182);
        assert_eq!(crc16_slot("bar"), 5061);
        assert_eq!(crc16_slot("hello"), 866);
    }

    #[test]
    fn test_hash_tag() {
        assert_eq!(
            crc16_slot("{user}.following"),
            crc16_slot("{user}.followers")
        );
        assert_eq!(crc16_slot("{tag}key1"), crc16_slot("{tag}key2"));
    }

    #[test]
    fn test_hash_tag_empty_ignored() {
        // Empty hash tag {} should be ignored — full key is hashed
        assert_ne!(crc16_slot("{}key1"), crc16_slot("{}key2"));
    }
}
