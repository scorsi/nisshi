//! The TOML configuration of `nisshi-store` (milestone M1), rendered by the NixOS module with
//! `pkgs.formats.toml`; unknown keys are an error. Checked by `check-config` and at start.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub listen: Vec<String>,
    #[serde(default)]
    pub listen_metrics: String,
    #[serde(default)]
    pub data_dir: String,
    #[serde(default)]
    pub tls: Tls,
    /// hex sha256(token) → `node:<name>` | `reader` (ADR 0008).
    #[serde(default)]
    pub tokens: BTreeMap<String, String>,
    #[serde(default = "default_retention_days")]
    pub retention_days: i64,
    #[serde(default = "default_disk_reserve")]
    pub disk_reserve: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    #[serde(default)]
    pub cert_file: String,
    #[serde(default)]
    pub key_file: String,
}

fn default_retention_days() -> i64 {
    400
}

fn default_disk_reserve() -> u64 {
    2 << 30
}

/// An authenticated identity (ADR 0008).
#[derive(Debug, Clone, PartialEq)]
pub enum Identity {
    Node(String),
    Reader,
}

impl Config {
    /// Reads and checks a configuration file; errors name the file.
    pub fn load(path: &str) -> Result<Config, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| nisshi_core::store::ioerr("open", std::path::Path::new(path), &e))?;
        Config::parse(&text).map_err(|e| format!("{path}: {e}"))
    }

    pub fn parse(text: &str) -> Result<Config, String> {
        let cfg: Config = toml::from_str(text).map_err(|e| e.to_string().trim().to_string())?;
        let mut problems = Vec::new();
        if cfg.listen.is_empty() {
            problems.push("listen: at least one address".to_string());
        }
        if cfg.data_dir.is_empty() {
            problems.push("data_dir: required".into());
        }
        if cfg.tls.cert_file.is_empty() || cfg.tls.key_file.is_empty() {
            problems.push("tls.cert_file and tls.key_file: required".into());
        }
        if cfg.retention_days < 0 {
            problems.push("retention_days: 0 or more".into());
        }
        for (k, v) in &cfg.tokens {
            if k.len() != 64 || !k.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
                problems.push(format!("tokens: key {k} is not a lowercase hex SHA-256"));
            }
            if identity(v).is_none() {
                problems.push(format!("tokens: identity {v} is not node:<name> or reader"));
            }
        }
        if !problems.is_empty() {
            return Err(problems.join("; "));
        }
        Ok(cfg)
    }
}

pub fn identity(v: &str) -> Option<Identity> {
    match v.strip_prefix("node:") {
        _ if v == "reader" => Some(Identity::Reader),
        Some(n) if nisshi_core::logschema::is_node(n) => Some(Identity::Node(n.into())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_config() {
        let sha = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
        let good = format!(
            "listen = [\"127.0.0.1:9473\"]\ndata_dir = \"/var/lib/nisshi\"\n[tls]\ncert_file = \"c\"\nkey_file = \"k\"\n[tokens]\n\"{sha}\" = \"node:node-a\"\n"
        );
        let cfg = Config::parse(&good).unwrap();
        assert_eq!((cfg.retention_days, cfg.disk_reserve), (400, 2 << 30));
        assert_eq!(cfg.tokens[sha], "node:node-a");
        for (name, bad) in [
            ("unknown key", format!("listne_metrics = \"x\"\n{good}")),
            ("bad identity", good.replace("node:node-a", "admin")),
            ("bad hash", good.replace(sha, "abc")),
            (
                "no listen",
                good.replace("listen = [\"127.0.0.1:9473\"]", "listen = []"),
            ),
            ("negative days", format!("retention_days = -1\n{good}")),
            ("no tls", good.replace("cert_file = \"c\"\n", "")),
        ] {
            assert!(Config::parse(&bad).is_err(), "{name}: accepted");
        }
        assert_eq!(
            Config::parse("").unwrap_err(),
            "listen: at least one address; data_dir: required; tls.cert_file and tls.key_file: required"
        );
    }
}
