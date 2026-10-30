//! `nisshi-store`, the datastore (ADR 0016): the only writer of a store root. It owns its
//! configuration: `check-config` validates a file without starting.

mod config;
mod metrics;
mod server;

use nisshi_core::log::{self, Val};
use std::process::ExitCode;

const USAGE: &str = "usage:
  nisshi-store serve <config.toml>
  nisshi-store check-config <config.toml>
  nisshi-store --version";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["--version"] => {
            println!("nisshi-store {}", server::VERSION);
            ExitCode::SUCCESS
        }
        ["check-config", path] => match config::Config::load(path) {
            Ok(_) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("nisshi-store: {e}");
                ExitCode::FAILURE
            }
        },
        ["serve", path] => {
            // The service's stderr is its journal: even a fatal error is a schema line.
            match config::Config::load(path).and_then(server::run) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    log::error("exit", &[("err", Val::Str(&e))]);
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}
