//! `nisshi`, the data CLI (ADR 0016): the validator of the log schema (ADR 0004), `read` and
//! `verify` over a store's files (ADR 0010, 0007). It reads no configuration; the server is
//! `nisshi-store`.

use nisshi_core::{logschema, read, seal};
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::process::ExitCode;

const USAGE: &str = "usage:
  nisshi validate --logs < lines                  (raw service output or journalctl -o cat)
  nisshi read <root> --node <n> [--service <s>] [--event <e>] --from <ts> [--to <ts>]
  nisshi verify <root>";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    let code = run(
        &args,
        &mut io::stdin().lock(),
        &mut io::stdout().lock(),
        &mut io::stderr(),
    );
    ExitCode::from(code)
}

fn run(
    args: &[String],
    stdin: &mut dyn BufRead,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> u8 {
    let fail = |e: String, stderr: &mut dyn Write| {
        let _ = writeln!(stderr, "nisshi: {e}");
        1
    };
    match args {
        [cmd, flag] if cmd == "validate" && flag == "--logs" => validate(stdin, stderr),
        [cmd, root] if cmd == "verify" => match seal::verify(Path::new(root), stderr) {
            Ok(()) => 0,
            Err(e) => fail(e, stderr),
        },
        [cmd, root, rest @ ..] if cmd == "read" => {
            let Some(flags) = parse_read_flags(rest, stderr) else {
                let _ = writeln!(stderr, "{USAGE}");
                return 2;
            };
            let get = |k: &str| {
                flags
                    .iter()
                    .rev()
                    .find(|(n, _)| n == k)
                    .map(|(_, v)| v.clone())
            };
            let (node, from) = (
                get("node").unwrap_or_default(),
                get("from").unwrap_or_default(),
            );
            if node.is_empty() || from.is_empty() {
                let _ = writeln!(stderr, "{USAGE}");
                return 2;
            }
            let now = nisshi_core::time::real_now_ns();
            let from = match read::parse_time(&from, now) {
                Ok(t) => t,
                Err(e) => return fail(e, stderr),
            };
            let to = match get("to").filter(|t| !t.is_empty()) {
                None => now,
                Some(t) => match read::parse_time(&t, now) {
                    Ok(t) => t,
                    Err(e) => return fail(e, stderr),
                },
            };
            let q = read::Query {
                node,
                service: get("service").unwrap_or_default(),
                event: get("event").unwrap_or_default(),
                from,
                to,
            };
            match read::read(Path::new(root), &q, stdout) {
                Ok(()) => 0,
                Err(e) => fail(e, stderr),
            }
        }
        _ => {
            let _ = writeln!(stderr, "{USAGE}");
            2
        }
    }
}

/// The flags of `read` as Go's `flag` package parses them (`-x v`, `--x v`, `-x=v`; parsing
/// stops at the first non-flag or `--`; the last value wins), with its messages on errors.
fn parse_read_flags(args: &[String], stderr: &mut dyn Write) -> Option<Vec<(String, String)>> {
    const FLAGS: [(&str, &str); 5] = [
        ("event", "event filter"),
        ("from", "start, RFC 3339 or offset such as -1h (required)"),
        ("node", "node name (required)"),
        ("service", "service filter"),
        ("to", "end, RFC 3339 or offset; default now"),
    ];
    let usage = |stderr: &mut dyn Write| {
        let _ = writeln!(stderr, "Usage of read:");
        for (name, help) in FLAGS {
            let _ = writeln!(stderr, "  -{name} string\n    \t{help}");
        }
    };
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let s = &args[i];
        if s.len() < 2 || !s.starts_with('-') {
            break;
        }
        let dashes = if s.as_bytes()[1] == b'-' { 2 } else { 1 };
        if s == "--" {
            break;
        }
        let body = &s[dashes..];
        if body.is_empty() || body.starts_with('-') || body.starts_with('=') {
            let _ = writeln!(stderr, "bad flag syntax: {s}");
            usage(stderr);
            return None;
        }
        let (name, value) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_string())),
            None => (body, None),
        };
        if !FLAGS.iter().any(|(n, _)| *n == name) {
            if name != "help" && name != "h" {
                let _ = writeln!(stderr, "flag provided but not defined: -{name}");
            }
            usage(stderr);
            return None;
        }
        let value = match value {
            Some(v) => v,
            None if i + 1 < args.len() => {
                i += 1;
                args[i].clone()
            }
            None => {
                let _ = writeln!(stderr, "flag needs an argument: -{name}");
                usage(stderr);
                return None;
            }
        };
        out.push((name.to_string(), value));
        i += 1;
    }
    Some(out)
}

/// `validate --logs`: lines as `bufio.Scanner` splits them (a trailing CR dropped, the last
/// line without newline included); stops at the first invalid one.
fn validate(stdin: &mut dyn BufRead, stderr: &mut dyn Write) -> u8 {
    let mut n = 0usize;
    let mut line: Vec<u8> = Vec::new();
    let mut too_long = false;
    let mut check = |line: &mut Vec<u8>, too_long: &mut bool, n: &mut usize| -> Option<u8> {
        *n += 1;
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        let reason = if *too_long {
            "line-too-large".to_string()
        } else {
            logschema::line(line)
        };
        line.clear();
        *too_long = false;
        (!reason.is_empty()).then(|| {
            let _ = writeln!(stderr, "line {n}: {reason}");
            1
        })
    };
    loop {
        let chunk = match stdin.fill_buf() {
            Ok(c) => c,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                let _ = writeln!(
                    stderr,
                    "nisshi: reading stdin: {}",
                    nisshi_core::store::errno_text(&e)
                );
                return 1;
            }
        };
        if chunk.is_empty() {
            break;
        }
        let (take, newline) = match chunk.iter().position(|&c| c == b'\n') {
            Some(i) => (i, true),
            None => (chunk.len(), false),
        };
        // Bounded: past the limit the line is only counted, never kept.
        if !too_long {
            line.extend_from_slice(&chunk[..take]);
            if line.len() > logschema::MAX_LINE_BYTES + 1 {
                too_long = true;
                line.clear();
            }
        }
        stdin.consume(take + usize::from(newline));
        if newline && let Some(code) = check(&mut line, &mut too_long, &mut n) {
            return code;
        }
    }
    if (!line.is_empty() || too_long)
        && let Some(code) = check(&mut line, &mut too_long, &mut n)
    {
        return code;
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_validate(input: &[u8]) -> (u8, String) {
        let mut err = Vec::new();
        let code = run(
            &["validate".into(), "--logs".into()],
            &mut &input[..],
            &mut Vec::new(),
            &mut err,
        );
        (code, String::from_utf8(err).unwrap())
    }

    #[test]
    fn validate_logs() {
        let ok = r#"{"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"query"}"#;
        let big = format!(
            r#"{{"v":1,"ts":"2026-10-19T08:14:03.512004117Z","level":"info","event":"big","pad":"{}"}}"#,
            "x".repeat(2 << 20)
        );
        for (name, input, code, stderr) in [
            ("empty", String::new(), 0, ""),
            ("valid", format!("{ok}\n{ok}\n"), 0, ""),
            ("no final newline", format!("{ok}\n{ok}"), 0, ""),
            ("crlf", format!("{ok}\r\n{ok}\r"), 0, ""),
            (
                "first invalid line",
                format!("{ok}\n{ok}\nlevel=info event=query\n{{}}\n"),
                1,
                "line 3: not-json\n",
            ),
            (
                "line over the scanner buffer",
                format!("{ok}\n{big}\n{ok}\n"),
                1,
                "line 2: line-too-large\n",
            ),
            ("empty line", format!("{ok}\n\n"), 1, "line 2: not-json\n"),
        ] {
            assert_eq!(
                run_validate(input.as_bytes()),
                (code, stderr.to_string()),
                "{name}"
            );
        }
        let exact = format!("{{\"a\":\"{}\"}}", "x".repeat((1 << 20) - 8));
        assert_eq!(exact.len(), 1 << 20);
        assert_eq!(
            run_validate(format!("{exact}\r\n").as_bytes()).1,
            "line 1: missing-field:v\n"
        );
        assert_eq!(
            run_validate(format!("{exact}x\n").as_bytes()).1,
            "line 1: line-too-large\n"
        );
        let mut err = Vec::new();
        assert_eq!(
            run(
                &["validate".into()],
                &mut &b""[..],
                &mut Vec::new(),
                &mut err
            ),
            2
        );
    }

    #[test]
    fn read_flags_as_go() {
        let args = |s: &[&str]| s.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        let mut err = Vec::new();
        let f = parse_read_flags(
            &args(&[
                "--node",
                "a",
                "-from=-1h",
                "-to",
                "x",
                "extra",
                "--node",
                "b",
            ]),
            &mut err,
        )
        .unwrap();
        assert_eq!(
            f,
            vec![
                ("node".into(), "a".into()),
                ("from".into(), "-1h".into()),
                ("to".into(), "x".into())
            ]
        );
        assert!(parse_read_flags(&args(&["--nod", "a"]), &mut err).is_none());
        assert!(String::from_utf8_lossy(&err).starts_with("flag provided but not defined: -nod\nUsage of read:\n  -event string\n    \tevent filter\n"));
        let mut out = Vec::new();
        let code = run(
            &args(&["read", "/nonexistent", "--from", "-1h"]),
            &mut &b""[..],
            &mut out,
            &mut err,
        );
        assert_eq!(code, 2);
    }
}
