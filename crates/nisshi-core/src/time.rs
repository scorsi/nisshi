//! Time without a date crate (ADR 0015): the schema's `ts` format, calendar days, and the
//! parsers of Go's `time` package that `nisshi read` and `NISSHI_FAKE_NOW` accept (RFC 3339
//! with Go's leniency, `time.ParseDuration`), with Go's error texts.

use std::time::{SystemTime, UNIX_EPOCH};

pub const NS: i64 = 1_000_000_000;
pub const DAY_SECS: i64 = 86_400;

/// Unix nanoseconds now, from the real clock.
pub fn real_now_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

/// `2026-10-19T08:14:00.000213904Z` (the schema's `ts`, UTC, 9 digits).
pub fn format_ts(ns: i64) -> String {
    let (secs, nanos) = (ns.div_euclid(NS), ns.rem_euclid(NS));
    let (days, sod) = (secs.div_euclid(DAY_SECS), secs.rem_euclid(DAY_SECS));
    format!(
        "{}T{:02}:{:02}:{:02}.{nanos:09}Z",
        format_day(days),
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// `YYYY-MM-DD` of a unix day number.
pub fn format_day(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Unix day number of unix nanoseconds.
pub fn day_number(ns: i64) -> i64 {
    ns.div_euclid(NS).div_euclid(DAY_SECS)
}

/// `YYYY-MM-DD` (UTC) of unix nanoseconds.
pub fn day_of(ns: i64) -> String {
    format_day(day_number(ns))
}

// Howard Hinnant's algorithms.
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in(m: i64, y: i64) -> i64 {
    match m {
        2 if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The schema's `ts` range: a signed 64-bit count of nanoseconds (log-schema.md).
pub fn in_range(ns: i128) -> bool {
    ns >= i128::from(i64::MIN) && ns <= i128::from(i64::MAX)
}

fn num(b: &[u8]) -> Option<i64> {
    b.iter().try_fold(0i64, |n, &c| {
        c.is_ascii_digit().then(|| n * 10 + i64::from(c - b'0'))
    })
}

/// Unix nanoseconds of a string already of the `ts` shape (`YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`),
/// `None` unless it is a real date and time (Go's `time.Parse` of that layout).
pub fn parse_ts_shape(s: &str) -> Option<i128> {
    let b = s.as_bytes();
    if b.len() != 30 {
        return None;
    }
    let (y, mo, d) = (num(&b[0..4])?, num(&b[5..7])?, num(&b[8..10])?);
    let (h, mi, se) = (num(&b[11..13])?, num(&b[14..16])?, num(&b[17..19])?);
    let nanos = num(&b[20..29])?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in(mo, y) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let secs = days_from_civil(y, mo, d) * DAY_SECS + h * 3600 + mi * 60 + se;
    Some(i128::from(secs) * i128::from(NS) + i128::from(nanos))
}

/// Go's `time.Parse("2006-01-02", d)` succeeds: exactly a real calendar day.
pub fn valid_day(d: &str) -> bool {
    let b = d.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    match (num(&b[0..4]), num(&b[5..7]), num(&b[8..10])) {
        (Some(y), Some(m), Some(dd)) => (1..=12).contains(&m) && dd >= 1 && dd <= days_in(m, y),
        _ => false,
    }
}

/// Go's `quote` of the time package (error messages).
pub fn go_quote(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        if (c as u32) < 0x20 || (c as u32) >= 0x80 {
            let mut buf = [0u8; 4];
            for byte in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("\\x{byte:02x}"));
            }
        } else {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
    }
    out.push('"');
    out
}

const RFC3339_NANO: &str = "2006-01-02T15:04:05.999999999Z07:00";

/// Go's `time.Parse(time.RFC3339Nano, s)`: unix nanoseconds, or Go's error text.
pub fn parse_rfc3339(s: &str) -> Result<i128, String> {
    let cannot = |value: &[u8], elem: &str| {
        format!(
            "parsing time {} as {}: cannot parse {} as {}",
            go_quote(s),
            go_quote(RFC3339_NANO),
            go_quote(&String::from_utf8_lossy(value)),
            go_quote(elem)
        )
    };
    let range = |what: &str| format!("parsing time {}: {what} out of range", go_quote(s));
    let mut v = s.as_bytes();
    let digit = |v: &[u8], i: usize| v.get(i).is_some_and(u8::is_ascii_digit);
    // getnum: one or two digits, two when fixed.
    let getnum = |v: &[u8], fixed: bool| -> Option<(i64, usize)> {
        if !digit(v, 0) {
            return None;
        }
        if !digit(v, 1) {
            return (!fixed).then(|| (i64::from(v[0] - b'0'), 1));
        }
        Some((i64::from(v[0] - b'0') * 10 + i64::from(v[1] - b'0'), 2))
    };
    let lit = |v: &mut &[u8], c: u8| -> Result<(), String> {
        if v.first() != Some(&c) {
            return Err(cannot(v, &(c as char).to_string()));
        }
        *v = &v[1..];
        Ok(())
    };
    // 2006
    if v.len() < 4 || !digit(v, 0) {
        return Err(cannot(v, "2006"));
    }
    let year = num(&v[..4]).ok_or_else(|| cannot(v, "2006"))?;
    v = &v[4..];
    lit(&mut v, b'-')?;
    let (month, n) = getnum(v, true).ok_or_else(|| cannot(v, "01"))?;
    v = &v[n..];
    if !(1..=12).contains(&month) {
        return Err(range("month"));
    }
    lit(&mut v, b'-')?;
    let (day, n) = getnum(v, true).ok_or_else(|| cannot(v, "02"))?;
    v = &v[n..];
    lit(&mut v, b'T')?;
    let (hour, n) = getnum(v, false).ok_or_else(|| cannot(v, "15"))?;
    v = &v[n..];
    if hour >= 24 {
        return Err(range("hour"));
    }
    lit(&mut v, b':')?;
    let (min, n) = getnum(v, true).ok_or_else(|| cannot(v, "04"))?;
    v = &v[n..];
    if min >= 60 {
        return Err(range("minute"));
    }
    lit(&mut v, b':')?;
    let (sec, n) = getnum(v, true).ok_or_else(|| cannot(v, "05"))?;
    v = &v[n..];
    if sec >= 60 {
        return Err(range("second"));
    }
    // .999999999: optional, '.' or ',', any number of digits (nine kept).
    let mut nsec = 0i64;
    if v.len() >= 2 && matches!(v[0], b'.' | b',') && v[1].is_ascii_digit() {
        let mut i = 1;
        while i < v.len() && v[i].is_ascii_digit() {
            i += 1;
        }
        let kept = &v[1..i.min(10)];
        nsec = num(kept).unwrap() * 10i64.pow(9 - kept.len() as u32);
        v = &v[i..];
    }
    // Z07:00
    let mut offset = 0i64;
    if v.first() == Some(&b'Z') {
        v = &v[1..];
    } else {
        let bad = || cannot(v, "Z07:00");
        if v.len() < 6 || v[3] != b':' {
            return Err(bad());
        }
        let (hr, mm) = match (getnum(&v[1..3], true), getnum(&v[4..6], true)) {
            (Some((h, _)), Some((m, _))) => (h, m),
            _ => return Err(bad()),
        };
        if hr > 24 {
            return Err(range("time zone offset hour"));
        }
        if mm > 60 {
            return Err(range("time zone offset minute"));
        }
        offset = (hr * 60 + mm) * 60;
        match v[0] {
            b'+' => {}
            b'-' => offset = -offset,
            _ => return Err(bad()),
        }
        v = &v[6..];
    }
    if !v.is_empty() {
        return Err(format!(
            "parsing time {}: extra text: {}",
            go_quote(s),
            go_quote(&String::from_utf8_lossy(v))
        ));
    }
    if day < 1 || day > days_in(month, year) {
        return Err(format!("parsing time {}: day out of range", go_quote(s)));
    }
    let secs = days_from_civil(year, month, day) * DAY_SECS + hour * 3600 + min * 60 + sec - offset;
    Ok(i128::from(secs) * i128::from(NS) + i128::from(nsec))
}

/// Go's `time.ParseDuration`: nanoseconds, or Go's error text.
pub fn parse_duration(orig: &str) -> Result<i64, String> {
    let err = |msg: &str| format!("time: {msg} {}", go_quote(orig));
    let invalid = || err("invalid duration");
    let mut s = orig.as_bytes();
    let mut neg = false;
    if let Some(&c) = s.first()
        && (c == b'-' || c == b'+')
    {
        neg = c == b'-';
        s = &s[1..];
    }
    if s == b"0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    const LIMIT: u64 = 1 << 63;
    let mut d: u64 = 0;
    while !s.is_empty() {
        if !(s[0] == b'.' || s[0].is_ascii_digit()) {
            return Err(invalid());
        }
        // leadingInt
        let mut v: u64 = 0;
        let mut i = 0;
        while i < s.len() && s[i].is_ascii_digit() {
            if v > LIMIT / 10 {
                return Err(invalid());
            }
            v = v * 10 + u64::from(s[i] - b'0');
            if v > LIMIT {
                return Err(invalid());
            }
            i += 1;
        }
        let pre = i > 0;
        s = &s[i..];
        let (mut f, mut scale, mut post) = (0u64, 1f64, false);
        if s.first() == Some(&b'.') {
            s = &s[1..];
            let mut i = 0;
            let mut overflow = false;
            while i < s.len() && s[i].is_ascii_digit() {
                if !overflow {
                    if f > (LIMIT - 1) / 10 {
                        overflow = true;
                    } else {
                        let y = f * 10 + u64::from(s[i] - b'0');
                        if y > LIMIT {
                            overflow = true;
                        } else {
                            f = y;
                            scale *= 10.0;
                        }
                    }
                }
                i += 1;
            }
            post = i > 0;
            s = &s[i..];
        }
        if !pre && !post {
            return Err(invalid());
        }
        let mut i = 0;
        while i < s.len() && !(s[i] == b'.' || s[i].is_ascii_digit()) {
            i += 1;
        }
        if i == 0 {
            return Err(err("missing unit in duration"));
        }
        let u = String::from_utf8_lossy(&s[..i]).into_owned();
        s = &s[i..];
        let unit: u64 = match u.as_str() {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(err(&format!("unknown unit {} in duration", go_quote(&u)))),
        };
        if v > LIMIT / unit {
            return Err(invalid());
        }
        v *= unit;
        if f > 0 {
            v += (f as f64 * (unit as f64 / scale)) as u64;
            if v > LIMIT {
                return Err(invalid());
            }
        }
        // Go's uint64 sum wraps (2^63 + 2^63 is 0, accepted); here it is an invalid duration.
        d = d
            .checked_add(v)
            .filter(|&d| d <= LIMIT)
            .ok_or_else(invalid)?;
    }
    if neg {
        return Ok((d as i64).wrapping_neg());
    }
    if d > LIMIT - 1 {
        return Err(invalid());
    }
    Ok(d as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_round_trip() {
        for ts in [
            "1970-01-01T00:00:00.000000000Z",
            "2026-10-19T08:14:00.000213904Z",
            "2028-02-29T23:59:59.999999999Z",
            "1677-09-21T00:12:43.145224192Z",
            "2262-04-11T23:47:16.854775807Z",
        ] {
            let ns = parse_ts_shape(ts).unwrap();
            assert!(in_range(ns));
            assert_eq!(format_ts(ns as i64), ts);
        }
        assert_eq!(parse_ts_shape("2026-02-29T00:00:00.000000000Z"), None);
        assert_eq!(parse_ts_shape("2026-10-19T24:00:00.000000000Z"), None);
        assert!(!in_range(
            parse_ts_shape("2262-04-11T23:47:16.854775808Z").unwrap()
        ));
        assert!(valid_day("2028-02-29") && !valid_day("2026-02-30") && !valid_day("2026-1-01"));
    }

    #[test]
    fn rfc3339_as_go() {
        let ns = |s| parse_rfc3339(s).unwrap();
        assert_eq!(ns("2026-10-19T08:00:00Z"), ns("2026-10-19T10:00:00+02:00"));
        assert_eq!(ns("2026-10-19T8:00:00.5Z") % 1_000_000_000, 500_000_000);
        assert_eq!(
            ns("2026-10-19T08:00:00,123456789999Z") % 1_000_000_000,
            123_456_789
        );
        assert_eq!(
            parse_rfc3339("yesterday").unwrap_err(),
            r#"parsing time "yesterday" as "2006-01-02T15:04:05.999999999Z07:00": cannot parse "yesterday" as "2006""#
        );
        assert_eq!(
            parse_rfc3339("2026-02-30T00:00:00Z").unwrap_err(),
            r#"parsing time "2026-02-30T00:00:00Z": day out of range"#
        );
        assert_eq!(
            parse_rfc3339("2026-10-19T08:00:00Zx").unwrap_err(),
            r#"parsing time "2026-10-19T08:00:00Zx": extra text: "x""#
        );
        assert_eq!(
            parse_rfc3339("2026-13-19T08:00:00Z").unwrap_err(),
            r#"parsing time "2026-13-19T08:00:00Z": month out of range"#
        );
        assert!(parse_rfc3339("2026-10-19T08:00:00+24:00").is_ok());
        assert!(parse_rfc3339("2026-10-19 08:00:00Z").is_err());
    }

    #[test]
    fn durations_as_go() {
        assert_eq!(parse_duration("-1h"), Ok(-3_600_000_000_000));
        assert_eq!(parse_duration("+1h30m"), Ok(5_400_000_000_000));
        assert_eq!(parse_duration("-1.5h"), Ok(-5_400_000_000_000));
        assert_eq!(parse_duration("-0"), Ok(0));
        assert_eq!(parse_duration("1µs"), Ok(1_000));
        assert_eq!(
            parse_duration("-1d").unwrap_err(),
            r#"time: unknown unit "d" in duration "-1d""#
        );
        assert_eq!(
            parse_duration("-1").unwrap_err(),
            r#"time: missing unit in duration "-1""#
        );
        assert_eq!(
            parse_duration("-x").unwrap_err(),
            r#"time: invalid duration "-x""#
        );
        assert!(parse_duration("+2562048h").is_err());
        assert!(parse_duration("+2562047h").is_ok());
        // Each term at the limit: no overflow panic, invalid (Go wrapped the sum to 0).
        assert_eq!(
            parse_duration("+9223372036854775808ns9223372036854775808ns").unwrap_err(),
            r#"time: invalid duration "+9223372036854775808ns9223372036854775808ns""#
        );
        assert_eq!(parse_duration("-9223372036854775808ns"), Ok(i64::MIN));
    }
}
