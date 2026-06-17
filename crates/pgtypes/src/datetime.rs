//! SP37: date/time *values* — the `Interval` type plus parsing, formatting,
//! binary encodings, and value arithmetic. PostgreSQL semantics; `jiff` does the
//! calendar/timezone math. This is the single source of truth for date/time
//! values (the SP32 `numeric` module pattern).

use jiff::civil::{Date, DateTime, Time};
use jiff::tz::{Offset, TimeZone};
use jiff::{Timestamp, ToSpan};

use crate::TypeError;

// ---------------------------------------------------------------------------
// Value-level arithmetic helpers (called from pgtypes::ops)
// ---------------------------------------------------------------------------

/// Add two intervals field-wise with overflow checking.
pub fn add_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    let months = a.months.checked_add(b.months).ok_or(TypeError::Overflow)?;
    let days = a.days.checked_add(b.days).ok_or(TypeError::Overflow)?;
    let micros = a.micros.checked_add(b.micros).ok_or(TypeError::Overflow)?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Subtract two intervals field-wise with overflow checking.
pub fn sub_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    let neg = neg_interval(b)?;
    add_interval(a, neg)
}

/// Negate an interval field-wise with overflow checking.
pub fn neg_interval(a: Interval) -> Result<Interval, TypeError> {
    let months = a.months.checked_neg().ok_or(TypeError::Overflow)?;
    let days = a.days.checked_neg().ok_or(TypeError::Overflow)?;
    let micros = a.micros.checked_neg().ok_or(TypeError::Overflow)?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Multiply an interval by a scalar factor. PostgreSQL distributes the factor
/// over each field and spills any fractional months into days (30-day month)
/// and fractional days into microseconds (86400000000 µs/day), matching PG's
/// `interval_mul` behaviour.
pub fn mul_interval(a: Interval, factor: f64) -> Result<Interval, TypeError> {
    if !factor.is_finite() {
        return Err(TypeError::Overflow);
    }
    // Scale months; carry the fraction down to days.
    let months_f = f64::from(a.months) * factor;
    let months_whole = months_f.trunc();
    let months_frac = months_f.fract();
    let months = months_whole as i64;

    // Spill fractional months → days (PG uses 30-day month).
    let days_from_months = months_frac * 30.0;
    let days_f = f64::from(a.days) * factor + days_from_months;
    let days_whole = days_f.trunc();
    let days_frac = days_f.fract();
    let days = days_whole as i64;

    // Spill fractional days → micros.
    let micros_from_days = days_frac * USECS_PER_DAY_I64 as f64;
    let micros_f = a.micros as f64 * factor + micros_from_days;

    // Range-check the fields (interval fields are i32/i64).
    let months = i32::try_from(months).map_err(|_| TypeError::Overflow)?;
    let days = i32::try_from(days).map_err(|_| TypeError::Overflow)?;
    // Guard micros: a finite f64 larger than i64::MAX (= 2^63 = 9.22e18) would
    // silently saturate to i64::MAX on `as i64` cast; reject it explicitly.
    // `i64::MAX as f64` rounds up to exactly 2^63, so `>= 2^63` is the right bound.
    if !micros_f.is_finite() || micros_f.abs() >= 9_223_372_036_854_775_808.0_f64 {
        return Err(TypeError::Overflow);
    }
    let micros = micros_f.round() as i64;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Divide an interval by a scalar divisor (zero → 22012).
pub fn div_interval(a: Interval, divisor: f64) -> Result<Interval, TypeError> {
    if divisor == 0.0 {
        return Err(TypeError::DivisionByZero);
    }
    mul_interval(a, 1.0 / divisor)
}

/// Add `days` to a `Date`, returning the new `Date` (overflow → 22008).
pub fn date_plus_days(d: Date, days: i64) -> Result<Date, TypeError> {
    d.checked_add(days.days())
        .map_err(|_| TypeError::DatetimeFieldOverflow {
            value: days.to_string(),
        })
}

/// Subtract two dates, returning the number of days between them (a - b).
pub fn date_diff_days(a: Date, b: Date) -> i32 {
    a.since((jiff::Unit::Day, b))
        .map(|span| span.get_days())
        .expect("difference of in-range date values always fits in a Span")
}

/// Promote a `Date` to a civil `DateTime` at midnight.
pub fn date_to_midnight(d: Date) -> DateTime {
    d.to_datetime(Time::midnight())
}

/// Add an `Interval` to a `Date` (PG: promotes date→midnight timestamp first),
/// returning a `DateTime`.  Applies months, then days, then micros in order
/// (calendar-aware via jiff `Span`).
pub fn date_plus_interval(d: Date, iv: Interval) -> Result<DateTime, TypeError> {
    timestamp_plus_interval(date_to_midnight(d), iv)
}

/// Add an `Interval` to a `DateTime`. Applies months, days, and micros in
/// sequence so that `+1 month` lands on the correct calendar date and only then
/// the time offset is applied.
pub fn timestamp_plus_interval(ts: DateTime, iv: Interval) -> Result<DateTime, TypeError> {
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: "interval arithmetic".into(),
    };
    // Apply months (calendar-aware, e.g. Jan 31 + 1 month → Feb 28/29).
    let after_months = if iv.months != 0 {
        ts.checked_add(iv.months.months()).map_err(overflow)?
    } else {
        ts
    };
    // Apply days (calendar-aware, skips DST ambiguity for civil datetimes).
    let after_days = if iv.days != 0 {
        after_months.checked_add(iv.days.days()).map_err(overflow)?
    } else {
        after_months
    };
    // Apply microseconds.
    let result = if iv.micros != 0 {
        after_days
            .checked_add(iv.micros.microseconds())
            .map_err(overflow)?
    } else {
        after_days
    };
    Ok(result)
}

/// Compute `a - b` for two `DateTime` values, returning an `Interval` with
/// months = 0 (PG's `timestamp - timestamp` result: total micros, stored in
/// the days + micros fields — days for full 86400 µs days, remainder in micros).
pub fn timestamp_diff(a: DateTime, b: DateTime) -> Interval {
    let total_micros = a
        .since((jiff::Unit::Microsecond, b))
        .map(|span| span.get_microseconds())
        .expect("difference of in-range timestamp values always fits in a Span");
    // Split into whole days + remaining micros (matching PG's interval storage).
    let days = (total_micros / USECS_PER_DAY_I64) as i32;
    let micros = total_micros % USECS_PER_DAY_I64;
    Interval {
        months: 0,
        days,
        micros,
    }
}

/// Add an `Interval` to a `Time`, returning the new `Time`. PostgreSQL's
/// `time + interval` uses ONLY the interval's microseconds component — a `time`
/// has no date, so the interval's `months`/`days` are ignored — and wraps the
/// result modulo 24 h (`time '23:00' + interval '2 hours'` → `01:00:00`,
/// `time '12:00' + interval '1 day'` → `12:00:00`).
pub fn time_plus_interval(t: Time, iv: Interval) -> Time {
    // Micros-of-day of the input time.
    let base = i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000);
    // Add the interval micros and wrap into [0, 86_400_000_000) (the `.rem_euclid`
    // keeps a negative shift positive, so `time '00:30' - interval '1 hour'`
    // wraps to `23:30:00`).
    let micros = (base + iv.micros).rem_euclid(USECS_PER_DAY_I64);
    let hour = (micros / 3_600_000_000) as i8;
    let rem = micros % 3_600_000_000;
    let minute = (rem / 60_000_000) as i8;
    let rem = rem % 60_000_000;
    let second = (rem / 1_000_000) as i8;
    let nanos = ((rem % 1_000_000) * 1_000) as i32;
    Time::new(hour, minute, second, nanos)
        .expect("a micros-of-day in [0, 86_400_000_000) is always a valid Time")
}

/// Combine a `Date` and a `Time` into a `DateTime` (PostgreSQL's `date + time`
/// and `time + date` → `timestamp`).
pub fn combine_date_time(d: Date, t: Time) -> DateTime {
    d.to_datetime(t)
}

/// Add an `Interval` to a `timestamptz` instant, calendar-aware in `tz`. The
/// months and days are applied to the WALL-CLOCK time in the session zone (so a
/// `+1 day` across a DST boundary lands on the same wall-clock time the next day,
/// not exactly 24 h later), while the microseconds are an absolute (instant)
/// shift. This is tz-aware, so it lives here (used from the executor's
/// `apply_binary`, which has the session zone) rather than in `pgtypes::ops`.
pub fn timestamptz_plus_interval(
    ts: Timestamp,
    iv: Interval,
    tz: &TimeZone,
) -> Result<Timestamp, TypeError> {
    let overflow = |_| TypeError::DatetimeFieldOverflow {
        value: "interval arithmetic".into(),
    };
    // Apply the calendar (months, then days) to the zoned wall-clock time.
    let zoned = ts.to_zoned(tz.clone());
    let after_cal = if iv.months != 0 || iv.days != 0 {
        zoned
            .checked_add(iv.months.months())
            .and_then(|z| z.checked_add(iv.days.days()))
            .map_err(overflow)?
    } else {
        zoned
    };
    // Apply the microseconds as an absolute (instant) shift.
    let after_micros = if iv.micros != 0 {
        after_cal
            .checked_add(iv.micros.microseconds())
            .map_err(overflow)?
    } else {
        after_cal
    };
    Ok(after_micros.timestamp())
}

/// Compute `a - b` for two `timestamptz` instants, returning an `Interval` of pure
/// micros (split into whole days + remainder, matching PG's interval storage). The
/// subtraction is on absolute instants, so no time zone is needed.
pub fn timestamptz_diff(a: Timestamp, b: Timestamp) -> Interval {
    let total_micros = a.as_microsecond() - b.as_microsecond();
    let days = (total_micros / USECS_PER_DAY_I64) as i32;
    let micros = total_micros % USECS_PER_DAY_I64;
    Interval {
        months: 0,
        days,
        micros,
    }
}

/// A PostgreSQL `interval`: months, days, and microseconds kept SEPARATE (PG does
/// not fold `1 month` into `30 days` for storage/arithmetic — only for ordering).
#[derive(Debug, Clone, Copy)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

const USECS_PER_DAY: i128 = 86_400_000_000;

impl Interval {
    /// PostgreSQL's `interval_cmp` canonical value: a 30-day month and 24-hour
    /// day estimate, in microseconds, as `i128` to avoid overflow.
    pub fn canonical_micros(&self) -> i128 {
        (i128::from(self.months) * 30 + i128::from(self.days)) * USECS_PER_DAY
            + i128::from(self.micros)
    }
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_micros() == other.canonical_micros()
    }
}
impl Eq for Interval {}
impl std::hash::Hash for Interval {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.canonical_micros().hash(state);
    }
}
impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical_micros().cmp(&other.canonical_micros())
    }
}

// ---------------------------------------------------------------------------
// Epoch constants. PostgreSQL stores `date`/`timestamp` relative to 2000-01-01
// (the PostgreSQL epoch), NOT the Unix epoch. `timestamptz` is an absolute
// instant; its binary wire form is µs since the PG epoch in UTC.
// ---------------------------------------------------------------------------

/// The PostgreSQL epoch (`2000-01-01`) as a `jiff` civil date.
fn pg_epoch_date() -> Date {
    Date::constant(2000, 1, 1)
}

/// The PostgreSQL epoch (`2000-01-01 00:00:00`) as a civil datetime.
fn pg_epoch_datetime() -> DateTime {
    DateTime::constant(2000, 1, 1, 0, 0, 0, 0)
}

/// Microseconds in one calendar day (24h estimate — civil days are always 24h).
const USECS_PER_DAY_I64: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// Sub-second rendering. PostgreSQL prints the fractional seconds only when
// non-zero, trimming trailing zeros (so `.450000` → `.45`, `.500000` → `.5`).
// jiff exposes the sub-second component as nanoseconds; PG's resolution is
// microseconds, so we render up to six fractional digits.
// ---------------------------------------------------------------------------

/// Append `.ffffff` (trailing zeros trimmed) for a non-zero sub-second part.
/// `subsec_nanos` is the time's nanosecond-of-second; PG truncates to µs.
fn push_subsecond(out: &mut String, subsec_nanos: i32) {
    let micros = subsec_nanos / 1_000;
    if micros == 0 {
        return;
    }
    // Six zero-padded digits, then strip trailing zeros (always leaves ≥1).
    let mut frac = format!("{micros:06}");
    while frac.ends_with('0') {
        frac.pop();
    }
    out.push('.');
    out.push_str(&frac);
}

// ---------------------------------------------------------------------------
// date
// ---------------------------------------------------------------------------

/// Parse a `date` literal in `YYYY-MM-DD` form. A well-formed shape whose fields
/// jiff rejects (e.g. `2023-02-29`) is a field overflow (22008); anything that
/// does not even look like a date is a format error (22007).
pub fn parse_date(s: &str) -> Result<Date, TypeError> {
    let t = s.trim();
    // Shape check: exactly three `-`-separated all-digit fields (optionally a
    // leading `-` for a BC-ish negative year is NOT supported here — PG uses an
    // `AD`/`BC` suffix; out of scope for this slice).
    let parts: Vec<&str> = t.split('-').collect();
    let well_shaped = parts.len() == 3
        && !parts[0].is_empty()
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    match t.parse::<Date>() {
        Ok(d) => Ok(d),
        Err(_) if well_shaped => Err(TypeError::DatetimeFieldOverflow {
            value: s.to_string(),
        }),
        Err(_) => Err(TypeError::InvalidDatetimeFormat {
            type_name: "date",
            value: s.to_string(),
        }),
    }
}

/// Render a `date` as ISO `YYYY-MM-DD` (PostgreSQL `date_out`, ISO datestyle).
pub fn date_to_text(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), d.month(), d.day())
}

/// `date_send`: i32 big-endian days since the PostgreSQL epoch (2000-01-01).
pub fn date_to_binary(d: Date) -> [u8; 4] {
    // `since` with largest unit Day yields a Span carrying only `days`.
    let days = d
        .since((jiff::Unit::Day, pg_epoch_date()))
        .map(|span| span.get_days())
        .expect("difference from a valid date to the PG epoch always fits");
    days.to_be_bytes()
}

/// `date_recv`: i32 big-endian days since the PostgreSQL epoch.
pub fn date_from_binary(b: &[u8]) -> Result<Date, TypeError> {
    let arr: [u8; 4] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "date",
        value: format!("{b:?}"),
    })?;
    let days = i32::from_be_bytes(arr);
    pg_epoch_date()
        .checked_add(days.days())
        .map_err(|_| TypeError::DatetimeFieldOverflow {
            value: days.to_string(),
        })
}

// ---------------------------------------------------------------------------
// time without time zone
// ---------------------------------------------------------------------------

/// Parse a `time` literal in `HH:MM[:SS[.ffffff]]` form.
pub fn parse_time(s: &str) -> Result<Time, TypeError> {
    let t = s.trim();
    parse_time_inner(t).ok_or_else(|| TypeError::InvalidDatetimeFormat {
        type_name: "time without time zone",
        value: s.to_string(),
    })
}

/// Best-effort `HH:MM[:SS[.ffffff]]` parse (returns `None` on any malformation).
/// jiff's `Time` FromStr requires `HH:MM:SS`, so we normalize `HH:MM` ourselves.
fn parse_time_inner(t: &str) -> Option<Time> {
    // jiff parses `HH:MM:SS[.fff]`; supply `:00` seconds when omitted.
    let normalized = match t.split(':').count() {
        2 => format!("{t}:00"),
        3 => t.to_string(),
        _ => return None,
    };
    normalized.parse::<Time>().ok()
}

/// Render a `time` as `HH:MM:SS[.ffffff]` (PostgreSQL `time_out`).
pub fn time_to_text(t: Time) -> String {
    let mut out = format!("{:02}:{:02}:{:02}", t.hour(), t.minute(), t.second());
    push_subsecond(&mut out, t.subsec_nanosecond());
    out
}

/// `time_send`: i64 big-endian microseconds since midnight.
pub fn time_to_binary(t: Time) -> [u8; 8] {
    let micros = i64::from(t.hour()) * 3_600_000_000
        + i64::from(t.minute()) * 60_000_000
        + i64::from(t.second()) * 1_000_000
        + i64::from(t.subsec_nanosecond() / 1_000);
    micros.to_be_bytes()
}

/// `time_recv`: i64 big-endian microseconds since midnight.
pub fn time_from_binary(b: &[u8]) -> Result<Time, TypeError> {
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "time without time zone",
        value: format!("{b:?}"),
    })?;
    let mut micros = i64::from_be_bytes(arr);
    let hour = (micros / 3_600_000_000) as i8;
    micros %= 3_600_000_000;
    let minute = (micros / 60_000_000) as i8;
    micros %= 60_000_000;
    let second = (micros / 1_000_000) as i8;
    micros %= 1_000_000;
    let nanos = (micros * 1_000) as i32;
    Time::new(hour, minute, second, nanos).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: i64::from_be_bytes(arr).to_string(),
    })
}

// ---------------------------------------------------------------------------
// timestamp without time zone
// ---------------------------------------------------------------------------

/// Parse a `timestamp` literal: `YYYY-MM-DD{ |T}HH:MM[:SS[.ffffff]]`. jiff accepts
/// a space or `T`/`t` separator natively, but requires `HH:MM:SS` for the time,
/// so we split on the separator and reuse the `time` normalization.
pub fn parse_timestamp(s: &str) -> Result<DateTime, TypeError> {
    let t = s.trim();
    parse_timestamp_inner(t).ok_or_else(|| TypeError::InvalidDatetimeFormat {
        type_name: "timestamp without time zone",
        value: s.to_string(),
    })
}

/// Split a civil datetime into its date and time around a space/`T` separator,
/// parsing each part (normalizing the time's optional seconds). Returns `None`
/// on any malformation.
fn parse_timestamp_inner(t: &str) -> Option<DateTime> {
    // Find the date/time separator: the first ` `, `T`, or `t`.
    let sep = t.find([' ', 'T', 't'])?;
    let date_part = &t[..sep];
    let time_part = &t[sep + 1..];
    let date = date_part.parse::<Date>().ok()?;
    let time = parse_time_inner(time_part)?;
    Some(date.to_datetime(time))
}

/// Render a `timestamp` as `YYYY-MM-DD HH:MM:SS[.ffffff]` (SPACE separator —
/// PostgreSQL `timestamp_out`, ISO datestyle).
pub fn timestamp_to_text(ts: DateTime) -> String {
    let d = ts.date();
    let tm = ts.time();
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        d.year(),
        d.month(),
        d.day(),
        tm.hour(),
        tm.minute(),
        tm.second()
    );
    push_subsecond(&mut out, tm.subsec_nanosecond());
    out
}

/// `timestamp_send`: i64 big-endian microseconds since the PG epoch.
pub fn timestamp_to_binary(ts: DateTime) -> [u8; 8] {
    let micros = ts
        .since((jiff::Unit::Microsecond, pg_epoch_datetime()))
        .map(|span| span.get_microseconds())
        .expect("difference from a valid timestamp to the PG epoch always fits");
    micros.to_be_bytes()
}

/// `timestamp_recv`: i64 big-endian microseconds since the PG epoch.
pub fn timestamp_from_binary(b: &[u8]) -> Result<DateTime, TypeError> {
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "timestamp without time zone",
        value: format!("{b:?}"),
    })?;
    let micros = i64::from_be_bytes(arr);
    pg_epoch_datetime()
        .checked_add(micros.microseconds())
        .map_err(|_| TypeError::DatetimeFieldOverflow {
            value: micros.to_string(),
        })
}

// ---------------------------------------------------------------------------
// timestamp with time zone
// ---------------------------------------------------------------------------

/// Parse a `timestamptz` literal into an absolute instant. If the text carries an
/// explicit offset (`Z`/`z` or `±HH[:MM[:SS]]`), that offset fixes the instant;
/// otherwise the wall-clock time is interpreted as local to the session `tz`.
pub fn parse_timestamptz(s: &str, tz: &TimeZone) -> Result<Timestamp, TypeError> {
    let t = s.trim();
    let err = || TypeError::InvalidDatetimeFormat {
        type_name: "timestamp with time zone",
        value: s.to_string(),
    };
    let (civil_str, offset) = split_offset(t);
    let dt = parse_timestamp_inner(civil_str).ok_or_else(err)?;
    match offset {
        // Explicit offset: the instant is the civil time minus the offset.
        Some(off) => off
            .to_timestamp(dt)
            .map_err(|_| TypeError::DatetimeFieldOverflow {
                value: s.to_string(),
            }),
        // No offset: interpret the wall clock as local to `tz`.
        None => dt.to_zoned(tz.clone()).map(|z| z.timestamp()).map_err(|_| {
            TypeError::DatetimeFieldOverflow {
                value: s.to_string(),
            }
        }),
    }
}

/// Split a trailing UTC-offset designator off a civil-datetime string. Returns
/// the civil portion and the parsed offset (if any). Recognizes `Z`/`z` (UTC) and
/// `±HH`, `±HH:MM`, `±HH:MM:SS` (and the colon-less `±HHMM` form).
fn split_offset(t: &str) -> (&str, Option<Offset>) {
    if let Some(stripped) = t.strip_suffix(['Z', 'z']) {
        return (stripped, Some(Offset::UTC));
    }
    // Scan for the offset sign AFTER the time portion. The date uses `-` as a
    // field separator, so only a `+`/`-` at/after the time can begin an offset;
    // we find the date/time separator first and search the time portion only.
    let sep = match t.find([' ', 'T', 't']) {
        Some(i) => i,
        None => return (t, None),
    };
    let time_region = &t[sep + 1..];
    // The offset sign is the first `+` or `-` in the time region.
    if let Some(rel) = time_region.find(['+', '-']) {
        let abs = sep + 1 + rel;
        let civil = &t[..abs];
        let off_str = &t[abs..];
        if let Some(off) = parse_offset_str(off_str) {
            return (civil, Some(off));
        }
    }
    (t, None)
}

/// Parse a `±HH[:MM[:SS]]` (or `±HHMM`/`±HHMMSS`) UTC offset into seconds.
fn parse_offset_str(s: &str) -> Option<Offset> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1i32, &s[1..]),
        b'-' => (-1i32, &s[1..]),
        _ => return None,
    };
    let (h, m, sec) = if rest.contains(':') {
        let mut it = rest.split(':');
        let h = it.next()?;
        let m = it.next().unwrap_or("0");
        let sec = it.next().unwrap_or("0");
        if it.next().is_some() {
            return None;
        }
        (h, m, sec)
    } else {
        // Colon-less: HH, HHMM, or HHMMSS.
        match rest.len() {
            1 | 2 => (rest, "0", "0"),
            4 => (&rest[..2], &rest[2..4], "0"),
            6 => (&rest[..2], &rest[2..4], &rest[4..6]),
            _ => return None,
        }
    };
    let hours: i32 = h.parse().ok()?;
    let mins: i32 = m.parse().ok()?;
    let secs: i32 = sec.parse().ok()?;
    let total = sign * (hours * 3600 + mins * 60 + secs);
    Offset::from_seconds(total).ok()
}

/// Render a `timestamptz` instant in `tz`: `YYYY-MM-DD HH:MM:SS[.ffffff]±HH[:MM[:SS]]`
/// (PostgreSQL `timestamptz_out`, ISO datestyle). The offset suffix shows `:MM`/`:SS`
/// only when non-zero.
pub fn timestamptz_to_text(ts: Timestamp, tz: &TimeZone) -> String {
    let dt = tz.to_datetime(ts);
    let off = tz.to_offset(ts);
    let mut out = timestamp_to_text(dt);
    push_offset(&mut out, off);
    out
}

/// Append a PostgreSQL-style offset suffix `±HH`, with `:MM` and `:SS` added only
/// when those finer components are non-zero.
fn push_offset(out: &mut String, off: Offset) {
    let total = off.seconds();
    let sign = if total < 0 { '-' } else { '+' };
    let abs = total.unsigned_abs();
    let hours = abs / 3600;
    let mins = (abs % 3600) / 60;
    let secs = abs % 60;
    out.push(sign);
    out.push_str(&format!("{hours:02}"));
    if mins != 0 || secs != 0 {
        out.push_str(&format!(":{mins:02}"));
        if secs != 0 {
            out.push_str(&format!(":{secs:02}"));
        }
    }
}

/// `timestamptz_send`: i64 big-endian microseconds since the PG epoch (UTC).
pub fn timestamptz_to_binary(ts: Timestamp) -> [u8; 8] {
    // Unix-epoch µs, then rebase to the PG epoch (2000-01-01 is 946684800s after
    // the Unix epoch).
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let unix_micros = ts.as_microsecond();
    let micros = unix_micros - PG_EPOCH_UNIX_SECS * 1_000_000;
    micros.to_be_bytes()
}

/// `timestamptz_recv`: i64 big-endian microseconds since the PG epoch (UTC).
pub fn timestamptz_from_binary(b: &[u8]) -> Result<Timestamp, TypeError> {
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let arr: [u8; 8] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "timestamp with time zone",
        value: format!("{b:?}"),
    })?;
    let pg_micros = i64::from_be_bytes(arr);
    let unix_micros = pg_micros + PG_EPOCH_UNIX_SECS * 1_000_000;
    Timestamp::from_microsecond(unix_micros).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: pg_micros.to_string(),
    })
}

// ---------------------------------------------------------------------------
// interval
// ---------------------------------------------------------------------------

/// Parse a PostgreSQL verbose `interval`: a sequence of signed `<qty> <unit>`
/// terms and/or a `[-]HH:MM[:SS[.ffffff]]` clock term. Fractional quantities spill
/// into the next-smaller unit (PG rule); weeks fold to days, years to months.
pub fn parse_interval(s: &str) -> Result<Interval, TypeError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(TypeError::InvalidDatetimeFormat {
            type_name: "interval",
            value: s.to_string(),
        });
    }
    let err = || TypeError::InvalidDatetimeFormat {
        type_name: "interval",
        value: s.to_string(),
    };

    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i128 = 0;

    let tokens: Vec<&str> = t.split_whitespace().collect();
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        // A clock term `[-]HH:MM[:SS[.ffffff]]` stands alone (no trailing unit).
        if tok.contains(':') {
            micros += parse_clock_term(tok).ok_or_else(err)? as i128;
            i += 1;
            continue;
        }
        // Otherwise a `<qty> <unit>` pair.
        let qty: f64 = tok.parse().map_err(|_| err())?;
        let unit = tokens.get(i + 1).ok_or_else(err)?;
        accumulate_unit(qty, unit, &mut months, &mut days, &mut micros).ok_or_else(err)?;
        i += 2;
    }

    let months = i32::try_from(months).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })?;
    let days = i32::try_from(days).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })?;
    let micros = i64::try_from(micros).map_err(|_| TypeError::DatetimeFieldOverflow {
        value: s.to_string(),
    })?;
    Ok(Interval {
        months,
        days,
        micros,
    })
}

/// Parse a `[-]HH:MM[:SS[.ffffff]]` clock term into signed microseconds.
fn parse_clock_term(tok: &str) -> Option<i64> {
    let (sign, rest) = match tok.strip_prefix('-') {
        Some(r) => (-1i64, r),
        None => (1i64, tok.strip_prefix('+').unwrap_or(tok)),
    };
    let mut parts = rest.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let (s_whole, s_frac_micros) = match parts.next() {
        Some(sec) => {
            if let Some((whole, frac)) = sec.split_once('.') {
                let whole: i64 = whole.parse().ok()?;
                // Pad/truncate the fraction to six µs digits.
                let mut frac_digits = frac.to_string();
                while frac_digits.len() < 6 {
                    frac_digits.push('0');
                }
                let micros: i64 = frac_digits[..6].parse().ok()?;
                (whole, micros)
            } else {
                (sec.parse().ok()?, 0)
            }
        }
        None => (0, 0),
    };
    if parts.next().is_some() {
        return None;
    }
    let total = h * 3_600_000_000 + m * 60_000_000 + s_whole * 1_000_000 + s_frac_micros;
    Some(sign * total)
}

/// Add one `<qty> <unit>` term, spilling a fractional quantity into the next
/// smaller field (PG semantics). Returns `None` for an unknown unit.
fn accumulate_unit(
    qty: f64,
    unit: &str,
    months: &mut i64,
    days: &mut i64,
    micros: &mut i128,
) -> Option<()> {
    let u = unit.trim_end_matches('s').to_ascii_lowercase();
    // The whole part of `qty`; the fractional part spills down.
    let whole = qty.trunc() as i64;
    let frac = qty.fract();
    match u.as_str() {
        "year" | "yr" => {
            *months += whole * 12;
            // Fractional years → months.
            *micros += 0; // (no µs contribution)
            *months += (frac * 12.0).round() as i64;
        }
        "month" | "mon" => {
            *months += whole;
            // Fractional months → days (PG uses a 30-day month).
            *days += (frac * 30.0).trunc() as i64;
            let day_frac = (frac * 30.0).fract();
            *micros += (day_frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        "week" | "wk" => {
            *days += whole * 7;
            *micros += (frac * 7.0 * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        "day" => {
            *days += whole;
            *micros += (frac * USECS_PER_DAY_I64 as f64).round() as i128;
        }
        "hour" | "hr" | "h" => {
            *micros += whole as i128 * 3_600_000_000;
            *micros += (frac * 3_600_000_000.0).round() as i128;
        }
        "minute" | "min" | "m" => {
            *micros += whole as i128 * 60_000_000;
            *micros += (frac * 60_000_000.0).round() as i128;
        }
        "second" | "sec" | "s" => {
            *micros += whole as i128 * 1_000_000;
            *micros += (frac * 1_000_000.0).round() as i128;
        }
        "millisecond" | "msec" | "ms" => {
            *micros += (qty * 1_000.0).round() as i128;
        }
        "microsecond" | "usec" | "us" => {
            *micros += qty.round() as i128;
        }
        _ => return None,
    }
    Some(())
}

/// Render an `interval` in PostgreSQL's `postgres` IntervalStyle (the default):
/// `[<y> year[s]] [<m> mons] [<d> days] [±HH:MM:SS[.ffffff]]`; a fully-zero
/// interval prints `00:00:00`.
pub fn interval_to_text(iv: Interval) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Year/month component, derived from total months. PostgreSQL pluralizes the
    // unit name unless the value is *exactly* 1 (so `-1` and `2` are plural, only
    // `1` is singular — `1 year`, `-1 days`, `2 mons`).
    let years = iv.months / 12;
    let mons = iv.months % 12;
    if years != 0 {
        parts.push(format!("{years} year{}", if years == 1 { "" } else { "s" }));
    }
    if mons != 0 {
        parts.push(format!("{mons} mon{}", if mons == 1 { "" } else { "s" }));
    }
    // Day component.
    if iv.days != 0 {
        parts.push(format!(
            "{} day{}",
            iv.days,
            if iv.days == 1 { "" } else { "s" }
        ));
    }
    // Clock component (only when non-zero, OR when the whole interval is zero so
    // we have something to print).
    let has_clock = iv.micros != 0;
    if has_clock {
        parts.push(format_clock(iv.micros));
    }
    if parts.is_empty() {
        // A fully-zero interval prints the clock zero.
        return "00:00:00".to_string();
    }
    parts.join(" ")
}

/// Format the µs component of an interval as a signed `HH:MM:SS[.ffffff]` clock.
/// The sign applies to the whole clock (PG prints `-01:00:00`, not `01:-00:00`).
fn format_clock(total_micros: i64) -> String {
    let neg = total_micros < 0;
    let abs = total_micros.unsigned_abs();
    let hours = abs / 3_600_000_000;
    let rem = abs % 3_600_000_000;
    let mins = rem / 60_000_000;
    let rem = rem % 60_000_000;
    let secs = rem / 1_000_000;
    let micros = (rem % 1_000_000) as i32;
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.push_str(&format!("{hours:02}:{mins:02}:{secs:02}"));
    push_subsecond(&mut out, micros * 1_000);
    out
}

/// `interval_send`: i64 µs ++ i32 days ++ i32 months, all big-endian (16 bytes).
pub fn interval_to_binary(iv: Interval) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&iv.micros.to_be_bytes());
    out[8..12].copy_from_slice(&iv.days.to_be_bytes());
    out[12..16].copy_from_slice(&iv.months.to_be_bytes());
    out
}

/// `interval_recv`: i64 µs ++ i32 days ++ i32 months, all big-endian.
pub fn interval_from_binary(b: &[u8]) -> Result<Interval, TypeError> {
    let arr: [u8; 16] = b.try_into().map_err(|_| TypeError::InvalidDatetimeFormat {
        type_name: "interval",
        value: format!("{b:?}"),
    })?;
    let micros = i64::from_be_bytes(arr[0..8].try_into().expect("8-byte slice"));
    let days = i32::from_be_bytes(arr[8..12].try_into().expect("4-byte slice"));
    let months = i32::from_be_bytes(arr[12..16].try_into().expect("4-byte slice"));
    Ok(Interval {
        months,
        days,
        micros,
    })
}

#[cfg(test)]
mod interval_tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn h(i: &Interval) -> u64 {
        let mut s = DefaultHasher::new();
        i.hash(&mut s);
        s.finish()
    }

    #[test]
    fn interval_grouping_equality_uses_canonical_estimate() {
        let one_month = Interval {
            months: 1,
            days: 0,
            micros: 0,
        };
        let thirty_days = Interval {
            months: 0,
            days: 30,
            micros: 0,
        };
        assert_eq!(one_month, thirty_days);
        assert_eq!(h(&one_month), h(&thirty_days));
        let one_day = Interval {
            months: 0,
            days: 1,
            micros: 0,
        };
        let day_us = 86_400_000_000i64;
        let twentyfour_h = Interval {
            months: 0,
            days: 0,
            micros: day_us,
        };
        assert_eq!(one_day, twentyfour_h);
        assert_eq!(h(&one_day), h(&twentyfour_h));
        assert_ne!(one_month, one_day);
    }

    #[test]
    fn interval_ordering_is_by_canonical_estimate() {
        use std::cmp::Ordering;
        let a = Interval {
            months: 0,
            days: 1,
            micros: 0,
        };
        let b = Interval {
            months: 1,
            days: 0,
            micros: 0,
        };
        assert_eq!(a.cmp(&b), Ordering::Less);
        assert_eq!(a.canonical_micros(), 86_400_000_000i128);
        assert_eq!(b.canonical_micros(), 30 * 86_400_000_000i128);
    }
}

#[cfg(test)]
mod io_tests {
    use super::*;

    #[test]
    fn parse_and_format_date() {
        let d = parse_date("2024-02-29").expect("leap day");
        assert_eq!(date_to_text(d), "2024-02-29");
        assert!(matches!(
            parse_date("2023-02-29"),
            Err(crate::TypeError::DatetimeFieldOverflow { .. })
        ));
        assert!(matches!(
            parse_date("not-a-date"),
            Err(crate::TypeError::InvalidDatetimeFormat { .. })
        ));
    }

    #[test]
    fn parse_and_format_time_trims_subseconds() {
        assert_eq!(
            time_to_text(parse_time("12:34:56").expect("valid time")),
            "12:34:56"
        );
        assert_eq!(
            time_to_text(parse_time("12:34").expect("valid time")),
            "12:34:00"
        );
        assert_eq!(
            time_to_text(parse_time("01:02:03.450000").expect("valid time")),
            "01:02:03.45"
        );
    }

    #[test]
    fn parse_and_format_timestamp() {
        let ts = parse_timestamp("2024-01-15 13:45:00").expect("valid timestamp");
        assert_eq!(timestamp_to_text(ts), "2024-01-15 13:45:00");
        let ts2 = parse_timestamp("2024-01-15T13:45:00.5").expect("valid timestamp");
        assert_eq!(timestamp_to_text(ts2), "2024-01-15 13:45:00.5");
    }

    #[test]
    fn parse_and_format_timestamptz_uses_session_zone() {
        let tz = jiff::tz::TimeZone::get("America/New_York").expect("tzdb has NY");
        let ts = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("valid tstz");
        assert_eq!(timestamptz_to_text(ts, &tz), "2024-01-15 12:00:00-05");
        assert_eq!(
            timestamptz_to_text(ts, &jiff::tz::TimeZone::UTC),
            "2024-01-15 17:00:00+00"
        );
        let ts3 = parse_timestamptz("2024-01-15 12:00:00+02", &tz).expect("valid tstz");
        assert_eq!(
            timestamptz_to_text(ts3, &jiff::tz::TimeZone::UTC),
            "2024-01-15 10:00:00+00"
        );
    }

    #[test]
    fn parse_and_format_interval_postgres_style() {
        assert_eq!(
            interval_to_text(parse_interval("1 day").expect("valid interval")),
            "1 day"
        );
        assert_eq!(
            interval_to_text(parse_interval("1 year 2 months").expect("valid interval")),
            "1 year 2 mons"
        );
        assert_eq!(
            interval_to_text(parse_interval("3 days 04:05:06").expect("valid interval")),
            "3 days 04:05:06"
        );
        assert_eq!(
            interval_to_text(parse_interval("2 hours 30 minutes").expect("valid interval")),
            "02:30:00"
        );
        assert_eq!(
            interval_to_text(parse_interval("0 days").expect("valid interval")),
            "00:00:00"
        );
        assert_eq!(
            interval_to_text(parse_interval("-1 day").expect("valid interval")),
            "-1 days"
        );
    }

    // -----------------------------------------------------------------------
    // Teeth test: proves the CURRENT (unfixed) code saturates instead of
    // returning Err(Overflow).  After the fix this test must PASS.
    // -----------------------------------------------------------------------
    #[test]
    fn mul_interval_micros_overflow_is_caught() {
        // micros = i64::MAX, factor = 1000.0 → product ≈ 9.22e21, far above
        // i64::MAX; the fixed code must return Err(Overflow), not Ok with a
        // saturated i64::MAX value.
        let big = Interval {
            months: 0,
            days: 0,
            micros: i64::MAX,
        };
        assert!(
            matches!(mul_interval(big, 1000.0), Err(crate::TypeError::Overflow)),
            "expected Overflow but got a saturated Ok — fix the finite-range guard"
        );
    }

    #[test]
    fn binary_round_trips_through_pg_epoch() {
        let d = parse_date("2000-01-02").expect("valid date");
        assert_eq!(date_to_binary(d), 1i32.to_be_bytes());
        assert_eq!(date_from_binary(&date_to_binary(d)).expect("round-trip"), d);
        let i = Interval {
            months: 14,
            days: 3,
            micros: 4_000_000,
        };
        assert_eq!(
            interval_from_binary(&interval_to_binary(i)).expect("round-trip"),
            i
        );
    }
}

// ---------------------------------------------------------------------------
// SP37: mutation-killing tests — every arithmetic operator, match arm, binary
// encode/decode, parse/format internal, and the Interval/Datum eq/hash impls is
// pinned to its exact PG-faithful value so a cargo-mutants edit (`* → /`,
// `+ → -`, `|| → &&`, a deleted match arm, a `[0;8]`/`Default` body) breaks an
// assertion. Values cross-checked against PostgreSQL semantics.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod mutation_tests {
    use super::*;

    fn iv(months: i32, days: i32, micros: i64) -> Interval {
        Interval {
            months,
            days,
            micros,
        }
    }

    // -- mul_interval / div_interval -------------------------------------

    #[test]
    fn mul_interval_scales_each_field_exactly() {
        // A clean ×2 with no spill: each field doubles.
        assert_eq!(
            mul_interval(iv(1, 2, 3_000_000), 2.0).expect("mul"),
            iv(2, 4, 6_000_000)
        );
    }

    #[test]
    fn mul_interval_spills_fractional_months_into_days_and_micros() {
        // months 3 × 1.5 = 4.5 → 4 months, 0.5*30 = 15 days carried down (line 61
        // `months_frac * 30.0`); days 4*1.5 = 6, +15 = 21 (line 62 the `+`);
        // micros 6_000_000 × 1.5 = 9_000_000.
        assert_eq!(
            mul_interval(iv(3, 4, 6_000_000), 1.5).expect("mul"),
            iv(4, 21, 9_000_000)
        );
    }

    #[test]
    fn mul_interval_spills_fractional_days_into_micros() {
        // months 0; days 1 × 0.5 = 0.5 → 0 days, 0.5 day = 43_200_000_000 µs spilt
        // down (lines 68/69 `days_frac * USECS_PER_DAY` + `micros*factor`).
        assert_eq!(
            mul_interval(iv(0, 1, 0), 0.5).expect("mul"),
            iv(0, 0, 43_200_000_000)
        );
    }

    #[test]
    fn div_interval_by_nonzero_divides_each_field() {
        // /4 = ×0.25: months 2 → 0.5 → 0 mon + 15 days; days 4×0.25 = 1, +15 = 16;
        // micros 6_000_000 × 0.25 = 1_500_000.
        assert_eq!(
            div_interval(iv(2, 4, 6_000_000), 4.0).expect("div"),
            iv(0, 16, 1_500_000)
        );
        // A pure-micros interval /2 halves the micros.
        assert_eq!(
            div_interval(iv(0, 0, 7_000_000), 2.0).expect("div"),
            iv(0, 0, 3_500_000)
        );
    }

    #[test]
    fn div_interval_by_zero_is_division_by_zero() {
        // Line 90 `divisor == 0.0`: a zero divisor is 22012, NOT a pass-through.
        assert!(matches!(
            div_interval(iv(1, 1, 1), 0.0),
            Err(TypeError::DivisionByZero)
        ));
        // A non-zero divisor must NOT error (guards `== → !=`).
        assert!(div_interval(iv(1, 1, 1), 2.0).is_ok());
    }

    // -- timestamp_plus_interval ----------------------------------------

    #[test]
    fn timestamp_plus_interval_applies_months_then_micros() {
        // Non-zero months AND micros so both `iv.months != 0` (line 131) and
        // `iv.micros != 0` (line 143) branches are taken.
        let base = parse_timestamp("2024-01-31 10:00:00").expect("ts");
        // +1 month lands on Feb 29 (2024 leap), + 90 min → 11:30.
        let got = timestamp_plus_interval(base, iv(1, 0, 90 * 60_000_000)).expect("ok");
        assert_eq!(
            timestamp_to_text(got),
            "2024-02-29 11:30:00",
            "months applied calendar-aware before the micros offset"
        );
        // Days branch too (line 137 already covered by other tests, pin here).
        let got2 = timestamp_plus_interval(
            parse_timestamp("2024-01-01 00:00:00").expect("ts"),
            iv(0, 3, 0),
        )
        .expect("ok");
        assert_eq!(timestamp_to_text(got2), "2024-01-04 00:00:00");
    }

    // -- time_plus_interval ---------------------------------------------

    #[test]
    fn time_plus_interval_micros_of_day_math_is_exact() {
        // Construct base micros-of-day (lines 178-181: the `+` chain and the
        // `* / 1000` subsec) and the wrap (lines 185/190/191).
        let t = parse_time("23:59:59.500000").expect("t");
        // + 2 s wraps past midnight to 00:00:01.5.
        let got = time_plus_interval(t, iv(0, 0, 2_000_000));
        assert_eq!(time_to_text(got), "00:00:01.5");
        // A mid-day shift that exercises the hour/min/sec split (lines 186-191).
        let t2 = parse_time("01:02:03").expect("t");
        let got2 = time_plus_interval(t2, iv(0, 0, 3_600_000_000 + 60_000_000 + 1_000_000));
        assert_eq!(time_to_text(got2), "02:03:04");
        // The interval's days/months are ignored (only micros matter): adding the
        // micros for "12:00" with months/days set still wraps on micros alone.
        let t3 = parse_time("12:00:00").expect("t");
        let got3 = time_plus_interval(t3, iv(5, 9, 0));
        assert_eq!(time_to_text(got3), "12:00:00");
    }

    // -- timestamptz_plus_interval --------------------------------------

    #[test]
    fn timestamptz_plus_interval_applies_calendar_and_micros() {
        let tz = TimeZone::UTC;
        let base = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("base");
        // months != 0 (line 218 left), days != 0 (line 218 right), micros != 0
        // (line 227): +1 month +2 days +30 min.
        let got = timestamptz_plus_interval(base, iv(1, 2, 30 * 60_000_000), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got, &tz), "2024-02-17 12:30:00+00");
        // A pure-micros shift (months == 0 && days == 0, so the cal branch is the
        // `else`): +1 h.
        let got2 = timestamptz_plus_interval(base, iv(0, 0, 3_600_000_000), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got2, &tz), "2024-01-15 13:00:00+00");
        // months only (days == 0) — proves `||` is OR not AND (line 218): a value
        // with months but zero days must still apply the month.
        let got3 = timestamptz_plus_interval(base, iv(1, 0, 0), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got3, &tz), "2024-02-15 12:00:00+00");
        // days only (months == 0) — the other `||` operand.
        let got4 = timestamptz_plus_interval(base, iv(0, 5, 0), &tz).expect("ok");
        assert_eq!(timestamptz_to_text(got4, &tz), "2024-01-20 12:00:00+00");
    }

    // -- timestamptz_diff -----------------------------------------------

    #[test]
    fn timestamptz_diff_splits_days_and_remaining_micros() {
        // a − b a non-round number of µs apart: a = 2024-01-15 12:00:00.000000Z,
        // b = 2024-01-13 09:30:00.250000Z → 181_799_750_000 µs = 2 days +
        // 8_999_750_000 µs remainder (lines 241 `-`, 242 `/`, 243 `%`).
        let tz = TimeZone::UTC;
        let a = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("a");
        let b = parse_timestamptz("2024-01-13 09:30:00.250000", &tz).expect("b");
        assert_eq!(timestamptz_diff(a, b), iv(0, 2, 8_999_750_000));
        // The reverse is the negation (proves the `-` is a real subtraction).
        assert_eq!(timestamptz_diff(b, a), iv(0, -2, -8_999_750_000));
    }

    // -- Interval Hash / PartialOrd -------------------------------------

    #[test]
    fn interval_hash_and_partial_cmp_use_canonical_estimate() {
        use std::cmp::Ordering;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        fn h(i: &Interval) -> u64 {
            let mut s = DefaultHasher::new();
            i.hash(&mut s);
            s.finish()
        }
        // Canonically-equal intervals (1 month == 30 days) hash equally — kills
        // the `hash with ()` mutant (line 279) only if UNEQUAL intervals hash
        // DIFFERENTLY, so also assert that.
        assert_eq!(h(&iv(1, 0, 0)), h(&iv(0, 30, 0)));
        assert_ne!(h(&iv(1, 0, 0)), h(&iv(0, 1, 0)));
        // partial_cmp returns a real ordering (kills `partial_cmp -> None`, line 284).
        assert_eq!(iv(0, 1, 0).partial_cmp(&iv(1, 0, 0)), Some(Ordering::Less));
        assert_eq!(
            iv(1, 0, 0).partial_cmp(&iv(0, 1, 0)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            iv(0, 30, 0).partial_cmp(&iv(1, 0, 0)),
            Some(Ordering::Equal)
        );
        assert!(iv(0, 1, 0) < iv(1, 0, 0));
        assert!(iv(1, 0, 0) > iv(0, 1, 0));
    }

    // -- pg_epoch_datetime ----------------------------------------------

    #[test]
    fn pg_epoch_datetime_is_2000_01_01_midnight() {
        // Kills `pg_epoch_datetime -> Default::default()` (line 306): the PG epoch
        // is 2000-01-01, not jiff's default (0001-01-01 wall-clock zero). Observed
        // via timestamp_to_binary, whose reference point IS pg_epoch_datetime.
        let one_sec_after =
            parse_timestamp("2000-01-01 00:00:01").expect("ts one second after epoch");
        assert_eq!(
            timestamp_to_binary(one_sec_after),
            1_000_000i64.to_be_bytes(),
            "1 s after the PG epoch is 1_000_000 µs"
        );
    }

    // -- time_to_binary / time_from_binary ------------------------------

    #[test]
    fn time_to_binary_exact_bytes_and_round_trip() {
        // Known case: 00:00:01 → 1_000_000 µs (sanity-checked PG-correct).
        assert_eq!(
            time_to_binary(parse_time("00:00:01").expect("t")),
            1_000_000i64.to_be_bytes()
        );
        // Non-trivial: 13:45:06.123456 → 49_506_123_456 µs (lines 428-431 each
        // `*`/`+`/`/`). Wrong arithmetic ⇒ wrong bytes.
        let t = parse_time("13:45:06.123456").expect("t");
        assert_eq!(time_to_binary(t), 49_506_123_456i64.to_be_bytes());
        // Round-trip a non-trivial value (lines 442-448).
        assert_eq!(time_from_binary(&time_to_binary(t)).expect("round-trip"), t);
        // And from a known byte vector back to the exact time.
        assert_eq!(
            time_from_binary(&49_506_123_456i64.to_be_bytes()).expect("from"),
            t
        );
    }

    // -- timestamp_to_binary / timestamp_from_binary --------------------

    #[test]
    fn timestamp_to_binary_exact_bytes_and_round_trip() {
        // 2024-07-15 13:45:06.5 → 774_366_306_500_000 µs since the PG epoch.
        let ts = parse_timestamp("2024-07-15 13:45:06.5").expect("ts");
        assert_eq!(
            timestamp_to_binary(ts),
            774_366_306_500_000i64.to_be_bytes()
        );
        assert_eq!(
            timestamp_from_binary(&timestamp_to_binary(ts)).expect("round-trip"),
            ts
        );
        // A known byte vector decodes to the exact timestamp.
        assert_eq!(
            timestamp_from_binary(&774_366_306_500_000i64.to_be_bytes()).expect("from"),
            ts
        );
    }

    // -- timestamptz_to_binary / timestamptz_from_binary ----------------

    #[test]
    fn timestamptz_to_binary_exact_bytes_and_round_trip() {
        // Instant 2024-01-15 12:00:00 UTC → 758_635_200_000_000 µs since the PG
        // epoch (lines 649 `-`/`*`, 661 `+`/`*`).
        let tz = TimeZone::UTC;
        let ts = parse_timestamptz("2024-01-15 12:00:00", &tz).expect("tstz");
        assert_eq!(
            timestamptz_to_binary(ts),
            758_635_200_000_000i64.to_be_bytes()
        );
        assert_eq!(
            timestamptz_from_binary(&timestamptz_to_binary(ts)).expect("round-trip"),
            ts
        );
        assert_eq!(
            timestamptz_from_binary(&758_635_200_000_000i64.to_be_bytes()).expect("from"),
            ts
        );
        // An off-UTC instant also round-trips (the offset is absorbed into the
        // absolute instant, so the bytes match the equivalent UTC instant).
        let tz_ny = TimeZone::get("America/New_York").expect("NY");
        let ts2 = parse_timestamptz("2024-01-15 07:00:00", &tz_ny).expect("NY 07:00 = 12:00 UTC");
        assert_eq!(timestamptz_to_binary(ts2), timestamptz_to_binary(ts));
    }

    // -- parse_offset_str -----------------------------------------------

    #[test]
    fn parse_offset_str_spellings_and_sign() {
        let tz = TimeZone::UTC;
        // Helper: parse a tstz with an explicit offset and read back the instant
        // via UTC text, so the offset's effect on the instant is observable.
        let inst = |lit: &str| {
            timestamptz_to_text(
                parse_timestamptz(&format!("2024-01-15 12:00:00{lit}"), &tz).expect("parse"),
                &tz,
            )
        };
        // +05 → instant is 07:00 UTC (subtract the offset).
        assert_eq!(inst("+05"), "2024-01-15 07:00:00+00");
        // +0530 (colon-less HHMM, the `4 =>` arm) → 06:30 UTC.
        assert_eq!(inst("+0530"), "2024-01-15 06:30:00+00");
        // +053045 (colon-less HHMMSS, the `6 =>` arm) → 06:29:15 UTC.
        assert_eq!(inst("+053045"), "2024-01-15 06:29:15+00");
        // +05:30 (colon path) → 06:30 UTC.
        assert_eq!(inst("+05:30"), "2024-01-15 06:30:00+00");
        // +05:30:45 (HH:MM:SS colon path) → 06:29:15 UTC.
        assert_eq!(inst("+05:30:45"), "2024-01-15 06:29:15+00");
        // -08 → 20:00 UTC (the `b'-'` arm + the negative sign, lines 585/609).
        assert_eq!(inst("-08"), "2024-01-15 20:00:00+00");
        // Z → UTC, unchanged.
        assert_eq!(inst("Z"), "2024-01-15 12:00:00+00");
    }

    // -- push_offset ----------------------------------------------------

    #[test]
    fn push_offset_renders_hh_mm_ss_only_when_nonzero() {
        let mut s = String::new();
        push_offset(
            &mut s,
            Offset::from_seconds(5 * 3600 + 30 * 60).expect("off"),
        );
        assert_eq!(s, "+05:30");
        let mut s = String::new();
        push_offset(
            &mut s,
            Offset::from_seconds(5 * 3600 + 30 * 60 + 45).expect("off"),
        );
        assert_eq!(s, "+05:30:45");
        // Whole-hour offset prints only `±HH` (mins == 0 && secs == 0 → the `||`
        // is false, line 635).
        let mut s = String::new();
        push_offset(&mut s, Offset::from_seconds(5 * 3600).expect("off"));
        assert_eq!(s, "+05");
        // Negative offset.
        let mut s = String::new();
        push_offset(&mut s, Offset::from_seconds(-8 * 3600).expect("off"));
        assert_eq!(s, "-08");
        // Seconds-only-nonzero exercises the inner `secs != 0` branch (line 637)
        // and the `mins` div/rem (line 631).
        let mut s = String::new();
        push_offset(&mut s, Offset::from_seconds(45).expect("off"));
        assert_eq!(s, "+00:00:45");
    }

    // -- parse_clock_term -----------------------------------------------

    #[test]
    fn parse_clock_term_negative_and_fraction() {
        // A clock-only interval observed via parse_interval / interval_to_text.
        // Negative clock term (line 727 `-` prefix): -1:02:03 → -3_723_000_000 µs.
        assert_eq!(
            parse_interval("-1:02:03").expect("iv"),
            iv(0, 0, -3_723_000_000)
        );
        // Positive with fraction (the frac-pad loop, line 739 `< 6`): 1:02:03.5 →
        // 3_723_500_000 µs (the `.5` pads to 500000 µs).
        assert_eq!(
            parse_interval("1:02:03.5").expect("iv"),
            iv(0, 0, 3_723_500_000)
        );
        // The total combines h/m/s/frac additively (line 753 `+`).
        assert_eq!(
            parse_interval("00:00:01.000001").expect("iv"),
            iv(0, 0, 1_000_001)
        );
    }

    // -- accumulate_unit (every unit + fraction spill) -------------------

    #[test]
    fn accumulate_unit_every_term() {
        // year / yr (lines 771-775): whole×12 + round(frac×12).
        assert_eq!(parse_interval("2 years").expect("iv"), iv(24, 0, 0));
        assert_eq!(parse_interval("0.5 year").expect("iv"), iv(6, 0, 0)); // frac→months
        // month / mon (lines 777-782): whole + frac×30 days + sub-day µs.
        assert_eq!(parse_interval("3 months").expect("iv"), iv(3, 0, 0));
        assert_eq!(parse_interval("1.5 months").expect("iv"), iv(1, 15, 0)); // .5*30=15 days
        // week / wk (line 784-786): whole×7 days; fractional → µs.
        assert_eq!(parse_interval("2 weeks").expect("iv"), iv(0, 14, 0));
        assert_eq!(
            parse_interval("1.5 wk").expect("iv"),
            iv(0, 7, 302_400_000_000) // .5 wk = 3.5 days = 302_400_000_000 µs
        );
        // day (line 788-790).
        assert_eq!(parse_interval("4 days").expect("iv"), iv(0, 4, 0));
        assert_eq!(
            parse_interval("0.5 day").expect("iv"),
            iv(0, 0, 43_200_000_000)
        );
        // hour / hr / h (line 792-794).
        assert_eq!(
            parse_interval("2 hours").expect("iv"),
            iv(0, 0, 7_200_000_000)
        );
        assert_eq!(
            parse_interval("1.5 hr").expect("iv"),
            iv(0, 0, 5_400_000_000)
        );
        // minute / min / m (line 796-798).
        assert_eq!(
            parse_interval("90 minutes").expect("iv"),
            iv(0, 0, 5_400_000_000)
        );
        assert_eq!(
            parse_interval("2.5 min").expect("iv"),
            iv(0, 0, 150_000_000)
        );
        // second / sec / s (line 800-802).
        assert_eq!(
            parse_interval("3 seconds").expect("iv"),
            iv(0, 0, 3_000_000)
        );
        assert_eq!(parse_interval("2.5 sec").expect("iv"), iv(0, 0, 2_500_000));
        // millisecond / msec (line 804-805). NOTE: the literal "ms" trims to "m"
        // (minute) under trim_end_matches('s'), so the millisecond arm is reached
        // via "millisecond"/"msec" — those spellings pin the arm.
        assert_eq!(
            parse_interval("500 milliseconds").expect("iv"),
            iv(0, 0, 500_000)
        );
        assert_eq!(parse_interval("2.5 msec").expect("iv"), iv(0, 0, 2_500));
        // microsecond / usec (line 807-808). Likewise "us" trims to "u" (unknown);
        // the arm is reached via "microsecond"/"usec".
        assert_eq!(
            parse_interval("123 microseconds").expect("iv"),
            iv(0, 0, 123)
        );
        assert_eq!(parse_interval("7 usec").expect("iv"), iv(0, 0, 7));
        // An unknown unit is rejected (the `_ => None` arm).
        assert!(parse_interval("3 fortnights").is_err());
    }

    #[test]
    fn accumulate_unit_year_term_does_not_clobber_prior_micros() {
        // Line 774 is `*micros += 0` in the year arm. A prior term sets micros
        // non-zero; the `year` term must LEAVE it (`+= 0`), not `*= 0` (wipe it).
        // "2 hours 1 year": hours set 7_200_000_000 µs, the year adds 12 months
        // and must NOT zero the µs.
        assert_eq!(
            parse_interval("2 hours 1 year").expect("iv"),
            iv(12, 0, 7_200_000_000),
            "the year arm's `*micros += 0` must not wipe accumulated micros"
        );
    }

    #[test]
    fn accumulate_unit_fractional_month_spills_subday_into_micros() {
        // Line 780-782: a fractional month whose 30-day spill has a SUB-DAY
        // remainder exercises the micros line (line 782). 1.05 months → frac 0.05
        // × 30 = 1.5 days → 1 day + 0.5 day = 43_200_000_000 µs. A zero-remainder
        // fraction (like 1.5 months → exactly 15 days) leaves line 782 adding 0,
        // so use a remainder-bearing fraction to give that line teeth.
        assert_eq!(
            parse_interval("1.05 months").expect("iv"),
            iv(1, 1, 43_200_000_000)
        );
    }

    #[test]
    fn parse_interval_clock_then_pair_advances_index() {
        // A clock term FOLLOWED by a `<qty> <unit>` pair: the clock advances `i`
        // (line 698) to 1, then the pair's `i += 2` (line 705) lands on len. Under
        // `i *= 2` the pair would jump to `1*2 = 2`, leaving "day" to be parsed as a
        // quantity → an error; the real `i += 2` reaches len cleanly.
        assert_eq!(
            parse_interval("01:00:00 1 day").expect("iv"),
            iv(0, 1, 3_600_000_000)
        );
    }

    // -- parse_interval multi-term (also converts the two += timeouts) ---

    #[test]
    fn parse_interval_multi_term_accumulates_each_field() {
        // Each term ADDS to its field (lines 698/705 `+=`, NOT `*=`): a multi-term
        // interval sums months (1y2mo = 14), days (3), micros (4h5m6s).
        let got = parse_interval("1 year 2 months 3 days 4 hours 5 minutes 6 seconds").expect("iv");
        assert_eq!(got, iv(14, 3, 14_706_000_000));
        // Two clock terms accumulate via the `micros += parse_clock_term` path
        // (line 698): 01:00:00 + 00:30:00.
        assert_eq!(
            parse_interval("01:00:00 00:30:00").expect("iv"),
            iv(0, 0, 5_400_000_000)
        );
    }

    // -- format_clock ---------------------------------------------------

    #[test]
    fn format_clock_signs_and_subsecond() {
        // hours ≥ 10 (kills the `* / 1000` subsec mutant at line 869, and the
        // `< 0` sign test at line 856): 10:00:00.5.
        assert_eq!(
            interval_to_text(iv(0, 0, 10 * 3_600_000_000 + 500_000)),
            "10:00:00.5"
        );
        // Negative whole-hour clock prints `-01:00:00` (line 856 `< 0`).
        assert_eq!(interval_to_text(iv(0, 0, -3_600_000_000)), "-01:00:00");
        // Negative mixed clock.
        assert_eq!(
            interval_to_text(iv(0, 0, -(3_600_000_000 + 2 * 60_000_000 + 3 * 1_000_000))),
            "-01:02:03"
        );
        // A positive sub-second-only clock (the subsec multiply, line 869).
        assert_eq!(interval_to_text(iv(0, 0, 250_000)), "00:00:00.25");
    }
}
