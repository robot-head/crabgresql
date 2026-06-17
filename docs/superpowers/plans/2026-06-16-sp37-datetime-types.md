# SP37 — Date/time types + core semantics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PostgreSQL's `date`, `time`, `timestamp`, `timestamptz`, and `interval` types with their literals, I/O encodings, comparison/ordering, arithmetic, the full cast matrix, transactional `SET`/`SHOW`/`RESET timezone`, `AT TIME ZONE`, injectable-clock functions, and `extract`/`date_part`/`date_trunc`/`age`.

**Architecture:** Five new `ColumnType`/`Datum` variants backed by `jiff` (pure Rust, bundled tzdb). Values + their parse/format/arithmetic live in a new `pgtypes::datetime` module (the SP32 `numeric` pattern). A new executor `EvalCtx { now, stmt_now, time_zone, clock }` is threaded through `eval` so clock functions and session-timezone-dependent `timestamptz` parsing/rendering work; the session timezone is a faithful transactional GUC. No Stateright model (pure-data / single-session carve-out — see the spec §11.3).

**Tech Stack:** Rust 2024, `jiff = "0.2"` (feature `tzdb-bundle-always`), `cargo nextest`, the PostgreSQL conformance oracle.

**Spec:** `docs/superpowers/specs/2026-06-16-SP37-datetime-types-design.md` — read it first.

---

## Conventions for every task

- **Test runner:** `cargo nextest run -p <crate> <filter>` for crate tests; `cargo test -p <crate> --doc` for doctests. Run `cargo fmt` before every commit (implementers run clippy+test but forget fmt — bake it in).
- **Per-task gate (before each commit):** `cargo fmt --all` → `cargo clippy -p <crate> --all-targets -- -D warnings` → the task's `cargo nextest` filter green.
- **jiff API note:** code below uses jiff 0.2 method names to the best of current knowledge. If a method name differs in the resolved jiff version, adjust the call — **the tests pin the behavior**, so make the test pass with the real API. Do not change a test's expected value to match a wrong implementation.
- **PG fidelity (greenfield principle, CLAUDE.md):** where a comment says "byte-exact vs PG", the conformance corpus (Task 15) is the arbiter; match PostgreSQL 18's output exactly.

## File structure (created / modified)

**Created:**
- `crates/pgtypes/src/datetime.rs` — `Interval`, parse/format/binary, value ops, the tz-aware helpers. The single source of truth for date/time *values*.
- `crates/executor/src/datetime_fn.rs` — `extract`/`date_part`/`date_trunc`/`age` + the clock functions registry.
- `crates/executor/src/clock.rs` — the `Clock` trait, `SystemClock`, `FixedClock`, and `EvalCtx`.
- `crates/executor/tests/datetime.rs` — the end-to-end wire test.
- `crates/conformance/corpus/datetime.sql`, `crates/conformance/corpus/interval.sql`.

**Modified (with the new variants / arms):**
- `crates/pgtypes/src/{lib.rs,datum.rs,encoding.rs,ops.rs,cast.rs}`
- `crates/pgparser/src/{token.rs,ast.rs,parser.rs}`
- `crates/executor/src/{lib.rs,eval.rs,exec.rs,session.rs,func.rs,agg.rs,error.rs}`
- `crates/kv/src/rowenc.rs`, `crates/catalog/src/serde.rs`
- `crates/pgwire/src/{session.rs,messages/backend.rs}` (mid-session `ParameterStatus`)
- `crates/executor/Cargo.toml`, `crates/pgtypes/Cargo.toml` (jiff dep)
- `CLAUDE.md` (slice summary + `executor` test list)

---

## Task 1: Add `jiff` + the five `ColumnType`/`Datum` variants (skeleton)

Get the type vocabulary into `pgtypes` so everything else can reference it. Encoders/ops/casts get **temporary `todo!()`-free** stub arms only where the compiler forces them; we drive real behavior in later tasks via tests. To keep the tree compiling without dead stubs, this task adds the variants and the *infallible, non-rendering* methods (`oid`/`name`/`type_size`/`from_sql_name`/`column_type`) with real values, and leaves encode/ops/cast to later tasks (their `match` is non-exhaustive → compile error → we add the arms in their own tasks).

**Files:**
- Modify: `crates/pgtypes/Cargo.toml`
- Modify: `crates/pgtypes/src/datum.rs`
- Modify: `crates/pgtypes/src/lib.rs` (add `pub mod datetime;`)
- Create: `crates/pgtypes/src/datetime.rs` (just the `Interval` struct + `oids` for now)

- [ ] **Step 1: Add the dependency.** In `crates/pgtypes/Cargo.toml` under `[dependencies]`:

```toml
jiff = { version = "0.2", features = ["tzdb-bundle-always"] }
```

- [ ] **Step 2: Write the failing test** in `crates/pgtypes/src/datum.rs` (in the existing `mod tests`):

```rust
#[test]
fn datetime_oids_names_sizes_match_postgres() {
    assert_eq!(ColumnType::Date.oid(), 1082);
    assert_eq!(ColumnType::Time.oid(), 1083);
    assert_eq!(ColumnType::Timestamp.oid(), 1114);
    assert_eq!(ColumnType::Timestamptz.oid(), 1184);
    assert_eq!(ColumnType::Interval.oid(), 1186);
    assert_eq!(ColumnType::Date.name(), "date");
    assert_eq!(ColumnType::Time.name(), "time without time zone");
    assert_eq!(ColumnType::Timestamp.name(), "timestamp without time zone");
    assert_eq!(ColumnType::Timestamptz.name(), "timestamp with time zone");
    assert_eq!(ColumnType::Interval.name(), "interval");
    assert_eq!(ColumnType::Date.type_size(), 4);
    assert_eq!(ColumnType::Time.type_size(), 8);
    assert_eq!(ColumnType::Timestamp.type_size(), 8);
    assert_eq!(ColumnType::Timestamptz.type_size(), 8);
    assert_eq!(ColumnType::Interval.type_size(), 16);
}

#[test]
fn datetime_type_names_resolve_and_timetz_is_unsupported() {
    assert_eq!(ColumnType::from_sql_name("date"), Some(ColumnType::Date));
    assert_eq!(ColumnType::from_sql_name("time"), Some(ColumnType::Time));
    assert_eq!(
        ColumnType::from_sql_name("time without time zone"),
        Some(ColumnType::Time)
    );
    assert_eq!(
        ColumnType::from_sql_name("timestamp"),
        Some(ColumnType::Timestamp)
    );
    assert_eq!(
        ColumnType::from_sql_name("timestamp without time zone"),
        Some(ColumnType::Timestamp)
    );
    assert_eq!(
        ColumnType::from_sql_name("timestamptz"),
        Some(ColumnType::Timestamptz)
    );
    assert_eq!(
        ColumnType::from_sql_name("timestamp with time zone"),
        Some(ColumnType::Timestamptz)
    );
    assert_eq!(
        ColumnType::from_sql_name("interval"),
        Some(ColumnType::Interval)
    );
    // timetz / time with time zone is an explicit non-goal → unsupported.
    assert_eq!(ColumnType::from_sql_name("timetz"), None);
    assert_eq!(ColumnType::from_sql_name("time with time zone"), None);
}
```

- [ ] **Step 3: Run to verify it fails to compile** (variants don't exist):

Run: `cargo nextest run -p pgtypes datetime_oids`
Expected: compile error `no variant ... Date`.

- [ ] **Step 4: Implement.** In `crates/pgtypes/src/datum.rs`:

In `pub mod oids` add:
```rust
    /// SP37: date/time family.
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
```

Add variants to `ColumnType` (after `Numeric`):
```rust
    /// SP37: `date` (no time, no zone).
    Date,
    /// SP37: `time` without time zone.
    Time,
    /// SP37: `timestamp` without time zone.
    Timestamp,
    /// SP37: `timestamp with time zone` (stored as a UTC instant).
    Timestamptz,
    /// SP37: `interval` (months/days/microseconds, kept separate as PG does).
    Interval,
```

Add to `Datum` (after `Numeric`):
```rust
    /// SP37: `date`.
    Date(jiff::civil::Date),
    /// SP37: `time` without time zone.
    Time(jiff::civil::Time),
    /// SP37: `timestamp` without time zone.
    Timestamp(jiff::civil::DateTime),
    /// SP37: `timestamp with time zone` — an absolute instant (UTC).
    Timestamptz(jiff::Timestamp),
    /// SP37: `interval`.
    Interval(crate::datetime::Interval),
```

Extend `from_sql_name`'s match (the multi-word names are normalized by the parser before reaching here, so match the normalized single strings AND the canonical multi-word ones for direct callers/tests):
```rust
            "date" => Some(ColumnType::Date),
            "time" | "time without time zone" => Some(ColumnType::Time),
            "timestamp" | "timestamp without time zone" => Some(ColumnType::Timestamp),
            "timestamptz" | "timestamp with time zone" => Some(ColumnType::Timestamptz),
            "interval" => Some(ColumnType::Interval),
            // `timetz` / `time with time zone` is a documented non-goal.
```

Extend `oid`, `name`, `type_size`, and `Datum::column_type` with the obvious arms (`name`s exactly as in the test; `type_size`: Date=4, Time/Timestamp/Timestamptz=8, Interval=16; `column_type`: each `Datum::X(_) => Some(ColumnType::X)`).

In `crates/pgtypes/src/lib.rs` add `pub mod datetime;`.

In `crates/pgtypes/src/datetime.rs` (new), the minimal `Interval` so `Datum` compiles:
```rust
//! SP37: date/time *values* — the `Interval` type plus parsing, formatting,
//! binary encodings, and value arithmetic. PostgreSQL semantics; `jiff` does the
//! calendar/timezone math. This is the single source of truth for date/time
//! values (the SP32 `numeric` module pattern).

/// A PostgreSQL `interval`: months, days, and microseconds kept SEPARATE (PG does
/// not fold `1 month` into `30 days` for storage/arithmetic — only for ordering).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}
```

- [ ] **Step 5: Keep the lib compiling.** Adding `Datum` variants breaks only the **exhaustive** matches (most `ops`/`cast` matches have a `_` catch-all and are fine). The matches with NO catch-all that must get arms now:
  - `Datum::column_type()` — add the real arms (done in Step 4).
  - `Datum`'s `impl Hash` (datum.rs) — add **real** arms now: `Datum::Date(d) => d.hash(state)`, `Time`/`Timestamp`/`Timestamptz` likewise, `Datum::Interval(i) => i.hash(state)` (Interval derives `Hash` here in Task 1; Task 2 replaces that derive — the `i.hash(state)` call is unaffected).
  - `Datum`'s `impl PartialEq` already has a `_ => false` arm, so it compiles; the real arms are added in Task 3 (until then, two equal new-variant values compare `false`, which no Task-1/Task-2 test relies on).
  - `encoding::encode_text`/`encode_binary` (exhaustive, no catch-all) — add the five new arms as `unimplemented!("SP37 Task 4/5")` stubs now; Tasks 4 and 5 replace them. (No test before Task 4 encodes a new-variant Datum, so the stubs are never hit.)

  Then `cargo build -p pgtypes` compiles and `cargo nextest run -p pgtypes datetime_` → PASS.

- [ ] **Step 6: Commit.**
```bash
git add crates/pgtypes
git commit -m "SP37: add jiff dep + date/time ColumnType/Datum variants"
```

---

## Task 2: `Interval` ordering/grouping equality + `canonical_micros`

PG's `interval` btree equality compares the **canonical estimate** `(months·30 + days)·86_400_000_000 + micros` (30-day months, 24-hour days), so `'1 month' = '30 days'` and they must group/hash together. `Interval` derived `Eq`/`Hash` field-wise in Task 1 — **that is wrong for grouping**; replace it.

**Files:**
- Modify: `crates/pgtypes/src/datetime.rs`

- [ ] **Step 1: Write the failing tests** in `crates/pgtypes/src/datetime.rs`:

```rust
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
        let one_month = Interval { months: 1, days: 0, micros: 0 };
        let thirty_days = Interval { months: 0, days: 30, micros: 0 };
        // PG: '1 month' = '30 days' (30-day-month estimate). Equal AND hash-equal.
        assert_eq!(one_month, thirty_days);
        assert_eq!(h(&one_month), h(&thirty_days));
        // '1 day' = '24 hours'.
        let one_day = Interval { months: 0, days: 1, micros: 0 };
        let day_us = 86_400_000_000i64;
        let twentyfour_h = Interval { months: 0, days: 0, micros: day_us };
        assert_eq!(one_day, twentyfour_h);
        assert_eq!(h(&one_day), h(&twentyfour_h));
        // Distinct estimates are unequal.
        assert_ne!(one_month, one_day);
    }

    #[test]
    fn interval_ordering_is_by_canonical_estimate() {
        use std::cmp::Ordering;
        let a = Interval { months: 0, days: 1, micros: 0 };
        let b = Interval { months: 1, days: 0, micros: 0 };
        assert_eq!(a.cmp(&b), Ordering::Less); // 1 day < 1 month(=30 days)
        assert_eq!(a.canonical_micros(), 86_400_000_000i128);
        assert_eq!(b.canonical_micros(), 30 * 86_400_000_000i128);
    }
}
```

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p pgtypes interval_` → FAIL (`canonical_micros` undefined; `cmp` may not exist).

- [ ] **Step 3: Implement.** In `crates/pgtypes/src/datetime.rs`, remove `PartialEq, Eq, Hash` (and any `PartialOrd, Ord`) from the `#[derive]` on `Interval` (keep `Debug, Clone, Copy`), then add:

```rust
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
```

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p pgtypes interval_` → PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/pgtypes/src/datetime.rs
git commit -m "SP37: Interval canonical-estimate equality/ordering (PG '1 month' = '30 days')"
```

---

## Task 3: `Datum` grouping equality/hash for the date/time variants

Task 1 added the `Hash` arms (it had to — `Hash` is exhaustive) and Task 2 fixed `Interval`'s canonical equality, but `Datum`'s `impl PartialEq` still routes the new variants through its `_ => false` arm, so two equal dates never compare equal (breaks `GROUP BY`/`DISTINCT`/`ORDER BY`). Add the real `PartialEq` arms (the `Hash` arms already exist from Task 1; this task completes the Eq/Hash contract for the new variants).

**Files:**
- Modify: `crates/pgtypes/src/datum.rs`

- [ ] **Step 1: Write the failing test** in `mod tests`:

```rust
#[test]
fn datetime_datum_grouping_equality_and_hash() {
    use crate::datetime::Interval;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    fn h(d: &Datum) -> u64 {
        let mut s = DefaultHasher::new();
        d.hash(&mut s);
        s.finish()
    }
    let d1 = Datum::Date("2024-01-15".parse().unwrap());
    let d2 = Datum::Date("2024-01-15".parse().unwrap());
    assert_eq!(d1, d2);
    assert_eq!(h(&d1), h(&d2));
    // interval grouping equality threads through Datum.
    let m = Datum::Interval(Interval { months: 1, days: 0, micros: 0 });
    let dd = Datum::Interval(Interval { months: 0, days: 30, micros: 0 });
    assert_eq!(m, dd);
    assert_eq!(h(&m), h(&dd));
    // cross-variant never equal.
    assert_ne!(d1, Datum::Timestamp("2024-01-15T00:00:00".parse().unwrap()));
}
```

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p pgtypes datetime_datum_grouping` → FAIL.

- [ ] **Step 3: Implement.** In `Datum`'s `impl PartialEq`, add arms before `_ => false`:
```rust
            (Datum::Date(a), Datum::Date(b)) => a == b,
            (Datum::Time(a), Datum::Time(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            // timestamptz equality is by absolute instant (jiff Timestamp).
            (Datum::Timestamptz(a), Datum::Timestamptz(b)) => a == b,
            // interval uses its canonical-estimate Eq (Task 2).
            (Datum::Interval(a), Datum::Interval(b)) => a == b,
```
The `impl Hash` arms were already added in Task 1 (`Datum::Date(d) => d.hash(state)`, …, `Datum::Interval(i) => i.hash(state)`) — leave them as-is. With Task 2's canonical-estimate `Interval` hash, the interval part of the test below now passes.

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p pgtypes datetime_datum_grouping` → PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/pgtypes/src/datum.rs
git commit -m "SP37: Datum grouping equality/hash for date/time variants"
```

---

## Task 4: Parsing + text output + binary encodings (`pgtypes::datetime`)

The value I/O. Parse per the spec §6.1 grammar; render per §6.2 (PG `*_out`, byte-exact). `timestamptz` parse/format take a `&jiff::tz::TimeZone`. Binary uses the PG 2000-01-01 epoch.

**Files:**
- Modify: `crates/pgtypes/src/datetime.rs`
- Modify: `crates/pgtypes/src/error.rs` (two new `TypeError` variants)
- Modify: `crates/pgtypes/src/encoding.rs` (wire the new `Datum` arms — needs tz: see Task 5; here add the *text* arms that don't need tz and a temporary UTC render for timestamptz, replaced in Task 5)

- [ ] **Step 1: Add error variants.** In `crates/pgtypes/src/error.rs`, add to `TypeError`:
```rust
    /// SP37: malformed date/time/interval literal or text (22007).
    #[error("invalid input syntax for type {type_name}: \"{value}\"")]
    InvalidDatetimeFormat {
        type_name: &'static str,
        value: String,
    },
    /// SP37: a date/time field out of range (e.g. month 13) (22008).
    #[error("date/time field value out of range: \"{value}\"")]
    DatetimeFieldOverflow { value: String },
```
and in `sqlstate()`:
```rust
            TypeError::InvalidDatetimeFormat { .. } => "22007",
            TypeError::DatetimeFieldOverflow { .. } => "22008",
```

- [ ] **Step 2: Write the failing tests** in `crates/pgtypes/src/datetime.rs`:

```rust
#[cfg(test)]
mod io_tests {
    use super::*;

    fn utc() -> jiff::tz::TimeZone {
        jiff::tz::TimeZone::UTC
    }

    #[test]
    fn parse_and_format_date() {
        let d = parse_date("2024-02-29").expect("leap day"); // 2024 is a leap year
        assert_eq!(date_to_text(d), "2024-02-29");
        // out-of-range day → 22008.
        assert!(matches!(
            parse_date("2023-02-29"),
            Err(crate::TypeError::DatetimeFieldOverflow { .. })
        ));
        // malformed → 22007.
        assert!(matches!(
            parse_date("not-a-date"),
            Err(crate::TypeError::InvalidDatetimeFormat { .. })
        ));
    }

    #[test]
    fn parse_and_format_time_trims_subseconds() {
        assert_eq!(time_to_text(parse_time("12:34:56").unwrap()), "12:34:56");
        assert_eq!(time_to_text(parse_time("12:34").unwrap()), "12:34:00");
        // sub-second prints only the significant digits.
        assert_eq!(
            time_to_text(parse_time("01:02:03.450000").unwrap()),
            "01:02:03.45"
        );
    }

    #[test]
    fn parse_and_format_timestamp() {
        let ts = parse_timestamp("2024-01-15 13:45:00").unwrap();
        assert_eq!(timestamp_to_text(ts), "2024-01-15 13:45:00");
        // ISO 'T' separator accepted on input, space on output.
        let ts2 = parse_timestamp("2024-01-15T13:45:00.5").unwrap();
        assert_eq!(timestamp_to_text(ts2), "2024-01-15 13:45:00.5");
    }

    #[test]
    fn parse_and_format_timestamptz_uses_session_zone() {
        let tz = jiff::tz::TimeZone::get("America/New_York").unwrap();
        // offset-less input is interpreted in the session zone (EST = -05:00 in Jan).
        let ts = parse_timestamptz("2024-01-15 12:00:00", &tz).unwrap();
        // rendered back in the same zone, PG prints the offset.
        assert_eq!(timestamptz_to_text(ts, &tz), "2024-01-15 12:00:00-05");
        // the SAME instant rendered in UTC shifts +5h and prints +00.
        assert_eq!(
            timestamptz_to_text(ts, &jiff::tz::TimeZone::UTC),
            "2024-01-15 17:00:00+00"
        );
        // explicit offset in the input is honored regardless of session zone.
        let ts3 = parse_timestamptz("2024-01-15 12:00:00+02", &tz).unwrap();
        assert_eq!(
            timestamptz_to_text(ts3, &jiff::tz::TimeZone::UTC),
            "2024-01-15 10:00:00+00"
        );
    }

    #[test]
    fn parse_and_format_interval_postgres_style() {
        assert_eq!(interval_to_text(parse_interval("1 day").unwrap()), "1 day");
        assert_eq!(
            interval_to_text(parse_interval("1 year 2 months").unwrap()),
            "1 year 2 mons"
        );
        assert_eq!(
            interval_to_text(parse_interval("3 days 04:05:06").unwrap()),
            "3 days 04:05:06"
        );
        assert_eq!(
            interval_to_text(parse_interval("2 hours 30 minutes").unwrap()),
            "02:30:00"
        );
        // zero interval prints 00:00:00.
        assert_eq!(interval_to_text(parse_interval("0 days").unwrap()), "00:00:00");
        // negative.
        assert_eq!(
            interval_to_text(parse_interval("-1 day").unwrap()),
            "-1 days"
        );
    }

    #[test]
    fn binary_round_trips_through_pg_epoch() {
        let d = parse_date("2000-01-02").unwrap();
        assert_eq!(date_to_binary(d), 1i32.to_be_bytes()); // 1 day after epoch
        assert_eq!(date_from_binary(&date_to_binary(d)).unwrap(), d);
        let i = Interval { months: 14, days: 3, micros: 4_000_000 };
        assert_eq!(interval_from_binary(&interval_to_binary(i)).unwrap(), i);
        let _ = utc();
    }
}
```

- [ ] **Step 3: Run to verify fail.** `cargo nextest run -p pgtypes io_tests` → FAIL (functions undefined).

- [ ] **Step 4: Implement** in `crates/pgtypes/src/datetime.rs`. Use jiff for calendar/zone math. Skeleton (fill the bodies with the real jiff API; the semantics and PG-output rules are the contract):

```rust
use jiff::civil::{Date, DateTime, Time};
use jiff::tz::TimeZone;
use jiff::Timestamp;

use crate::TypeError;

fn bad_format(type_name: &'static str, value: &str) -> TypeError {
    TypeError::InvalidDatetimeFormat { type_name, value: value.to_string() }
}

// ---- DATE ----

/// Parse `YYYY-MM-DD`. A well-formed-but-out-of-range field (e.g. 2023-02-29)
/// is 22008; other malformed input is 22007. jiff's parse already range-checks,
/// so distinguish "looks like a date but field invalid" from "garbage" by a
/// shape check: split on '-' into 3 integer fields first.
pub fn parse_date(s: &str) -> Result<Date, TypeError> {
    let t = s.trim();
    // shape: y-m-d all integers → field-overflow on construction failure; else 22007.
    let parts: Vec<&str> = t.split('-').collect();
    let looks_like_date = (parts.len() == 3 || (parts.len() == 4 && t.starts_with('-')))
        && parts.iter().all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    match t.parse::<Date>() {
        Ok(d) => Ok(d),
        Err(_) if looks_like_date => Err(TypeError::DatetimeFieldOverflow { value: s.to_string() }),
        Err(_) => Err(bad_format("date", s)),
    }
}

pub fn date_to_text(d: Date) -> String {
    // jiff Date Display is ISO `YYYY-MM-DD`; confirm against PG (matches for AD).
    format!("{d}")
}

/// PG date binary: i32 days since 2000-01-01.
pub fn date_to_binary(d: Date) -> [u8; 4] {
    let epoch = Date::constant(2000, 1, 1);
    let days = (d - epoch).get_days(); // jiff Span/diff → days; adjust to real API
    (days as i32).to_be_bytes()
}
pub fn date_from_binary(b: &[u8]) -> Result<Date, TypeError> {
    let days = i32::from_be_bytes(b.try_into().map_err(|_| bad_format("date", "<binary>"))?);
    let epoch = Date::constant(2000, 1, 1);
    epoch.checked_add(jiff::Span::new().days(days))
        .map_err(|_| TypeError::DatetimeFieldOverflow { value: days.to_string() })
}
```

Implement the remaining functions analogously, all `pub`, matching the tests:
- `parse_time(&str) -> Result<Time, TypeError>` / `time_to_text(Time) -> String` (HH:MM[:SS[.ffffff]]; output trims trailing sub-second zeros, always prints HH:MM:SS). `time_to_binary`/`time_from_binary` = i64 µs since midnight.
- `parse_timestamp(&str) -> Result<DateTime, TypeError>` (accept space or `T`) / `timestamp_to_text(DateTime) -> String` (space separator). `timestamp_to_binary`/`_from_binary` = i64 µs since 2000-01-01T00:00:00.
- `parse_timestamptz(&str, &TimeZone) -> Result<Timestamp, TypeError>` (offset-less → interpret in tz via `DateTime.to_zoned(tz)`; explicit offset → honored). `timestamptz_to_text(Timestamp, &TimeZone) -> String` (render in tz; offset suffix `±HH[:MM[:SS]]`, PG style). `timestamptz_to_binary` = i64 µs since 2000-01-01 UTC (tz-independent); `_from_binary`.
- `parse_interval(&str) -> Result<Interval, TypeError>` per §6.1 (unit terms + `[-]HH:MM:SS[.ffffff]` clock term; fractional quantities spill down). `interval_to_text(Interval) -> String` — PG `postgres` IntervalStyle (years/mons from months; days; signed `HH:MM:SS[.ffffff]` clock; zero → `00:00:00`). `interval_to_binary` = i64 µs ++ i32 days ++ i32 months (BE, 16 bytes); `interval_from_binary`.

> Implementer: the interval text/parse and timestamptz offset formatting are the byte-exact-vs-PG hotspots. Pin every shape with a unit test here, then confirm against the oracle in Task 15. If a PG output detail surprises you (e.g. `-1 days +00:00:01` mixed-sign form), match PG — do not "tidy" it.

- [ ] **Step 5: Wire the text encoder (UTC placeholder for tz).** In `crates/pgtypes/src/encoding.rs`, add arms to `encode_text` and `encode_binary`. For now render `Timestamptz` in UTC (Task 5 threads the real session tz):
```rust
        Datum::Date(d) => crate::datetime::date_to_text(*d).into_bytes(),
        Datum::Time(t) => crate::datetime::time_to_text(*t).into_bytes(),
        Datum::Timestamp(ts) => crate::datetime::timestamp_to_text(*ts).into_bytes(),
        Datum::Timestamptz(ts) => {
            crate::datetime::timestamptz_to_text(*ts, &jiff::tz::TimeZone::UTC).into_bytes()
        }
        Datum::Interval(i) => crate::datetime::interval_to_text(*i).into_bytes(),
```
and `encode_binary`:
```rust
        Datum::Date(d) => crate::datetime::date_to_binary(*d).to_vec(),
        Datum::Time(t) => crate::datetime::time_to_binary(*t).to_vec(),
        Datum::Timestamp(ts) => crate::datetime::timestamp_to_binary(*ts).to_vec(),
        Datum::Timestamptz(ts) => crate::datetime::timestamptz_to_binary(*ts).to_vec(),
        Datum::Interval(i) => crate::datetime::interval_to_binary(*i).to_vec(),
```

- [ ] **Step 6: Run to verify pass.** `cargo nextest run -p pgtypes` → all green (io_tests + existing).

- [ ] **Step 7: Commit.**
```bash
git add crates/pgtypes
git commit -m "SP37: date/time parse, text output, and binary encodings"
```

---

## Task 5: Thread session `TimeZone` into text rendering (`encode_text`)

`timestamptz` text depends on the session zone everywhere (`||`, `concat`, `*::text`, DataRow). Add a `tz: &TimeZone` parameter to `encode_text` and the renderers that call it, so the real session zone reaches every text rendering. `encode_binary` stays pure (binary timestamptz is UTC-independent).

**Files:**
- Modify: `crates/pgtypes/src/encoding.rs`, `crates/pgtypes/src/ops.rs`, `crates/pgtypes/src/cast.rs`
- Modify: `crates/executor/src/exec.rs`, `crates/executor/src/func.rs` (callers)

- [ ] **Step 1: Write the failing test** in `crates/pgtypes/src/encoding.rs` `mod tests`:
```rust
#[test]
fn timestamptz_text_uses_supplied_zone() {
    let ny = jiff::tz::TimeZone::get("America/New_York").unwrap();
    let ts = crate::datetime::parse_timestamptz("2024-01-15 12:00:00", &jiff::tz::TimeZone::UTC).unwrap();
    assert_eq!(encode_text(&Datum::Timestamptz(ts), &ny), b"2024-01-15 07:00:00-05");
    assert_eq!(encode_text(&Datum::Timestamptz(ts), &jiff::tz::TimeZone::UTC), b"2024-01-15 12:00:00+00");
    // a non-timestamptz value ignores the zone.
    assert_eq!(encode_text(&Datum::Int4(5), &ny), b"5");
}
```

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p pgtypes timestamptz_text_uses_supplied_zone` → FAIL (arity).

- [ ] **Step 3: Implement the signature change.**
  - `pub fn encode_text(d: &Datum, tz: &jiff::tz::TimeZone) -> Vec<u8>` — the `Timestamptz` arm uses `tz`; every other arm ignores it.
  - `pgtypes::ops`: `text_of(d, tz)` and `pub fn concat(a, b, tz)` (concat renders operands as text — needs tz).
  - `pgtypes::cast`: `text_of(d, tz)` and `pub fn cast(value, to, tz)` — the `*→text` arms use `tz`; `text→timestamptz` (added Task 7) uses `tz` for offset-less input.
  - Update existing `pgtypes` unit tests that call `encode_text`/`concat`/`cast` to pass `&jiff::tz::TimeZone::UTC` (mechanical; UTC reproduces the prior behavior for non-temporal types byte-for-byte).
  - Executor callers: `func::text_render(d, tz)`, `func` concat path, and `exec::datum_to_cell(d, tz)` (Task 9 threads `tz` from `EvalCtx`; for now thread a `tz: &TimeZone` param up through `rows_result`/`project_order_limit` and pass `&jiff::tz::TimeZone::UTC` at the top call site as a placeholder, replaced in Task 9).

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p pgtypes` and `cargo build -p executor` → green.

- [ ] **Step 5: Commit.**
```bash
git add crates/pgtypes crates/executor
git commit -m "SP37: thread session TimeZone into text rendering (encode_text/concat/cast)"
```

---

## Task 6: Arithmetic + comparison (`pgtypes::ops`)

Add the §8 temporal matrix to `add`/`sub`/`mul`/`div`/`compare`. These operate on already-evaluated Datums and need **no** tz (instant arithmetic).

**Files:**
- Modify: `crates/pgtypes/src/ops.rs`
- Modify: `crates/pgtypes/src/datetime.rs` (the value-level add/sub helpers)

- [ ] **Step 1: Write the failing tests** in `crates/pgtypes/src/ops.rs` `mod tests`:
```rust
#[test]
fn datetime_arithmetic_matrix() {
    use crate::datetime::Interval;
    let d = |s: &str| Datum::Date(crate::datetime::parse_date(s).unwrap());
    let iv = |m, days, us| Datum::Interval(Interval { months: m, days, micros: us });
    // date + int = date (days).
    assert_eq!(add(&d("2024-01-01"), &Datum::Int4(31)).unwrap(), d("2024-02-01"));
    // date - date = int4 (days).
    assert_eq!(sub(&d("2024-02-01"), &d("2024-01-01")).unwrap(), Datum::Int4(31));
    // date + interval = timestamp.
    assert_eq!(
        add(&d("2024-01-01"), &iv(0, 1, 0)).unwrap(),
        Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-02 00:00:00").unwrap())
    );
    // interval + interval.
    assert_eq!(add(&iv(1, 0, 0), &iv(0, 5, 0)).unwrap(), iv(1, 5, 0));
    // interval * int.
    assert_eq!(mul(&iv(1, 2, 0), &Datum::Int4(3)).unwrap(), iv(3, 6, 0));
    // unary minus interval (via 0 - interval is NOT how PG does it; test ops::neg path
    // through Task 8's apply_unary instead — here test interval negation helper).
    // timestamp - timestamp = interval (days+micros, months stay 0).
    let ts = |s: &str| Datum::Timestamp(crate::datetime::parse_timestamp(s).unwrap());
    assert_eq!(
        sub(&ts("2024-01-02 00:00:00"), &ts("2024-01-01 00:00:00")).unwrap(),
        iv(0, 0, 86_400_000_000)
    );
}

#[test]
fn datetime_comparison_orders_and_promotes() {
    use std::cmp::Ordering;
    let d = |s: &str| Datum::Date(crate::datetime::parse_date(s).unwrap());
    assert_eq!(compare(&d("2024-01-01"), &d("2024-02-01")).unwrap(), Some(Ordering::Less));
    // date vs timestamp promotes the date to midnight timestamp.
    let ts = Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-01 00:00:01").unwrap());
    assert_eq!(compare(&d("2024-01-01"), &ts).unwrap(), Some(Ordering::Less));
}
```

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p pgtypes datetime_arithmetic` → FAIL.

- [ ] **Step 3: Implement.** Add value helpers in `datetime.rs`:
```rust
pub fn add_interval(a: Interval, b: Interval) -> Result<Interval, TypeError> {
    Ok(Interval {
        months: a.months.checked_add(b.months).ok_or(TypeError::Overflow)?,
        days: a.days.checked_add(b.days).ok_or(TypeError::Overflow)?,
        micros: a.micros.checked_add(b.micros).ok_or(TypeError::Overflow)?,
    })
}
pub fn neg_interval(a: Interval) -> Result<Interval, TypeError> { /* checked_neg each field */ }
pub fn mul_interval(a: Interval, factor: f64) -> Result<Interval, TypeError> { /* distribute + spill fraction down, PG rule; overflow → 22008 */ }
/// date + interval → DateTime (PG: add months, then days, then micros, calendar-aware).
pub fn date_plus_interval(d: Date, i: Interval) -> Result<DateTime, TypeError> { /* jiff Span add */ }
pub fn timestamp_plus_interval(ts: DateTime, i: Interval) -> Result<DateTime, TypeError> { /* jiff */ }
pub fn timestamptz_plus_interval(ts: Timestamp, i: Interval, tz: &TimeZone) -> Result<Timestamp, TypeError> { /* zoned add: months/days in tz, micros absolute */ }
/// timestamp - timestamp → interval (days + micros; PG keeps months 0 here).
pub fn timestamp_diff(a: DateTime, b: DateTime) -> Interval { /* total micros → days + micros */ }
pub fn timestamptz_diff(a: Timestamp, b: Timestamp) -> Interval { /* absolute micros → micros only */ }
```

In `ops::add`/`sub`/`mul`/`div`, add temporal arms BEFORE the numeric fast-paths (so a temporal operand isn't mis-promoted via `as_f64`). Mirror PG's matrix (spec §8). Note `timestamptz ± interval` needs tz — but `ops` has no tz; resolve by handling `timestamptz ± interval` in the executor `apply_binary` (Task 8) where `EvalCtx.time_zone` is available, NOT in `ops`. So `ops` handles the tz-free arms (date/timestamp/interval); `apply_binary` handles the timestamptz arms. Document this split in an `ops` comment.

In `ops::compare`, add temporal arms with cross-type promotion (date↔timestamp at midnight; timestamp↔timestamptz compare requires tz → also deferred to executor comparison where ctx exists, OR compare timestamptz↔timestamptz only in ops and reject mixed timestamp/timestamptz in ops with a clear error that the executor pre-promotes). **Decision:** keep `ops::compare` for same-family + date/timestamp; the executor's `apply_binary` pre-converts timestamp↔timestamptz using `ctx.time_zone` before calling `compare`.

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p pgtypes datetime_arithmetic datetime_comparison` → PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/pgtypes
git commit -m "SP37: date/time arithmetic + comparison (tz-free arms in ops)"
```

---

## Task 7: Cast matrix (`pgtypes::cast`)

Add the §9 temporal casts to `cast_allowed` + `cast`. `cast` already takes `tz` (Task 5). Cross-type temporal casts that need a zone (`date→timestamptz`, `timestamp↔timestamptz`, `timestamptz→*`) use `tz`.

**Files:**
- Modify: `crates/pgtypes/src/cast.rs`

- [ ] **Step 1: Write the failing tests** in `cast.rs` `mod tests` (use UTC tz):
```rust
#[test]
fn datetime_cast_matrix() {
    use ColumnType::{Date, Time, Timestamp, Timestamptz, Text};
    let utc = &jiff::tz::TimeZone::UTC;
    let d = Datum::Date(crate::datetime::parse_date("2024-01-15").unwrap());
    // text ↔ each.
    assert_eq!(cast(&Datum::Text("2024-01-15".into()), Date, utc).unwrap(), d);
    assert_eq!(cast(&d, Text, utc).unwrap(), Datum::Text("2024-01-15".into()));
    // date → timestamp (midnight).
    assert_eq!(
        cast(&d, Timestamp, utc).unwrap(),
        Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-15 00:00:00").unwrap())
    );
    // timestamp → date (truncate) and → time.
    let ts = Datum::Timestamp(crate::datetime::parse_timestamp("2024-01-15 13:45:06").unwrap());
    assert_eq!(cast(&ts, Date, utc).unwrap(), d);
    assert_eq!(cast(&ts, Time, utc).unwrap(), Datum::Time(crate::datetime::parse_time("13:45:06").unwrap()));
    // undefined: interval → date is 42846; numeric → date is 42846.
    assert!(matches!(cast(&Datum::Int4(1), Date, utc), Err(crate::TypeError::CannotCast { .. })));
    // allowed-matrix gate.
    assert!(cast_allowed(Date, Timestamptz));
    assert!(!cast_allowed(ColumnType::Interval, Date));
    assert!(!cast_allowed(ColumnType::Int4, Date));
}
```

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p pgtypes datetime_cast_matrix` → FAIL.

- [ ] **Step 3: Implement.** In `cast_allowed`, add a temporal sub-matrix: identity; `text↔{date,time,timestamp,timestamptz,interval}`; `date→{timestamp,timestamptz}`; `timestamp→{date,time,timestamptz}`; `timestamptz→{date,time,timestamp}`; and nothing else among temporals (no `interval↔` date/time/timestamp, no numeric/bool↔temporal). In `cast`, add the runtime arms (text parse via `datetime::parse_*` with `tz` for timestamptz; cross-type via jiff conversions with `tz`). `*→text` already routed through `text_of(d, tz)` (Task 5).

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p pgtypes datetime_cast_matrix` → PASS. Also extend the existing `cast_allowed_matches_the_postgres_matrix` test to assert the new pairs and the `42846` non-pairs.

- [ ] **Step 5: Commit.**
```bash
git add crates/pgtypes/src/cast.rs
git commit -m "SP37: explicit cast matrix for date/time types"
```

---

## Task 8: Storage — `rowenc` + `catalog::serde`

Append-only tags. `rowenc` next free = 7; `catalog::serde` next free = 6. Each variable-precision type reserves an optional-precision payload byte (`0` = none) for the deferred typmod (spec §1.3).

**Files:**
- Modify: `crates/kv/src/rowenc.rs`, `crates/catalog/src/serde.rs`

- [ ] **Step 1: Write the failing tests.** In `crates/kv/src/rowenc.rs` `mod tests`:
```rust
#[test]
fn datetime_row_round_trip() {
    use pgtypes::datetime::Interval;
    let row = vec![
        Datum::Date(pgtypes::datetime::parse_date("2024-01-15").unwrap()),
        Datum::Time(pgtypes::datetime::parse_time("13:45:06.5").unwrap()),
        Datum::Timestamp(pgtypes::datetime::parse_timestamp("2024-01-15 13:45:06").unwrap()),
        Datum::Timestamptz(pgtypes::datetime::parse_timestamptz("2024-01-15 13:45:06+00", &jiff::tz::TimeZone::UTC).unwrap()),
        Datum::Interval(Interval { months: 14, days: -3, micros: 4_500_000 }),
    ];
    assert_eq!(decode_row(&encode_row(&row)).unwrap(), row);
}
```
In `crates/catalog/src/serde.rs` `mod tests`, a round-trip of a `Table` whose columns include all five new types (mirror the existing numeric round-trip test).

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p kv datetime_row_round_trip` → FAIL.

- [ ] **Step 3: Implement.** `rowenc::tag`: `DATE=7, TIME=8, TIMESTAMP=9, TIMESTAMPTZ=10, INTERVAL=11`. `encode_row` arms append tag + bytes via `datetime::*_to_binary` (interval = 16 bytes). `decode_row` arms read fixed widths via `datetime::*_from_binary`. `catalog::serde::type_tag`: `DATE=6, TIME=7, TIMESTAMP=8, TIMESTAMPTZ=9, INTERVAL=10`; `write_type` pushes the tag then a `0` precision-reserved byte for Time/Timestamp/Timestamptz/Interval (Date has no sub-second precision, no payload); `read_type` reads + asserts the reserved byte is `0` (else `CorruptRow`).

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p kv -p catalog datetime` → PASS.

- [ ] **Step 5: Commit.**
```bash
git add crates/kv crates/catalog
git commit -m "SP37: storage tags for date/time (rowenc 7-11, catalog 6-10, reserved typmod byte)"
```

---

## Task 9: `EvalCtx` + `Clock`, threaded through `eval`

The invasive change. Introduce `clock.rs` (the `Clock` trait + `EvalCtx`), thread `&EvalCtx` through `eval` and its in-crate callers, and replace the UTC placeholders from Tasks 5/4 with the real `ctx.time_zone`. The clock funcs/literals come in Task 11/10; here we just thread the context with all existing tests still green.

**Files:**
- Create: `crates/executor/src/clock.rs`
- Modify: `crates/executor/src/{lib.rs,eval.rs,exec.rs,agg.rs,func.rs}`
- Modify: `crates/executor/Cargo.toml` (add `jiff = "0.2"`)

- [ ] **Step 1: Create `clock.rs`:**
```rust
//! SP37: the evaluation context (session timezone + the transaction/statement
//! clock) threaded through expression evaluation, and an injectable clock so
//! `now()`/`current_timestamp` are deterministic in tests.

use std::sync::Arc;

use jiff::Timestamp;
use jiff::tz::TimeZone;

/// Source of "current time". `SystemClock` in production; `FixedClock` in tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

#[derive(Debug, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}

/// A fixed clock for deterministic tests.
#[derive(Debug, Clone)]
pub struct FixedClock(pub Timestamp);
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

/// Per-statement evaluation context. `now`/`stmt_now` are the transaction- and
/// statement-start instants (PG transaction-stable semantics); `time_zone` is the
/// effective session zone; `clock` backs `clock_timestamp()`.
#[derive(Clone)]
pub struct EvalCtx {
    pub now: Timestamp,
    pub stmt_now: Timestamp,
    pub time_zone: TimeZone,
    pub clock: Arc<dyn Clock>,
}

impl EvalCtx {
    /// A UTC context anchored at the Unix epoch — for tests / non-temporal eval.
    pub fn test_default() -> Self {
        let epoch = Timestamp::UNIX_EPOCH;
        Self { now: epoch, stmt_now: epoch, time_zone: TimeZone::UTC, clock: Arc::new(SystemClock) }
    }
}
```
Add `pub mod clock;` to `crates/executor/src/lib.rs` and `jiff = "0.2"` to `crates/executor/Cargo.toml`.

- [ ] **Step 2: Write the failing test** in `crates/executor/src/eval.rs` `mod tests` (most eval tests call `eval(&e, None, &[])` — they'll need the new arg; add one new test asserting the signature + that a non-temporal expr ignores ctx):
```rust
#[test]
fn eval_takes_ctx_and_ignores_it_for_non_temporal() {
    let ctx = crate::clock::EvalCtx::test_default();
    let e = pgparser::parser::parse_expr_for_test("1 + 2").unwrap();
    assert_eq!(eval(&e, None, &[], &ctx).unwrap(), Datum::Int4(3));
}
```

- [ ] **Step 3: Run to verify fail.** `cargo build -p executor` → FAIL (arity).

- [ ] **Step 4: Implement the threading.**
  - `eval(expr, table, values, ctx: &crate::clock::EvalCtx)` — thread `ctx` through every recursive `eval(...)` call and into `apply_binary`/`apply_unary` (they gain `ctx` too: needed for the timestamptz arms + nothing else; pass through). The `Expr::Cast` arm calls `pgtypes::cast::cast(&v, *ty, &ctx.time_zone)`.
  - `apply_binary(op, l, r, ctx)`: for `Concat` call `ops::concat(l, r, &ctx.time_zone)`; add a pre-step for `Add`/`Sub` that handles the **timestamptz ± interval** and **timestamp/timestamptz mixed comparison** arms using `ctx.time_zone` (delegating tz-free arms to `ops`). 
  - `agg::eval_grouped` and its `apply_binary`/`eval_scalar`/`eval_*` calls: thread `ctx`.
  - `func::eval_scalar(fc, eval_child)` — the `eval_child` closures already capture the row; they now also capture `ctx` (callers pass `|e| eval(e, table, values, ctx)`).
  - `exec.rs`: every `eval(...)` / `apply_binary(...)` / `execute_read` / `project_*` / `datum_to_cell` call site threads a `ctx`. **Build the ctx** in `session.rs`'s read path (Task 11 captures real now/tz; for THIS task, build `EvalCtx::test_default()` at the execute entry so all existing behavior is unchanged — UTC + epoch). Replace the Task 5 placeholder `&jiff::tz::TimeZone::UTC` in `datum_to_cell`/`rows_result` with `&ctx.time_zone`.
  - Update every existing `eval(...)`/`apply_*` call in `executor` unit tests to pass `&EvalCtx::test_default()` (mechanical; UTC+epoch reproduces prior behavior).

- [ ] **Step 5: Run to verify pass.** `cargo nextest run -p executor` → all existing tests green + the new one.

- [ ] **Step 6: Commit.**
```bash
git add crates/executor
git commit -m "SP37: introduce EvalCtx + Clock; thread through eval (UTC/epoch default)"
```

---

## Task 10: Parser — type names, typed literals, EXTRACT, AT TIME ZONE

**Files:**
- Modify: `crates/pgparser/src/{token.rs,ast.rs,parser.rs}`

- [ ] **Step 1: Write the failing parser tests** in `crates/pgparser/src/parser.rs` `mod tests` (use `parse_expr_for_test`):
```rust
#[test]
fn parses_typed_datetime_literals() {
    use crate::ast::Expr;
    assert!(matches!(parse_expr_for_test("DATE '2024-01-01'").unwrap(), Expr::Cast { .. }));
    assert!(matches!(parse_expr_for_test("INTERVAL '1 day'").unwrap(), Expr::Cast { .. }));
    assert!(matches!(parse_expr_for_test("TIMESTAMP '2024-01-01 00:00:00'").unwrap(), Expr::Cast { .. }));
    assert!(matches!(parse_expr_for_test("TIMESTAMPTZ '2024-01-01 00:00:00+00'").unwrap(), Expr::Cast { .. }));
}

#[test]
fn parses_extract_and_at_time_zone() {
    use crate::ast::Expr;
    // EXTRACT(field FROM x) → a function call extract(field-as-text, x).
    let e = parse_expr_for_test("extract(year from x)").unwrap();
    assert!(matches!(e, Expr::Func(_)));
    // AT TIME ZONE binds tighter than comparison.
    let e = parse_expr_for_test("ts AT TIME ZONE 'UTC' = ts2").unwrap();
    assert!(matches!(e, Expr::Binary { op: crate::ast::BinaryOp::Eq, .. }));
}

#[test]
fn parses_multiword_type_in_create_and_cast() {
    use crate::ast::{Expr, Statement};
    let stmts = crate::parser::parse("CREATE TABLE t (a timestamp with time zone, b time without time zone)").unwrap();
    assert!(matches!(&stmts[0], Statement::CreateTable { .. }));
    assert!(matches!(parse_expr_for_test("x::timestamp with time zone").unwrap(), Expr::Cast { ty: pgtypes::ColumnType::Timestamptz, .. }));
}
```

- [ ] **Step 2: Run to verify fail.** `cargo nextest run -p pgparser parses_typed parses_extract parses_multiword` → FAIL.

- [ ] **Step 3: Implement.**
  - **token.rs:** add `Keyword::{Date, Time, Timestamp, Timestamptz, Interval, Zone, At, Without, With, Extract, Show, Reset}` (and `Local` for SET LOCAL — Task 12). Note `With`/`Local`/`Set` may overlap with future needs; add only the missing ones. Wire each into `from_word` AND the `from_word_round_trips_every_keyword` `pairs` table. (`time`/`timestamp` etc. become keywords — verify this doesn't break using them as identifiers elsewhere; columns named `date` are uncommon and PG itself reserves these, so it's faithful.)
  - **ast.rs:** no new `Expr` variant — typed literals lower to `Expr::Cast { expr: StringLiteral, ty }` (PG treats `DATE '...'` as a cast). `EXTRACT(f FROM x)` lowers to `Expr::Func(FuncCall { name: "extract", distinct: false, args: Exprs(vec![StringLiteral(field), x]) })`. `AT TIME ZONE` lowers to `Expr::Func(FuncCall { name: "timezone", args: Exprs(vec![zone, operand]) })` (PG's internal form: `timezone(zone, ts)`).
  - **parser.rs `parse_type_name`:** before `from_sql_name`, fold the multi-word names: if `type_word` is `timestamp`/`time` and the next tokens are `with`/`without` + `time` + `zone`, consume them and normalize to `timestamp with time zone` / `time without time zone` etc. (mirror the existing `double precision` fold). `interval`/`date` are single-word.
  - **parser.rs `prefix`:** add arms for `Keyword::{Date,Time,Timestamp,Timestamptz,Interval}` when immediately followed by a `StringLit`: build `Expr::Cast { expr: Box::new(StringLiteral(s)), ty }`. Add a `Keyword::Extract` arm → `extract_expr()` (consume `(`, an ident field, `FROM` keyword, an `expr(0)`, `)`; build the `timezone`/`extract` FuncCall). 
  - **parser.rs `expr` loop:** add an `AT TIME ZONE` postfix at the comparison-ish level — when peek is `Keyword::At` and `peek2/3` are `Time`/`Zone`, consume `AT TIME ZONE`, parse the zone operand at a binding power tighter than comparison but looser than `+`/`-` (use the same level region as `||`, i.e. l_bp around 7), build the `timezone(zone, lhs)` FuncCall. (Add `peek3` helper if needed, or check tokens sequentially.)

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p pgparser` → green (+ run the libpg_query oracle test target to confirm the grammar parses like PG).

- [ ] **Step 5: Commit.**
```bash
git add crates/pgparser
git commit -m "SP37: parse date/time type names, typed literals, EXTRACT, AT TIME ZONE"
```

---

## Task 11: `eval`/`infer_type` for literals + arithmetic result types + the real session ctx

Wire the parser output into evaluation, and replace the Task 9 `EvalCtx::test_default()` at the execute entry with a context built from the session's transaction clock + effective timezone. (Clock funcs themselves land in Task 13; here `now` is captured so they can read it.)

**Files:**
- Modify: `crates/executor/src/eval.rs` (infer_type result types for temporal arithmetic), `crates/executor/src/session.rs` (capture clock/tz; build ctx)

- [ ] **Step 1: Write the failing tests** in `crates/executor/src/eval.rs` `mod tests`:
```rust
#[test]
fn datetime_literal_eval_and_infer() {
    let ctx = crate::clock::EvalCtx::test_default();
    let p = |s| pgparser::parser::parse_expr_for_test(s).unwrap();
    assert_eq!(
        eval(&p("DATE '2024-01-15'"), None, &[], &ctx).unwrap(),
        Datum::Date(pgtypes::datetime::parse_date("2024-01-15").unwrap())
    );
    assert_eq!(infer_type(&p("DATE '2024-01-15'"), None).unwrap(), ColumnType::Date);
    // date - date = int4; date + interval = timestamp (result-type inference).
    assert_eq!(infer_type(&p("DATE '2024-02-01' - DATE '2024-01-01'"), None).unwrap(), ColumnType::Int4);
    assert_eq!(infer_type(&p("DATE '2024-01-01' + INTERVAL '1 day'"), None).unwrap(), ColumnType::Timestamp);
}
```

- [ ] **Step 2: Run to verify fail.** FAIL (infer_type returns wrong type for temporal `+`/`-`).

- [ ] **Step 3: Implement.**
  - `eval`: the literal `Datum::Date(...)` etc. come "for free" via the `Expr::Cast` arm (typed literals are casts → text→type via `cast`), so no new eval arm is needed — confirm the test passes through cast. (If a direct literal path is preferred, none is required.)
  - `infer_type`: the `Expr::Binary` `Add|Sub|Mul|Div` arm currently returns `numeric_result_type`. Add a temporal check first: if either operand infers to a temporal type, return the §8 result type via a new `datetime_result_type(op, lt, rt) -> Option<ColumnType>` helper (returns `None` for non-temporal → fall back to `numeric_result_type`). Implement the matrix: `date - date → int4`; `date ± interval → timestamp`; `date + int → date`; `timestamp[tz] ± interval → same`; `timestamp[tz] - timestamp[tz] → interval`; `interval ± interval → interval`; `interval */ number → interval`; `time ± interval → time`.
  - `session.rs`: add `clock: Arc<dyn crate::clock::Clock>` to `SqlSession` (param on `new`, default `Arc::new(SystemClock)` at the engine seam — thread through engine construction; compile errors will pinpoint sites). Add `txn_now: Option<jiff::Timestamp>` to `TxnCtx`, captured in `begin` as `self.clock.now()`. Add a helper `fn eval_ctx(&self, stmt_now) -> EvalCtx` building `{ now: txn_now (or stmt_now in autocommit), stmt_now, time_zone: <effective GUC tz, Task 12>, clock: self.clock.clone() }`. For THIS task the effective tz is still UTC (GUC lands in Task 12). Replace the `EvalCtx::test_default()` placeholders in the execute path with `self.eval_ctx(self.clock.now())`.

- [ ] **Step 4: Run to verify pass.** `cargo nextest run -p executor` → green.

- [ ] **Step 5: Commit.**
```bash
git add crates/executor
git commit -m "SP37: temporal result-type inference + session-built EvalCtx (clock captured)"
```

---

## Task 12: Transactional `SET`/`SHOW`/`RESET timezone` (the GUC)

The faithful PG transactional GUC (spec §11). New statements + the `GucState` machine wired into begin/commit/rollback, feeding `EvalCtx.time_zone`. Mid-session `ParameterStatus` on a committed change.

**Files:**
- Modify: `crates/pgparser/src/{token.rs,ast.rs,parser.rs}` (SET/SHOW/RESET grammar)
- Modify: `crates/executor/src/session.rs` (GucState + dispatch)
- Modify: `crates/pgwire/src/{session.rs,messages/backend.rs}` (re-announce ParameterStatus)

- [ ] **Step 1: Write the failing GUC state tests** in `crates/executor/src/session.rs` `mod tests` (or a focused unit test on a `GucState` helper type — prefer a pure `GucState` with no async so the matrix is exhaustive and fast):

```rust
#[test]
fn guc_timezone_transactional_semantics() {
    use crate::session::GucState;
    let mut g = GucState::default();
    assert_eq!(g.effective(), "UTC");
    // autocommit SET persists.
    g.set_session("America/New_York".into());
    g.commit();
    assert_eq!(g.effective(), "America/New_York");
    // SET inside a txn, then ROLLBACK → reverts.
    g.set_session("UTC".into());
    assert_eq!(g.effective(), "UTC"); // visible within the txn
    g.rollback();
    assert_eq!(g.effective(), "America/New_York"); // reverted
    // SET inside a txn, then COMMIT → persists.
    g.set_session("UTC".into());
    g.commit();
    assert_eq!(g.effective(), "UTC");
    // SET LOCAL reverts at txn end regardless.
    g.set_local("America/New_York".into());
    assert_eq!(g.effective(), "America/New_York");
    g.commit();
    assert_eq!(g.effective(), "UTC"); // local dropped at commit
    // RESET = back to default.
    g.set_session("America/New_York".into());
    g.commit();
    g.reset();
    g.commit();
    assert_eq!(g.effective(), "UTC");
}
```

- [ ] **Step 2: Run to verify fail.** FAIL (`GucState` undefined).

- [ ] **Step 3: Implement `GucState`** in `session.rs` (pure, no async):
```rust
/// SP37: the transactional `timezone` GUC. `committed` survives transaction end;
/// the two overrides are promoted (SET) or dropped (rollback / SET LOCAL) at txn
/// end. Models PG's no-savepoint behavior.
#[derive(Debug, Clone)]
pub(crate) struct GucState {
    committed: String,
    txn_session_override: Option<String>,
    txn_local_override: Option<String>,
}
impl Default for GucState {
    fn default() -> Self {
        Self { committed: "UTC".into(), txn_session_override: None, txn_local_override: None }
    }
}
impl GucState {
    pub(crate) fn effective(&self) -> &str {
        self.txn_local_override.as_deref()
            .or(self.txn_session_override.as_deref())
            .unwrap_or(&self.committed)
    }
    pub(crate) fn set_session(&mut self, v: String) { self.txn_session_override = Some(v); }
    pub(crate) fn set_local(&mut self, v: String) { self.txn_local_override = Some(v); }
    pub(crate) fn reset(&mut self) { self.txn_session_override = Some("UTC".into()); }
    pub(crate) fn commit(&mut self) {
        if let Some(v) = self.txn_session_override.take() { self.committed = v; }
        self.txn_local_override = None;
    }
    pub(crate) fn rollback(&mut self) {
        self.txn_session_override = None;
        self.txn_local_override = None;
    }
}
```
Note: in **autocommit** (no open txn), the executor calls `set_session(..)` then `commit()` immediately (the implicit single-statement txn), so a bare `SET` persists; `set_local` in autocommit is then dropped by that immediate `commit()` — matching PG's "SET LOCAL outside a txn has no lasting effect".

- [ ] **Step 4: Parser — SET/SHOW/RESET.** Add to `ast.rs`:
```rust
    /// SP37: `SET [LOCAL] <name> = <value>` / `SET <name> TO <value>` / `SET TIME ZONE ...`.
    Set { local: bool, name: String, value: SetValue },
    /// SP37: `SHOW <name>` / `SHOW TIME ZONE`.
    Show { name: String },
    /// SP37: `RESET <name>`.
    Reset { name: String },
```
with `pub enum SetValue { Default, Value(String) }`. In `parser.rs` `statement()`, add arms for `Keyword::Set` (currently only consumed inside UPDATE — at statement start it's a SET stmt), `Keyword::Show`, `Keyword::Reset`. Parse: `SET [LOCAL] (timezone|time zone) (=|TO)? (value|DEFAULT|LOCAL)`; normalize `SET TIME ZONE x` to `name = "timezone"`. Value can be a string literal, an ident, or a number (offset like `SET TIME ZONE -5` is rare — accept string/ident). Write parser unit tests for each spelling.

- [ ] **Step 5: Executor dispatch.** In `session.rs`, add `guc: GucState` to `SqlSession` (default). In `run_one`, add arms:
  - `Statement::Set { local, name, value }` → require `name` is `timezone`/`TimeZone` (else 42704 unrecognized, except datestyle/intervalstyle default-value no-op per §11.1); validate the zone via `jiff::tz::TimeZone::get(v)` (unknown → `ExecError` 22023); then if in a txn, `guc.set_local/set_session`; if autocommit, `set_session`+`commit`. On a committed change of the effective value, signal the wire layer to emit a `ParameterStatus` (see Step 6). Return `QueryResult::Command { tag: "SET".into() }`.
  - `Statement::Show { name }` → build a `QueryResult::Rows` with one `FieldDescription` (name `TimeZone`, type_oid text=25) and one row = effective value. Tag `"SHOW"`.
  - `Statement::Reset { name }` → `guc.reset()` (+ commit if autocommit); `Command { tag: "RESET" }`.
  - In `commit_cmd`/`rollback_cmd`/the failed-txn path, call `self.guc.commit()` / `self.guc.rollback()`.
  - `eval_ctx` (Task 11) now reads `TimeZone::get(self.guc.effective())` for `time_zone`.

- [ ] **Step 6: Mid-session ParameterStatus.** The executor can't write wire bytes; surface a "TimeZone changed to X" signal on `QueryResult`. Minimal approach: when a committed `SET timezone` changes the effective committed value, return a dedicated `QueryResult::Command { tag }` AND have the pgwire simple/extended path, after running statements, compare the session's reported timezone. Simpler faithful approach: add an optional `param_status: Vec<(String,String)>` to the engine's per-query response, or thread a callback. **Chosen:** add `backend::parameter_status` emission in `write_results`/`handle_execute` when the engine reports a changed GUC — implement by having the engine's query entry point return the new effective timezone alongside results (extend the engine trait's return or stash it on the session and read it in pgwire). Write a wire test (Task 14) asserting a `ParameterStatus('TimeZone', 'America/New_York')` is sent after `SET TIME ZONE`. (Implementer: pick the least-invasive wiring consistent with the engine trait; document it.)

- [ ] **Step 7: Run to verify pass.** `cargo nextest run -p executor guc_timezone` and `-p pgparser` SET/SHOW tests → PASS.

- [ ] **Step 8: Commit.**
```bash
git add crates/pgparser crates/executor crates/pgwire
git commit -m "SP37: transactional SET/SHOW/RESET timezone GUC + mid-session ParameterStatus"
```

---

## Task 13: Functions — clock family, extract/date_part, date_trunc, age

**Files:**
- Create: `crates/executor/src/datetime_fn.rs`
- Modify: `crates/executor/src/{lib.rs,eval.rs,func.rs,agg.rs}`

- [ ] **Step 1: Write the failing tests** in `crates/executor/src/datetime_fn.rs` `mod tests` (use a `FixedClock` + UTC ctx):
```rust
#[cfg(test)]
mod tests {
    use crate::clock::{EvalCtx, FixedClock};
    use pgtypes::{ColumnType, Datum};
    use std::sync::Arc;

    fn ctx_at(rfc3339: &str) -> EvalCtx {
        let now: jiff::Timestamp = rfc3339.parse().unwrap();
        EvalCtx { now, stmt_now: now, time_zone: jiff::tz::TimeZone::UTC, clock: Arc::new(FixedClock(now)) }
    }
    fn ev(sql: &str, ctx: &EvalCtx) -> Datum {
        crate::eval::eval(&pgparser::parser::parse_expr_for_test(sql).unwrap(), None, &[], ctx).unwrap()
    }
    fn num(s: &str) -> Datum { Datum::Numeric(pgtypes::numeric::parse(s).unwrap()) }

    #[test]
    fn now_is_transaction_stable_and_typed_timestamptz() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(ev("now()", &ctx), ev("current_timestamp", &ctx)); // same instant
        assert_eq!(
            crate::eval::infer_type(&pgparser::parser::parse_expr_for_test("now()").unwrap(), None).unwrap(),
            ColumnType::Timestamptz
        );
        assert_eq!(ev("current_date", &ctx), Datum::Date("2024-01-15".parse().unwrap()));
    }

    #[test]
    fn extract_returns_numeric_date_part_returns_float8() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(ev("extract(year from TIMESTAMP '2024-01-15 13:45:06')", &ctx), num("2024"));
        assert_eq!(ev("extract(month from DATE '2024-07-01')", &ctx), num("7"));
        assert_eq!(ev("date_part('hour', TIMESTAMP '2024-01-15 13:45:06')", &ctx), Datum::Float8(13.0));
    }

    #[test]
    fn date_trunc_and_age() {
        let ctx = ctx_at("2024-01-15T12:00:00Z");
        assert_eq!(
            ev("date_trunc('month', TIMESTAMP '2024-07-15 13:45:06')", &ctx),
            Datum::Timestamp(pgtypes::datetime::parse_timestamp("2024-07-01 00:00:00").unwrap())
        );
        // age(a, b) symbolic interval.
        assert_eq!(
            ev("age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')", &ctx),
            Datum::Interval(pgtypes::datetime::Interval { months: 2, days: 0, micros: 0 })
        );
    }
}
```

- [ ] **Step 2: Run to verify fail.** FAIL (functions undefined; `now()` is rejected as unknown).

- [ ] **Step 3: Implement `datetime_fn.rs`** — a registry `fn datetime_func(name) -> Option<DtFunc>` (clock family, `extract`, `date_part`, `date_trunc`, `age`, `timezone`), an `eval_datetime(fc, ctx, eval_child) -> Result<Datum, ExecError>`, and `datetime_func_result_type(fc, table) -> Result<ColumnType, ExecError>` (named distinctly from the arithmetic `eval::datetime_result_type` of Task 11 — two different helpers in two modules). Use jiff for field extraction. `extract` returns `Numeric`, `date_part` returns `Float8`. Clock funcs read `ctx`. `age` implements PG's month-borrowing algorithm (verify vs oracle). The niladic keyword funcs (`current_timestamp`, `current_date`, `current_time`, `localtimestamp`, `localtime`, `now` is `now()` with parens) need parser support — handle `current_timestamp`/etc. as bare identifiers that `eval`/`infer_type` route to `datetime_fn` even without `()` (add to the parser: when a bare ident matches one of these names and is NOT followed by `(`, build a zero-arg `Expr::Func`). 

- [ ] **Step 4: Wire dispatch.** In `eval.rs`'s `Expr::Func` arm and `infer_type`'s `Expr::Func` arm, try `crate::func::is_scalar` first, then `crate::datetime_fn::is_datetime_func`, before falling to the aggregate path. Thread `ctx` into `eval_datetime`. `agg.rs`'s four traversals must recurse through a datetime func's args (so `max(extract(year from ts))` works) — add the same recursion they do for scalar `Func`.

- [ ] **Step 5: Run to verify pass.** `cargo nextest run -p executor datetime_fn` → PASS. (Also re-run `-p pgparser` for the bare-keyword-function parsing.)

- [ ] **Step 6: Commit.**
```bash
git add crates/executor crates/pgparser
git commit -m "SP37: clock funcs + extract/date_part/date_trunc/age"
```

---

## Task 14: End-to-end wire test

**Files:**
- Create: `crates/executor/tests/datetime.rs`

- [ ] **Step 1: Write the test.** Mirror an existing wire test (e.g. `crates/executor/tests/numeric.rs` if present, else `floating_point.rs`) for harness setup. Inject a `FixedClock` into the engine so clock funcs are deterministic. Cover: a table with all five column types (round-trip + result OIDs via RowDescription); typed literals; arithmetic; comparison/ORDER BY; the cast matrix; `extract`/`date_trunc`/`age`; `AT TIME ZONE`; `SET TIME ZONE 'America/New_York'` then a `timestamptz` select renders in NY and a `ParameterStatus` was emitted; `SHOW timezone`; transactional `SET` (BEGIN; SET; ROLLBACK; SHOW reverts); and the error surface (`DATE '2024-02-30'` → 22008, unknown zone → 22023, `INTERVAL → DATE` cast → 42846).

- [ ] **Step 2: Run.** `cargo nextest run -p executor --test datetime` → PASS.

- [ ] **Step 3: Commit.**
```bash
git add crates/executor/tests/datetime.rs
git commit -m "SP37: end-to-end wire test for date/time types"
```

---

## Task 15: Conformance corpus + CLAUDE.md slice summary

**Files:**
- Create: `crates/conformance/corpus/datetime.sql`, `crates/conformance/corpus/interval.sql`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Write the corpus.** Follow the header-comment + sectioned-`SELECT` style of `crates/conformance/corpus/numeric.sql`. Pin `SET TIME ZONE 'UTC'` at the top of `datetime.sql`, plus a section using `America/New_York` on a post-2007 (stable-DST) date. Cover: literals, casts, arithmetic, comparison/order, `extract`/`date_part`/`date_trunc`/`age`, `AT TIME ZONE`, `SHOW timezone`. **Exclude clock funcs** (non-deterministic — comment why). `interval.sql`: literals/output, arithmetic, the `'1 month' = '30 days'` grouping, `justify`-free interval math.

- [ ] **Step 2: Validate vs the oracle locally** (the spec's bar — `cast.sql`/`numeric.sql` validated 100%). Run the conformance runner against a local PostgreSQL (16+); fix every diff by matching PG (adjust the renderer, not the expectation). Document any unavoidable deviation in the corpus header (e.g. jiff calendar range).

- [ ] **Step 3: Run the full suite.** `cargo nextest run --workspace` and `cargo test --workspace --doc` → green. `cargo fmt --all` + `cargo clippy --workspace --all-targets -- -D warnings` → clean.

- [ ] **Step 4: Guard + CLAUDE.md.** Confirm UAC-safe: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` → empty (the new test is `datetime.rs`). Add the `SP37` slice summary paragraph to `CLAUDE.md` (mirror SP32's), updating the `executor` integration-test list to include `datetime`, noting: the jiff dep + bundled tzdb, the five types, transactional SET timezone, the no-Stateright justification, and the documented deviations/non-goals (timetz, fractional-second typmod, interval field qualifiers, savepoint-GUC stacking, non-default DateStyle/IntervalStyle, jiff calendar range; and that SP38 carries to_char/make_*/justify_*).

- [ ] **Step 5: Commit.**
```bash
git add crates/conformance CLAUDE.md
git commit -m "SP37: date/time conformance corpus + CLAUDE.md slice summary"
```

---

## Final verification (before opening a PR)

- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo nextest run --workspace` green
- [ ] `cargo test --workspace --doc` green
- [ ] `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` empty
- [ ] mutation sweep on `pgtypes` (`cargo mutants -p pgtypes` informational) — kill survivors in the new `datetime`/`cast`/`ops` code with targeted unit tests
- [ ] conformance corpus diffs clean vs the oracle
