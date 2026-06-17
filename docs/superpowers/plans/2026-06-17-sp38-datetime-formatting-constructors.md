# SP38 — date/time formatting + constructors + numeric `to_char` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PostgreSQL's `to_char` (date/time + numeric template engines), `to_timestamp`/`to_date`, the `make_*` field constructors, and `justify_days`/`justify_hours`/`justify_interval`, all as ordinary scalar functions.

**Architecture:** Pure, value-only template/constructor logic lives in `pgtypes` (`pgtypes::datetime` for the date/time engine, parse, `make_*`, `justify_*`; `pgtypes::numeric` for the numeric engine) — the mutation-tested layer. A new `executor::format_fn` module owns dispatch, arity/type checks, and `EvalCtx`-aware argument handling, wired into `eval.rs` as a new `Expr::Func` arm. No parser changes (all ordinary function calls). No Stateright model — pure-value / single-engine carve-out (SP27–SP37 precedent).

**Tech Stack:** Rust 2024, `jiff` 0.2 (civil date/time + tzdb), `bigdecimal` 0.4 (numeric), cargo-nextest, conformance corpus diffed vs PostgreSQL 18.

**Spec:** `docs/superpowers/specs/2026-06-17-sp38-datetime-formatting-constructors-design.md` — read §1.2/§1.3 for the exhaustive in-scope / deferred pattern lists.

**Cross-cutting rules (apply to every task):**
- Tests run under **cargo-nextest**: `cargo nextest run -p <crate>`. nextest does NOT run doctests — the final task runs `cargo test --workspace --doc` separately.
- `pgtypes` is a **mutation-baseline crate**: after each `pgtypes` task, the value code must survive cargo-mutants with zero missed mutants (excluding genuinely-equivalent mutants with a rationale in `.cargo/mutants.toml`). Write enough boundary-value tests to kill every value-affecting mutant.
- **The PG oracle is the arbiter of exact `to_char` output.** The representative expected strings in this plan's unit tests are high-confidence cases; PG's `to_char` has subtle padding/sign/overflow rules. Before committing each `to_char` task, run every (value, template) pair through a real PostgreSQL (≥16) and pin the exact string it returns. The conformance corpus (Task 9) is validated locally vs PG before commit (per the corpus-validation discipline).
- Existing error variants (no new ones): `pgtypes::TypeError::{InvalidDatetimeFormat (22007), DatetimeFieldOverflow (22008), Domain{sqlstate,message}, TypeMismatch (42804)}`; `executor::ExecError::{UndefinedFunction (42883), InvalidParameterValue (22023), TypeMismatch (42804)}`.
- Commit after each task with a `SP38:` prefix and the `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` trailer.

---

## File Structure

- `crates/pgtypes/src/datetime.rs` (modify) — add `DateTimeFields`, `format_datetime`, `format_interval`, `parse_by_template` + `ParsedDateTime`, `make_date`/`make_time`/`make_timestamp_civil`/`make_interval`, `justify_days`/`justify_hours`/`justify_interval`.
- `crates/pgtypes/src/numeric.rs` (modify) — add `format_numeric`.
- `crates/executor/src/format_fn.rs` (create) — `FmtFunc`, `is_format_func`, `format_func_result_type`, `eval_format`.
- `crates/executor/src/eval.rs` (modify) — dispatch arms in `eval` (~line 60) and `infer_type` (~line 483).
- `crates/executor/src/lib.rs` (modify) — `mod format_fn;`.
- `crates/executor/src/agg.rs` (modify, if needed) — confirm the four traversals recurse through format-function args.
- `crates/executor/tests/formatting.rs` (create) — the end-to-end wire test.
- `crates/conformance/corpus/{to_char_datetime,to_char_numeric,make_justify}.sql` (create).
- `CLAUDE.md` (modify) — SP38 slice summary + `executor` integration-test list adds `formatting`.

---

## Task 1: `pgtypes::datetime` — `DateTimeFields` + `format_datetime` (date/time `to_char` engine)

**Files:**
- Modify: `crates/pgtypes/src/datetime.rs`
- Test: same file, `#[cfg(test)] mod tests`

The engine tokenizes a template into longest-match pattern tokens + literal runs, then renders each from a pre-extracted field struct. The executor (Task 6) fills `DateTimeFields` from a `Datum`; this task is pure value logic.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/pgtypes/src/datetime.rs`:

```rust
#[test]
fn format_datetime_core_patterns() {
    use super::{format_datetime, DateTimeFields};
    // 2024-01-15 13:45:06.5, a Monday (dow Sun=1 → 2; ISO dow Mon=1 → 1).
    let f = DateTimeFields::from_civil(
        jiff::civil::DateTime::constant(2024, 1, 15, 13, 45, 6, 500_000_000),
        None,
    );
    let fmt = |t: &str| format_datetime(t, &f).expect(t);
    assert_eq!(fmt("YYYY-MM-DD HH24:MI:SS"), "2024-01-15 13:45:06");
    assert_eq!(fmt("HH12:MI:SS PM"), "01:45:06 PM");
    assert_eq!(fmt("HH12:MI am"), "01:45 pm");
    assert_eq!(fmt("Mon Month"), "Jan January  "); // Month blank-padded to 9
    assert_eq!(fmt("FMMonth DD, YYYY"), "January 15, 2024"); // FM suppresses padding
    assert_eq!(fmt("Dy Day"), "Mon Monday   "); // Day padded to 9
    assert_eq!(fmt("Q"), "1");
    assert_eq!(fmt("MS US"), "500 500000");
    assert_eq!(fmt(r#""year:" YYYY"#), "year: 2024"); // quoted literal
    assert_eq!(fmt("DDth"), "15th"); // ordinal suffix
    assert_eq!(fmt("FF3"), "500");
}

#[test]
fn format_datetime_timezone_patterns() {
    use super::{format_datetime, DateTimeFields};
    // timestamptz rendered at -05:00 (offset present).
    let f = DateTimeFields::from_civil(
        jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
        Some(-5 * 3600),
    );
    assert_eq!(format_datetime("OF", &f).expect("OF"), "-05");
    assert_eq!(format_datetime("TZH:TZM", &f).expect("tz"), "-05:00");
    // A plain timestamp (no offset) renders TZ patterns as empty (PG behavior).
    let g = DateTimeFields::from_civil(
        jiff::civil::DateTime::constant(2024, 1, 15, 12, 0, 0, 0),
        None,
    );
    assert_eq!(format_datetime("HH24OF", &g).expect("notz"), "12");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo nextest run -p pgtypes format_datetime`
Expected: FAIL — `DateTimeFields` / `format_datetime` not found.

- [ ] **Step 3: Implement `DateTimeFields` + `format_datetime`**

Add to `crates/pgtypes/src/datetime.rs` (use the existing `use jiff::civil::{Date, DateTime, Time};`). Implement:

```rust
/// Pre-extracted civil fields for the `to_char` date/time engine. The executor
/// fills this from a Datum; a `timestamptz` supplies `tz_offset_secs`.
pub struct DateTimeFields {
    pub year: i32,
    pub month: u32,     // 1..=12
    pub day: u32,       // 1..=31
    pub hour: u32,      // 0..=23
    pub minute: u32,
    pub second: u32,
    pub micros: u32,    // 0..=999_999
    pub iso_dow: u32,   // Mon=1..Sun=7
    pub dow: u32,       // Sun=1..Sat=7  (PG `D`)
    pub doy: u32,       // 1..=366
    pub iso_week: u32,
    pub iso_year: i32,
    pub week_of_year: u32,   // (doy-1)/7 + 1  (PG `WW`)
    pub week_of_month: u32,  // (day-1)/7 + 1  (PG `W`)
    pub tz_offset_secs: Option<i32>,
}

impl DateTimeFields {
    pub fn from_civil(dt: DateTime, tz_offset_secs: Option<i32>) -> Self { /* read jiff fields */ }
}

/// The date/time `to_char` engine. Tokenize `template` (longest pattern match
/// first — e.g. `HH24` before `HH12`/`HH`, `YYYY` before `YY`), apply `FM`/`TH`,
/// render. See spec §1.2 for the full in-scope pattern set.
pub fn format_datetime(template: &str, f: &DateTimeFields) -> Result<String, TypeError> { /* ... */ }
```

Implementation notes:
- Tokenize by scanning left-to-right. At each position: a `"` begins a quoted literal (honor `\"`/`\\` inside, emit verbatim); `FM` sets a one-shot "suppress padding/leading-zeros for the next pattern" flag; `TH`/`th` after a numeric pattern appends the English ordinal suffix of the just-rendered number (`st`/`nd`/`rd`/`th`, case per `TH` vs `th`); otherwise try the longest matching pattern keyword (build a static ordered list, longest first). A character matching no pattern is emitted literally (PG behavior — do NOT error).
- Month/day **names**: `Mon`/`MON`/`mon` (3-letter), `Month`/`MONTH`/`month` (full, blank-padded to width 9 unless `FM`). English/C-locale names only (deferred: `TM`).
- `RM`/`rm`: Roman numeral month I..XII (a 12-entry table), upper/lower.
- Numeric patterns pad to their natural width with leading zeros unless `FM` (e.g. `HH24` → 2 digits, `YYYY` → 4, `DDD` → 3). `Y,YYY` inserts a comma in the 4-digit year.
- `AM`/`PM` (and `am`/`pm`, `A.M.`/`P.M.`, `a.m.`/`p.m.`): from `hour < 12`.
- `HH12` = `((hour + 11) % 12) + 1`.
- Timezone patterns (`TZ`/`tz`/`OF`/`TZH`/`TZM`) read `tz_offset_secs`; when `None`, render empty. `OF` = sign + `HH` (+`:MM` only if minutes nonzero); `TZH` = sign+2-digit hours; `TZM` = 2-digit minutes; `TZ`/`tz` render the offset as `±HH` (abbreviation lookup is deferred — render the offset).
- Return `TypeError` only on an internal range failure; an unrecognized pattern char is literal, never an error.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p pgtypes format_datetime`
Expected: PASS. **Then oracle-check each asserted string against PG and adjust if any differ.** Expand the test to cover the rest of §1.2 (every year/month/day/week/time/era/tz pattern + `FM`/`TH`/quoted), oracle-pinning each.

- [ ] **Step 5: Mutation sweep + commit**

Run: `cargo mutants -p pgtypes -f crates/pgtypes/src/datetime.rs` (or the configured subset). Kill every missed mutant in the new code with a targeted test; exclude only provably-equivalent mutants with a rationale.

```bash
git add crates/pgtypes/src/datetime.rs
git commit -m "SP38: to_char date/time template engine (format_datetime)"
```

---

## Task 2: `pgtypes::datetime` — `format_interval`

**Files:**
- Modify: `crates/pgtypes/src/datetime.rs`
- Test: same file

PG renders an interval through the template using its **stored** fields, not normalized across day/month boundaries (mirrors PG `interval2tm`): clock patterns read `micros` (hours = `micros / 3_600_000_000`, may exceed 24), `DD` reads `days`, `MM`/`YYYY` read `months`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn format_interval_uses_stored_fields() {
    use super::{format_interval, Interval};
    let fmt = |iv: Interval, t: &str| format_interval(iv, t).expect(t);
    // 36 hours: HH24 reads the micros component → 36 (not normalized to 1 day 12h).
    let h36 = Interval { months: 0, days: 0, micros: 36 * 3_600_000_000 };
    assert_eq!(fmt(h36, "HH24:MI:SS"), "36:00:00");
    // 1 day 02:03:04 → DD=01, HH24=02 (days stay separate from the clock).
    let d1 = Interval { months: 0, days: 1, micros: (2 * 3600 + 3 * 60 + 4) * 1_000_000 };
    assert_eq!(fmt(d1, "DD HH24:MI:SS"), "01 02:03:04");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo nextest run -p pgtypes format_interval`
Expected: FAIL — `format_interval` not found.

- [ ] **Step 3: Implement**

```rust
/// `to_char(interval, fmt)`. Reuses the same tokenizer as `format_datetime`,
/// but the field source is the interval's STORED months/days/micros (PG `interval2tm`):
///   year = months/12, month = months%12, day = days,
///   hour = micros / 3_600_000_000 (NOT mod 24), minute/second/frac from the remainder.
pub fn format_interval(iv: Interval, template: &str) -> Result<String, TypeError> { /* ... */ }
```

Factor the tokenizer/renderer from Task 1 so both share it (e.g. an internal `render_tokens(tokens, &dyn FieldSource)` or a private `FieldSet` enum). Keep it DRY.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p pgtypes format_interval`
Expected: PASS. Oracle-check; expand to the interval-relevant patterns in §1.2.

- [ ] **Step 5: Mutation sweep + commit**

```bash
git add crates/pgtypes/src/datetime.rs
git commit -m "SP38: to_char(interval, fmt) — stored-field rendering"
```

---

## Task 3: `pgtypes::datetime` — `parse_by_template` (drives `to_timestamp`/`to_date`)

**Files:**
- Modify: `crates/pgtypes/src/datetime.rs`
- Test: same file

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn parse_by_template_extracts_fields() {
    use super::parse_by_template;
    let p = parse_by_template("YYYY-MM-DD HH24:MI:SS", "2024-01-15 13:45:06").expect("p");
    assert_eq!((p.year, p.month, p.day), (2024, 1, 15));
    assert_eq!((p.hour, p.minute, p.second), (13, 45, 6));
    // month name + 12-hour + meridiem
    let q = parse_by_template("Mon DD YYYY HH12:MI PM", "Jul 04 2024 01:30 PM").expect("q");
    assert_eq!((q.year, q.month, q.day, q.hour, q.minute), (2024, 7, 4, 13, 30));
    // absent fields default (PG): year→1, month→1, day→1, time→0.
    let d = parse_by_template("YYYY", "2030").expect("d");
    assert_eq!((d.year, d.month, d.day, d.hour), (2030, 1, 1, 0));
}

#[test]
fn parse_by_template_errors() {
    use super::parse_by_template;
    // non-digit where a digit is required → 22007.
    assert_eq!(
        parse_by_template("YYYY-MM-DD", "abcd-01-01").unwrap_err().sqlstate(),
        "22007"
    );
    // out-of-range field → 22008.
    assert_eq!(
        parse_by_template("YYYY-MM-DD", "2024-13-01").unwrap_err().sqlstate(),
        "22008"
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo nextest run -p pgtypes parse_by_template`
Expected: FAIL — not found.

- [ ] **Step 3: Implement**

```rust
pub struct ParsedDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micros: u32,
    pub tz_offset_secs: Option<i32>,
}

/// Template-driven parse for `to_timestamp`/`to_date`. Tokenize `template`; for a
/// numeric pattern consume up to its width of leading digits from `input`; for a
/// name pattern (`Mon`/`Month`) match a month/day name case-insensitively; literal
/// template chars must match (PG skips most separators — we accept the literal in
/// `input` or any non-alphanumeric run). Returns `ParsedDateTime` with PG defaults
/// for absent fields. Bad shape → 22007; out-of-range field → 22008.
pub fn parse_by_template(template: &str, input: &str) -> Result<ParsedDateTime, TypeError> { /* ... */ }
```

Notes: maintain a cursor into `input`. Handle `HH12`+`PM` by converting to 24-hour at the end. Validate ranges (month 1..12, day 1..31, hour 0..23, etc.) → `DatetimeFieldOverflow` (22008). The civil-date validity (e.g. Feb 30) is checked by the caller when it builds the `DateTime`/`Date` (Task 6) — but a clearly out-of-range field (month 13) is caught here.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p pgtypes parse_by_template`
Expected: PASS. Oracle-check; cover the in-scope patterns.

- [ ] **Step 5: Mutation sweep + commit**

```bash
git add crates/pgtypes/src/datetime.rs
git commit -m "SP38: parse_by_template (to_timestamp/to_date field extraction)"
```

---

## Task 4: `pgtypes::datetime` — `make_*` + `justify_*` value helpers

**Files:**
- Modify: `crates/pgtypes/src/datetime.rs`
- Test: same file

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn make_constructors() {
    use super::{make_date, make_time, make_timestamp_civil, make_interval, Interval};
    assert_eq!(make_date(2024, 7, 4).expect("d"), jiff::civil::date(2024, 7, 4));
    // make_time(hour, min, sec) — fractional seconds → micros.
    assert_eq!(
        make_time(13, 45, 6.5).expect("t"),
        jiff::civil::time(13, 45, 6, 500_000_000)
    );
    assert_eq!(
        make_timestamp_civil(2024, 7, 4, 13, 45, 6.0).expect("ts"),
        jiff::civil::datetime(2024, 7, 4, 13, 45, 6, 0)
    );
    // make_interval positional: 1 year, 2 months, 0 weeks, 3 days.
    assert_eq!(
        make_interval(1, 2, 0, 3, 0, 0, 0.0).expect("iv"),
        Interval { months: 14, days: 3, micros: 0 }
    );
    // weeks fold into days; fractional secs into micros.
    assert_eq!(
        make_interval(0, 0, 2, 0, 0, 0, 1.5).expect("iv"),
        Interval { months: 0, days: 14, micros: 1_500_000 }
    );
    // out-of-range field → 22008.
    assert_eq!(make_date(2024, 13, 1).unwrap_err().sqlstate(), "22008");
}

#[test]
fn justify_helpers() {
    use super::{justify_days, justify_hours, justify_interval, Interval};
    // 35 days → 1 month 5 days.
    assert_eq!(
        justify_days(Interval { months: 0, days: 35, micros: 0 }),
        Interval { months: 1, days: 5, micros: 0 }
    );
    // 27 hours → 1 day 3 hours.
    assert_eq!(
        justify_hours(Interval { months: 0, days: 0, micros: 27 * 3_600_000_000 }),
        Interval { months: 0, days: 1, micros: 3 * 3_600_000_000 }
    );
    // PG: justify_interval('1 mon -1 hour') = '29 days 23:00:00'.
    assert_eq!(
        justify_interval(Interval { months: 1, days: 0, micros: -3_600_000_000 }),
        Interval { months: 0, days: 29, micros: 23 * 3_600_000_000 }
    );
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo nextest run -p pgtypes "make_constructors|justify_helpers"`
Expected: FAIL — not found.

- [ ] **Step 3: Implement**

```rust
/// `make_date(year, month, day)`. Out-of-range → 22008.
pub fn make_date(year: i32, month: i32, day: i32) -> Result<Date, TypeError> { /* Date::new + map_err */ }
/// `make_time(hour, min, sec)`; fractional `sec` → micros (truncate to µs).
pub fn make_time(hour: i32, min: i32, sec: f64) -> Result<Time, TypeError> { /* ... */ }
/// civil timestamp builder shared by make_timestamp / make_timestamptz (the
/// executor wraps the tz step for the latter).
pub fn make_timestamp_civil(y: i32, mo: i32, d: i32, h: i32, mi: i32, sec: f64)
    -> Result<DateTime, TypeError> { /* ... */ }
/// `make_interval(years, months, weeks, days, hours, mins, secs)`; weeks→days,
/// years→months, fractional secs→micros. Field overflow → 22008.
pub fn make_interval(years: i32, months: i32, weeks: i32, days: i32, hours: i32, mins: i32, secs: f64)
    -> Result<Interval, TypeError> { /* months = years*12+months; days = weeks*7+days;
       micros = ((hours*60+mins)*60) * 1e6 + round(secs*1e6); checked arithmetic */ }

/// PG `interval_justify_days`: roll 30-day groups of `days` into `months`.
pub fn justify_days(iv: Interval) -> Interval { /* ... */ }
/// PG `interval_justify_hours`: roll 24-hour groups of `micros` into `days`.
pub fn justify_hours(iv: Interval) -> Interval { /* ... */ }
/// PG `interval_justify_interval`: justify hours then days, then sign-normalize so
/// no field's sign disagrees with the whole (replicate PG's borrow/carry).
pub fn justify_interval(iv: Interval) -> Interval { /* ... */ }
```

Notes: for the sign-normalization in `justify_interval`, mirror PG's `interval_justify_interval` (`src/backend/utils/adt/timestamp.c`): after the 24h→day and 30d→month rolls, if `months > 0 && days < 0` borrow a month into +30 days (and the symmetric negative cases), likewise day↔micros. Cover the mixed-sign cases with tests.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p pgtypes "make_constructors|justify_helpers"`
Expected: PASS. Oracle-check the justify mixed-sign cases.

- [ ] **Step 5: Mutation sweep + commit**

```bash
git add crates/pgtypes/src/datetime.rs
git commit -m "SP38: make_* constructors + justify_days/hours/interval"
```

---

## Task 5: `pgtypes::numeric` — `format_numeric` (numeric `to_char` engine)

**Files:**
- Modify: `crates/pgtypes/src/numeric.rs`
- Test: same file

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn format_numeric_core() {
    use super::{format_numeric, parse};
    let n = |s: &str| parse(s).expect(s);
    let fmt = |v: &str, t: &str| format_numeric(t, &n(v)).expect(t);
    // default reserves a leading sign column → leading blank for non-negative.
    assert_eq!(fmt("485", "999"), " 485");
    assert_eq!(fmt("-485", "999"), "-485");
    assert_eq!(fmt("485", "FM999"), "485");      // FM strips the sign blank
    assert_eq!(fmt("485", "0999"), " 0485");     // 0 forces a leading zero
    assert_eq!(fmt("12", "99"), " 12");
    assert_eq!(fmt("1234567", "9,999,999"), " 1,234,567");
    assert_eq!(fmt("1234567", "FM9,999,999"), "1,234,567");
    assert_eq!(fmt("1234.5", "9,999.9"), " 1,234.5");
    // rounding to the fractional digit count (half away from zero).
    assert_eq!(fmt("1.235", "9.99"), " 1.24");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo nextest run -p pgtypes format_numeric`
Expected: FAIL — not found.

- [ ] **Step 3: Implement**

```rust
/// The numeric `to_char` engine (independent of the date/time one). See spec §1.2
/// for the in-scope pattern set (`9 0 . , D G S MI PL SG PR L $ V FM TH B`, `#`
/// overflow). Returns text. Reuse `round` for the half-away-from-zero rounding.
pub fn format_numeric(template: &str, value: &BigDecimal) -> Result<String, TypeError> { /* ... */ }
```

Algorithm (spec §3.2): parse template → a grid descriptor (integer `9`/`0` count, fractional count, group-separator positions, decimal-point index, sign mode, currency/`V`/`FM`/`B`/`PR`/`TH` flags); apply `V` (×10ⁿ); round to the fractional count; lay digits (blank leading-`9`, zero-fill `0`), insert separators; render sign per the mode (default: reserved sign column); `#`-fill on integer overflow; apply `FM`/`TH`/`B` last.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p pgtypes format_numeric`
Expected: PASS. **Oracle-validate every (value, template) pair** (sign modes `S`/`MI`/`PL`/`SG`/`PR`, `L`/`$`, `V`, `B`, `TH`, overflow `#`) and pin exact strings — this is the highest-risk task for PG-divergence.

- [ ] **Step 5: Mutation sweep + commit**

```bash
git add crates/pgtypes/src/numeric.rs
git commit -m "SP38: numeric to_char engine (format_numeric)"
```

---

## Task 6: `executor::format_fn` — dispatch module + `eval.rs` wiring

**Files:**
- Create: `crates/executor/src/format_fn.rs`
- Modify: `crates/executor/src/lib.rs` (add `mod format_fn;` near the other `mod` lines)
- Modify: `crates/executor/src/eval.rs` (~line 60 in `eval`, ~line 483 in `infer_type`)
- Test: `crates/executor/src/format_fn.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing tests**

In `crates/executor/src/format_fn.rs`, mirror the test harness from `func.rs`/`datetime_fn.rs`:

```rust
#[cfg(test)]
mod tests {
    use crate::clock::EvalCtx;
    use crate::scope::Scope;
    use pgtypes::{ColumnType, Datum};

    fn ev(sql: &str) -> Datum {
        let ctx = EvalCtx::test_default();
        crate::eval::eval(
            &pgparser::parser::parse_expr_for_test(sql).expect("parse"),
            &Scope::empty(), &[], &ctx,
        ).expect("eval")
    }
    fn ty(sql: &str) -> ColumnType {
        crate::eval::infer_type(
            &pgparser::parser::parse_expr_for_test(sql).expect("p"), &Scope::empty(),
        ).expect("ty")
    }
    fn ec(sql: &str) -> String {
        let ctx = EvalCtx::test_default();
        crate::eval::eval(
            &pgparser::parser::parse_expr_for_test(sql).expect("p"),
            &Scope::empty(), &[], &ctx,
        ).expect_err("err").into_pg().code
    }

    #[test]
    fn to_char_dispatch_and_types() {
        assert_eq!(ev("to_char(TIMESTAMP '2024-01-15 13:45:06', 'YYYY-MM-DD')"),
                   Datum::Text("2024-01-15".into()));
        assert_eq!(ev("to_char(485, '999')"), Datum::Text(" 485".into()));
        assert_eq!(ty("to_char(485, '999')"), ColumnType::Text);
        assert_eq!(ty("to_char(now(), 'YYYY')"), ColumnType::Text);
    }

    #[test]
    fn to_timestamp_to_date_make_justify() {
        assert_eq!(ev("to_date('2024-07-04', 'YYYY-MM-DD')"),
                   Datum::Date(jiff::civil::date(2024, 7, 4)));
        assert_eq!(ty("to_timestamp('2024-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"),
                   ColumnType::Timestamptz);
        // to_timestamp(double) — Unix epoch → instant.
        assert_eq!(ev("to_timestamp(0)"),
                   Datum::Timestamptz("1970-01-01T00:00:00Z".parse().expect("ts")));
        assert_eq!(ev("make_date(2024, 7, 4)"), Datum::Date(jiff::civil::date(2024, 7, 4)));
        assert_eq!(ev("make_interval(0, 0, 0, 5)"),
                   Datum::Interval(pgtypes::datetime::Interval { months: 0, days: 5, micros: 0 }));
        assert_eq!(ev("justify_hours(INTERVAL '27 hours')"),
                   Datum::Interval(pgtypes::datetime::Interval { months: 0, days: 1, micros: 3 * 3_600_000_000 }));
    }

    #[test]
    fn error_surface() {
        assert_eq!(ec("to_char(485)"), "42883");           // wrong arity
        assert_eq!(ec("to_date('xx', 'YYYY-MM-DD')"), "22007"); // bad input
        assert_eq!(ec("make_date(2024, 13, 1)"), "22008");  // field overflow
        assert_eq!(ec("make_timestamptz(2024,1,1,0,0,0,'Mars/Olympus')"), "22023"); // bad zone
        assert_eq!(ec("to_char(true, 'YYYY')"), "42883");   // non-formattable type
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo nextest run -p executor format_fn`
Expected: FAIL — module/functions not found (won't compile until Step 3).

- [ ] **Step 3: Implement the module + wire dispatch**

Create `crates/executor/src/format_fn.rs` modeled on `datetime_fn.rs`:

```rust
//! SP38: date/time formatting + constructor functions + numeric to_char.
//! Pure scalar transforms over already-evaluated Datums (+ EvalCtx.time_zone);
//! no lock/write-path/visibility/interleaving → no Stateright model (SP27–SP37
//! carve-out). The value engines live in pgtypes::{datetime,numeric}.

use pgparser::ast::{Expr, FuncArgs, FuncCall};
use pgtypes::{ColumnType, Datum};
use crate::clock::EvalCtx;
use crate::error::ExecError;
use crate::scope::Scope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FmtFunc {
    ToChar, ToTimestamp, ToDate,
    MakeDate, MakeTime, MakeTimestamp, MakeTimestamptz, MakeInterval,
    JustifyDays, JustifyHours, JustifyInterval,
}

fn format_func(name: &str) -> Option<FmtFunc> {
    Some(match name {
        "to_char" => FmtFunc::ToChar,
        "to_timestamp" => FmtFunc::ToTimestamp,
        "to_date" => FmtFunc::ToDate,
        "make_date" => FmtFunc::MakeDate,
        "make_time" => FmtFunc::MakeTime,
        "make_timestamp" => FmtFunc::MakeTimestamp,
        "make_timestamptz" => FmtFunc::MakeTimestamptz,
        "make_interval" => FmtFunc::MakeInterval,
        "justify_days" => FmtFunc::JustifyDays,
        "justify_hours" => FmtFunc::JustifyHours,
        "justify_interval" => FmtFunc::JustifyInterval,
        _ => return None,
    })
}

pub(crate) fn is_format_func(name: &str) -> bool { format_func(name).is_some() }

pub(crate) fn format_func_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> { /* §1.1 table; to_char→Text after validating arg shape; to_timestamp 1-arg→Timestamptz, 2-arg→Timestamptz; to_date→Date; make_*→their types; justify_*→Interval */ }

pub(crate) fn eval_format(
    fc: &FuncCall,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> { /* see notes */ }
```

Implementation notes for `eval_format`:
- Strict: evaluate args; any NULL arg → `Datum::Null` (every PG `to_*`/`make_*`/`justify_*` is strict).
- `ToChar`: match the value Datum:
  - `Date`/`Time`/`Timestamp` → `DateTimeFields::from_civil(...)` with `None` offset (a `Date` promotes via `date_to_midnight`; a `Time` builds a `DateTime` on a dummy date — only time patterns are meaningful), call `datetime::format_datetime`.
  - `Timestamptz(ts)` → `let dt = ctx.time_zone.to_datetime(ts); let off = ctx.time_zone.to_offset(ts).seconds(); DateTimeFields::from_civil(dt, Some(off))`.
  - `Interval(iv)` → `datetime::format_interval(iv, template)`.
  - `Int4/Int8/Float8/Numeric` → promote to `BigDecimal` (`numeric::from_i64` / `from_f64` / clone), call `numeric::format_numeric`.
  - else → `Err(ExecError::UndefinedFunction(...))` (42883).
  Wrap `TypeError` via `ExecError::Type`.
- `ToTimestamp`: 1 arg (numeric) → Unix-epoch seconds → `jiff::Timestamp::from_microsecond((secs*1e6) as i64)` → `Timestamptz`. 2 args (text,text) → `datetime::parse_by_template` → build civil `DateTime` (Feb-30-class invalid → 22008) → interpret in `ctx.time_zone` (`dt.to_zoned(tz).timestamp()`) → `Timestamptz`.
- `ToDate`: `parse_by_template` → build `Date` → `Datum::Date`.
- `MakeDate`/`MakeTime`/`MakeTimestamp`: read `int_arg`/`f64_arg`, call the `pgtypes::datetime` helpers.
- `MakeTimestamptz`: build the civil `DateTime`; resolve the optional zone arg (default `ctx.time_zone`, else `zone_arg`-style lookup, unknown → 22023); `dt.to_zoned(zone).timestamp()` → `Timestamptz`.
- `MakeInterval`: collect 0–7 positional args, defaulting missing trailing args to 0 (ints; the 7th `secs` is `f64`); call `datetime::make_interval`.
- `JustifyDays/Hours/Interval`: expect one `Interval` arg; call the helper.
- Add `require_arity`/`int_arg`/`f64_arg`/`text_arg`/`zone_arg` helpers (copy the small ones from `datetime_fn.rs`/`func.rs`; keep DRY where a shared helper already exists and is reachable).

Wire dispatch in `crates/executor/src/eval.rs`. In `eval` (after the `is_datetime_func` arm near line 66):

```rust
        Expr::Func(fc) if crate::format_fn::is_format_func(&fc.name) => {
            crate::format_fn::eval_format(fc, ctx, |e| eval(e, scope, values, ctx))
        }
```

In `infer_type` (after the `is_datetime_func` arm near line 488):

```rust
        Expr::Func(fc) if crate::format_fn::is_format_func(&fc.name) => {
            crate::format_fn::format_func_result_type(fc, scope)
        }
```

Add `mod format_fn;` to `crates/executor/src/lib.rs`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p executor format_fn`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/format_fn.rs crates/executor/src/lib.rs crates/executor/src/eval.rs
git commit -m "SP38: executor::format_fn — to_char/to_timestamp/to_date/make_*/justify_* dispatch"
```

---

## Task 7: `agg.rs` traversal — confirm format functions compose with aggregates

**Files:**
- Modify (only if a traversal doesn't already recurse generically): `crates/executor/src/agg.rs`
- Test: `crates/executor/src/agg.rs` `tests`

- [ ] **Step 1: Write the failing test**

Add to `agg.rs` tests (adapt to the module's existing test helpers for building a grouped query):

```rust
#[test]
fn format_function_wraps_and_is_wrapped_by_aggregate() {
    // to_char(max(ts), 'YYYY') — a format fn wrapping an aggregate — must validate
    // and evaluate (max is the aggregate; to_char is applied to its result).
    // max(to_char(ts,'YYYY')) — an aggregate wrapping a format fn — likewise.
    // Use the module's existing grouped-eval test harness; assert both produce the
    // expected grouped result rather than a "misplaced aggregate"/42803 error.
}
```

- [ ] **Step 2: Run to verify failure or pass**

Run: `cargo nextest run -p executor agg`
Expected: If the four traversals (`contains_aggregate`, `collect_specs`, `validate_grouped`, `eval_grouped`) already recurse through `Expr::Func` args generically (they do for SP37's datetime funcs), this may PASS immediately — in which case the test is a regression guard. If it FAILS (a traversal special-cases only `is_scalar`/`is_datetime_func`), proceed to Step 3.

- [ ] **Step 3: Implement (if needed)**

In any traversal that branches on `is_scalar(name) || is_datetime_func(name)`, add `|| is_format_func(name)` so a format-function call is treated like any other scalar (recurse into its args; in `eval_grouped` it dispatches via the normal `eval` chain). If the traversals already use a generic `Expr::Func => recurse args` arm, no change is needed.

- [ ] **Step 4: Run to verify pass**

Run: `cargo nextest run -p executor agg`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/agg.rs
git commit -m "SP38: format functions compose with aggregates (agg traversal)"
```

---

## Task 8: wire test `executor::formatting`

**Files:**
- Create: `crates/executor/tests/formatting.rs`
- Test: itself

End-to-end over the wire (text/simple-query mode), mirroring `crates/executor/tests/datetime.rs`'s harness (`spawn`/`connect`/`text`/`col0`/`err_code`).

- [ ] **Step 1: Write the test**

Create `crates/executor/tests/formatting.rs`. Copy the `spawn`/`connect`/`text`/`err_code` helpers verbatim from `tests/datetime.rs` (no `FixedClock` needed — use fixed literals), then:

```rust
//! SP38: to_char / to_timestamp / to_date / make_* / justify_* — end-to-end.

#[tokio::test]
async fn to_char_datetime_and_numeric() {
    let client = connect(spawn().await).await;
    assert_eq!(
        text(&client, "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06', 'YYYY-MM-DD HH24:MI:SS')").await,
        Some("2024-01-15 13:45:06".into())
    );
    assert_eq!(
        text(&client, "SELECT to_char(DATE '2024-07-04', 'FMMonth FMDD, YYYY')").await,
        Some("July 4, 2024".into())
    );
    assert_eq!(text(&client, "SELECT to_char(485, '999')").await, Some(" 485".into()));
    assert_eq!(text(&client, "SELECT to_char(1234567, 'FM9,999,999')").await, Some("1,234,567".into()));
}

#[tokio::test]
async fn to_char_timestamptz_under_set_timezone() {
    let client = connect(spawn().await).await;
    client.batch_execute("SET TIME ZONE 'America/New_York'").await.expect("set tz");
    // 2024-01-15 17:00 UTC = 12:00 EST.
    assert_eq!(
        text(&client, "SELECT to_char(TIMESTAMPTZ '2024-01-15 17:00:00+00', 'YYYY-MM-DD HH24:MI:SS TZH')").await,
        Some("2024-01-15 12:00:00 -05".into())
    );
}

#[tokio::test]
async fn to_timestamp_to_date_make_justify() {
    let client = connect(spawn().await).await;
    assert_eq!(text(&client, "SELECT to_date('2024-07-04', 'YYYY-MM-DD')").await, Some("2024-07-04".into()));
    assert_eq!(text(&client, "SELECT make_date(2024, 2, 29)").await, Some("2024-02-29".into()));
    assert_eq!(text(&client, "SELECT make_interval(1, 2, 0, 3)").await, Some("1 year 2 mons 3 days".into()));
    assert_eq!(text(&client, "SELECT justify_interval(INTERVAL '1 mon -1 hour')").await,
               Some("29 days 23:00:00".into()));
}

#[tokio::test]
async fn result_oids() {
    use tokio_postgres::types::Type;
    let client = connect(spawn().await).await;
    let rows = client.query("SELECT to_char(485,'999'), to_date('2024-01-01','YYYY-MM-DD'), to_timestamp('2024-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')", &[]).await.expect("q");
    assert_eq!(rows[0].columns()[0].type_(), &Type::TEXT);
    assert_eq!(rows[0].columns()[1].type_(), &Type::DATE);
    assert_eq!(rows[0].columns()[2].type_(), &Type::TIMESTAMPTZ);
}

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;
    assert_eq!(err_code(&client, "SELECT to_date('xx', 'YYYY-MM-DD')").await, "22007");
    assert_eq!(err_code(&client, "SELECT make_date(2024, 13, 1)").await, "22008");
    assert_eq!(err_code(&client, "SELECT to_char(485)").await, "42883");
}
```

- [ ] **Step 2: Run to verify (it will exercise the full stack)**

Run: `cargo nextest run -p executor --test formatting`
Expected: PASS. Oracle-pin any `to_char` string that differs from PG.

- [ ] **Step 3: Commit**

```bash
git add crates/executor/tests/formatting.rs
git commit -m "SP38: executor::formatting wire test (end-to-end)"
```

---

## Task 9: conformance corpus + CLAUDE.md + final gauntlet

**Files:**
- Create: `crates/conformance/corpus/to_char_datetime.sql`, `to_char_numeric.sql`, `make_justify.sql`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Write the corpus files**

Model each on `crates/conformance/corpus/datetime.sql` (leading comment block stating scope + exclusions; `SET TIME ZONE 'UTC';` first; English/C-locale names only; dates within jiff's range). Cover:
- `to_char_datetime.sql` — `to_char` over `date`/`time`/`timestamp`/`timestamptz`/`interval` for the in-scope patterns (§1.2), including a long-stable timestamptz zone case.
- `to_char_numeric.sql` — `to_char(numeric/int/float, fmt)` across the digit/group/decimal/sign/currency/`V`/`FM`/`B`/`TH`/overflow patterns.
- `make_justify.sql` — `make_date`/`make_time`/`make_timestamp`/`make_timestamptz`/`make_interval` (positional) + `justify_days`/`justify_hours`/`justify_interval`.
Exclude: the deferred patterns (§1.3), named-arg `make_*`, anything non-deterministic.

- [ ] **Step 2: Validate the corpus locally vs PostgreSQL**

Run the corpus through a real PostgreSQL (≥16) oracle and the crabgresql subject and diff (per the corpus-validation workflow — SQLSTATE + row text, not OIDs). Fix any divergence (almost certainly in `format_numeric`/`format_datetime` padding/sign rules) until 100% clean. Report the local pass rate in the commit message.

- [ ] **Step 3: Update CLAUDE.md**

Add the SP38 slice summary paragraph (mirror the SP37 entry's structure: scope, the new `executor::formatting` test binary as UAC-safe, the new `pgtypes` engines, no new test target with a forbidden substring, "no Stateright model" rationale, documented deviations §1.3). Update the `executor` integration-test list to read `{aggregates, casts, concurrency, datetime, durability, end_to_end, floating_point, formatting, linearizable_reads, mutation_semantics, numeric, predicates, recovery, scalar_functions, transactions}`.

- [ ] **Step 4: Run the full gauntlet**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'   # must print nothing
```
Expected: fmt clean; clippy no warnings; all nextest tests pass; doctests pass; the guard prints nothing.

- [ ] **Step 5: Commit**

```bash
git add crates/conformance/corpus/ CLAUDE.md
git commit -m "SP38: conformance corpus (to_char/make/justify) + CLAUDE.md slice summary"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** to_char datetime (T1) + interval (T2); to_timestamp/to_date parse (T3); make_* + justify_* (T4); numeric to_char (T5); dispatch + types + errors (T6); aggregate composition (T7); wire e2e (T8); corpus + CLAUDE.md + gauntlet (T9). Every §1.1 function and §1.2 pattern family maps to a task; §1.3 deferrals are excluded from the corpus (T9) and noted in CLAUDE.md. No new parser/storage/Stateright work (spec §4/§7/§8) — correctly absent.
- **No new error variants** — every error path uses an existing `TypeError`/`ExecError` (verified against `error.rs` in both crates).
- **Type consistency:** `DateTimeFields`/`ParsedDateTime`/`format_datetime`/`format_interval`/`parse_by_template`/`make_*`/`justify_*`/`format_numeric`/`FmtFunc`/`is_format_func`/`format_func_result_type`/`eval_format` are referenced with consistent signatures across T1–T8.
- **Oracle caveat is explicit** — the highest-divergence-risk values (numeric `to_char`, datetime padding) are flagged for oracle-pinning in T1/T5/T8 and enforced by the T9 corpus diff.
