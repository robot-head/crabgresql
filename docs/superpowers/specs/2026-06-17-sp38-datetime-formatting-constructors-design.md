# SP38 — date/time formatting + constructors + numeric `to_char` (breadth wave 7, part 2 of 2)

- **Date:** 2026-06-17
- **Status:** Design — approved for spec review
- **Slice:** SP38 (this doc). Completes the date/time story SP37 began; SP37 explicitly
  deferred this function family ("formatting + constructors") to a separate, later spec.
- **Guiding principle:** crabgresql is greenfield — match PostgreSQL's observable
  behavior as *identically as possible* (see the "Greenfield" section of `CLAUDE.md`).
  Every quirk PostgreSQL has is the spec; deviations are allowed only as explicitly
  documented, scoped non-goals.

## 1. Goal

Add PostgreSQL's date/time **formatting** and **construction** functions, plus the
**numeric `to_char`** formatter:

- **`to_char(value, format)`** — the format-template mini-language, for date/time values
  (`timestamp`/`timestamptz`/`date`/`time`/`interval`) **and** for numbers
  (`numeric`/`int4`/`int8`/`float8`). These are two *distinct* template languages sharing
  one overloaded function name.
- **`to_timestamp(text, format)`** / **`to_date(text, format)`** — the inverse: parse a
  string by a format template. Plus the **`to_timestamp(double precision)`** Unix-epoch
  overload.
- **`make_date`** / **`make_time`** / **`make_timestamp`** / **`make_timestamptz`** /
  **`make_interval`** — field constructors.
- **`justify_days`** / **`justify_hours`** / **`justify_interval`** — interval
  normalization.

### 1.1 In scope (SP38)

| Function | Signature(s) | Result |
|---|---|---|
| `to_char` | `(timestamp, text)` `(timestamptz, text)` `(date, text)` `(time, text)` `(interval, text)` | `text` |
| `to_char` | `(numeric, text)` `(int4, text)` `(int8, text)` `(float8, text)` | `text` |
| `to_timestamp` | `(text, text)` | `timestamptz` |
| `to_timestamp` | `(double precision)` | `timestamptz` (Unix epoch → instant) |
| `to_date` | `(text, text)` | `date` |
| `make_date` | `(int, int, int)` (year, month, day) | `date` |
| `make_time` | `(int, int, double precision)` (hour, min, sec) | `time` |
| `make_timestamp` | `(int, int, int, int, int, double precision)` | `timestamp` |
| `make_timestamptz` | `(int, int, int, int, int, double precision [, text])` | `timestamptz` |
| `make_interval` | 0–7 positional args `(years int, months int, weeks int, days int, hours int, mins int, secs double precision)`, trailing args default to 0 | `interval` |
| `justify_days` | `(interval)` | `interval` |
| `justify_hours` | `(interval)` | `interval` |
| `justify_interval` | `(interval)` | `interval` |

Result types and arities are resolved statically in `format_func_result_type` (§5); a bad
name/arity/argument type is `42883`.

### 1.2 Template coverage

**Date/time `to_char` patterns — in scope:**

- **Year:** `YYYY`, `YYY`, `YY`, `Y`, `Y,YYY` (comma-grouped), `IYYY`/`IYY`/`IY`/`I` (ISO
  week-numbering year), `CC` (century), `AD`/`BC`/`A.D.`/`B.C.` (+ lowercase).
- **Month:** `MM`, `Mon`/`MON`/`mon`, `Month`/`MONTH`/`month` (blank-padded to 9 unless
  `FM`), `RM`/`rm` (uppercase/lowercase Roman month I–XII).
- **Day:** `DD`, `DDD` (day of year), `IDDD` (ISO day of year), `D` (day of week, Sunday=1),
  `ID` (ISO day of week, Monday=1), `Day`/`DAY`/`day` (blank-padded to 9 unless `FM`),
  `Dy`/`DY`/`dy`.
- **Week/quarter:** `W` (week of month), `WW` (week of year), `IW` (ISO week), `Q`.
- **Time:** `HH`/`HH12` (12-hour), `HH24`, `MI`, `SS`, `SSSS`/`SSSSS` (seconds past
  midnight), `MS` (millisecond), `US` (microsecond), `FF1`–`FF6` (fractional seconds).
- **Meridiem:** `AM`/`PM`/`am`/`pm`, `A.M.`/`P.M.`/`a.m.`/`p.m.`.
- **Time zone (only meaningful for `timestamptz`):** `TZ`/`tz` (abbrev or offset), `OF`
  (offset, e.g. `+05:30`), `TZH`/`TZM` (offset hours/minutes).
- **Modifiers:** `FM` (fill mode — suppress padding & leading zeros for the *next*
  pattern), `TH`/`th` (ordinal suffix on the preceding number).
- **Literals:** `"quoted text"` is emitted verbatim (with `\"` and `\\` escapes inside);
  any non-pattern run of characters passes through literally (PG behavior).

**Numeric `to_char` patterns — in scope:**

- `9` (digit, leading zeros → blank), `0` (digit, leading zeros shown).
- `.` / `D` (decimal point), `,` / `G` (group/thousands separator).
- `S` (sign anchored to the number, leading or trailing per its position), `MI`
  (trailing/leading minus, blank if non-negative), `PL` (plus sign), `SG` (plus or minus),
  `PR` (negatives wrapped in `<…>`).
- `L` (locale currency symbol) and `$` (currency-symbol position) — rendered to match PG's
  **C-locale** output exactly, validated against the oracle (the precise glyph PG emits in
  the C locale is whatever the corpus diff confirms; we do not assume it).
- `V` (shift the value left by *n* implied decimal digits — multiply by 10ⁿ where *n* is
  the number of `9`/`0` following `V`).
- `FM` (suppress leading/trailing blank padding), `TH`/`th` (ordinal suffix), `B` (blank
  result if the value is zero).
- **Overflow:** when the integer part does not fit the digit positions, the field is filled
  with `#` (PG behavior).

### 1.3 Non-goals (documented deviations)

- **Date/time deferred patterns:** `J` (Julian day), `SP` (spell-out, e.g. `DDSP`), `FX`
  (global fixed-format on `to_timestamp`/`to_date`), `TM` (translation / locale-translated
  month & day names). All non-C-locale / non-English names are out of scope (the corpus
  pins to the C locale / English names, where PG and crabgresql agree).
- **Numeric deferred patterns:** `RN`/`rn` (Roman numerals), `EEEE` (scientific notation),
  and any true non-C-locale `L`/`D`/`G` (locale currency, decimal, grouping). The C-locale
  defaults (`$`, `.`, `,`) are implemented; `SET lc_*` is unsupported (no GUC for it).
- **Named arguments** (`make_interval(days => 5)`, `make_timestamptz(..., timezone => 'X')`)
  — the parser has no `name => value` syntax; SP38 supports the **positional** call only.
  `make_interval` supports 0–7 positional args with trailing-omitted args defaulting to 0
  (PG's all-defaulted signature, called positionally).
- **`to_timestamp`/`to_date` leniency** — PostgreSQL's input parsing is famously permissive
  (it largely ignores separators and is whitespace-insensitive). SP38 implements
  template-driven parsing for the in-scope patterns with PG-compatible field extraction;
  inputs outside that path error (`22007`) rather than silently mis-parse. Accepted inputs
  produce PG-identical results (so the corpus exercises only accepted spellings).
- **Calendar range** — stays within jiff's civil range (years −9999..=9999), as SP37; the
  corpus stays well within it.
- **`to_char(value)` 1-arg / `to_char(interval, ...)` for the `interval` reference date** —
  the single-argument `to_char(numeric)` default-format form is not a PG function (PG always
  takes a format); not applicable.

## 2. Dependencies

**None new.** `jiff` (SP37) supplies every civil field needed by the date/time engine;
`bigdecimal` (SP32) supplies the numeric value the numeric engine formats. Both template
engines are hand-written string transforms — no new crate.

## 3. Value layer (`crates/pgtypes`)

The pure, value-only logic — the part that is mutation-tested to zero survivors and reused
across slices — lives in `pgtypes`, matching the SP37 split ("only value-pure, reusable
computations live in `pgtypes::datetime`; field math / dispatch lives in the executor").

### 3.1 `pgtypes::datetime` additions

- **`fn format_datetime(template: &str, fields: &DateTimeFields) -> Result<String, TypeError>`**
  — the date/time `to_char` engine. `DateTimeFields` is a small struct the executor fills
  from the source `Datum` (year, month, day, hour, minute, second, microsecond, weekday,
  day-of-year, ISO week/year, and `Option<offset_secs>`/`Option<tz_abbrev>` present only for
  `timestamptz`). The engine tokenizes `template` into pattern tokens + literal runs (longest
  match wins, e.g. `HH24` before `HH`), applies the `FM`/`TH` modifiers, and renders. A
  timezone pattern (`TZ`/`OF`/`TZH`/`TZM`) with no offset present renders empty (PG behavior
  for a plain `timestamp`).
- **`fn format_interval(template: &str, iv: Interval) -> Result<String, TypeError>`** — the
  `to_char(interval, fmt)` form. PG renders an interval through the template using its
  **stored** fields, *not* normalized across day/month boundaries: the clock patterns
  (`HH24`/`MI`/`SS`/`MS`/`US`) read the `micros` component only (hours = `micros / 3_600_000_000`,
  so `INTERVAL '36 hours'` formats `HH24` as `36`, and `INTERVAL '1 day 02:03:04'` formats
  `HH24` as `02` with `DD` = `01`); `DD` reads `days`, `MM`/`YYYY` read `months`. (Mirrors
  PG's `interval2tm`.) Year/era/name patterns that don't apply to an interval render per PG.
  Oracle-validated unit tests.
- **`fn parse_by_template(template: &str, input: &str) -> Result<ParsedDateTime, TypeError>`**
  — drives `to_timestamp`/`to_date`. Tokenizes the template, consumes `input` field-by-field
  (numeric patterns consume up to their width of digits; name patterns match month/day
  names case-insensitively), assembles a `ParsedDateTime { year, month, day, hour, minute,
  second, micros, meridiem, tz_offset }` with PG's defaults for absent fields (year defaults
  to 0001, month/day to 1, time to 00:00:00). Out-of-range fields → `22008`; unparseable
  input → `22007`.
- **`make_date`/`make_time`/`make_timestamp`/`make_interval` value helpers** — pure
  constructors over jiff `Date`/`Time`/`DateTime` and `Interval`. `make_interval` composes
  `years*12 + months` months, `weeks*7 + days` days, and `hours/mins/secs` → micros (a
  fractional `secs` spills micros). Field overflow → `22008`.
- **`justify_days(iv)`** — roll 30-day groups of `days` into `months`. **`justify_hours(iv)`**
  — roll 24-hour groups of `micros` into `days`. **`justify_interval(iv)`** — both, then a
  final sign-normalization pass (PG's `interval_justify_interval`: apply hours-then-days, and
  if a field's sign disagrees with the overall sign, borrow/carry — replicate exactly).

`make_timestamptz` needs the session/explicit zone, so its *zone resolution* + civil→instant
step lives in the executor (§5); `pgtypes::datetime` provides the civil-`DateTime` builder it
calls.

### 3.2 `pgtypes::numeric` additions

- **`fn format_numeric(template: &str, value: &BigDecimal) -> Result<String, TypeError>`** —
  the numeric `to_char` engine, independent of the date/time one. Parses the template into a
  digit-grid (count of integer `9`/`0` positions, fractional positions, group-separator
  positions, the decimal-point position, and the sign/currency/`V`/`FM`/`B`/`PR`/`TH`
  decorations), then:
  1. Applies `V` (multiply by 10ⁿ) if present.
  2. Rounds the value to the fractional-digit count (half-away-from-zero, PG numeric
     rounding — reuse `numeric::round`).
  3. Lays the digits into the grid, blanking leading-zero `9` positions and zero-filling `0`
     positions, inserting group separators among rendered integer digits.
  4. Renders the sign per `S`/`MI`/`PL`/`SG`/`PR` (default: a leading blank for non-negative,
     a leading `-` for negative — PG's default reserves a sign column).
  5. If the integer part overflows the available positions, replaces the field with `#`.
  6. Applies `FM` (strip the reserved blank/sign padding) and `TH`/`th` (ordinal suffix), `B`
     (empty if zero) last.

  Returns `text`. All branches oracle-validated by unit tests against a battery of
  (value, template) pairs.

`to_char(int4/int8/float8, fmt)` reuse this by promoting the argument to `BigDecimal`
(`numeric::from_i64` / `from_f64`) in the executor (§5).

## 4. No parser / lexer changes

Every SP38 function is an ordinary identifier-named call (`to_char`, `to_timestamp`,
`to_date`, `make_date`, `make_time`, `make_timestamp`, `make_timestamptz`, `make_interval`,
`justify_days`, `justify_hours`, `justify_interval`) producing the existing
`Expr::Func(FuncCall)` node (SP27). The format string is an ordinary `text` argument. So —
unlike SP37's `EXTRACT(… FROM …)` and `AT TIME ZONE` — **no new keyword, token, or grammar
rule is added**. (Confirmed: `to_char` etc. lex as plain identifiers; none collide with an
existing keyword.)

## 5. Executor: new module `executor::format_fn`

A new module paralleling `func.rs` (SP29) and `datetime_fn.rs` (SP37):

- **`enum FmtFunc`** — the 11 functions.
- **`fn format_func(name) -> Option<FmtFunc>`** + **`pub(crate) fn is_format_func(name)`**.
- **`pub(crate) fn format_func_result_type(fc, scope) -> Result<ColumnType, ExecError>`** —
  static result-type + arity resolution (the table in §1.1). `to_char` always → `text`
  (validating the first arg is a formattable type — temporal or numeric — and the second is
  `text`); the overload (datetime-engine vs numeric-engine) is *resolved at eval* from the
  runtime Datum, but the result type is `text` regardless, so `infer_type` needs no overload
  resolution. `to_timestamp(text,text)`/`(double)`, `to_date`, the `make_*`, and the
  `justify_*` types follow §1.1.
- **`pub(crate) fn eval_format(fc, ctx, eval_child) -> Result<Datum, ExecError>`** — the
  value evaluator (same `eval_child` closure pattern as `eval_scalar`/`eval_datetime`, so it
  composes with both scalar and grouped contexts). Strict in its arguments (any NULL → NULL),
  matching every PG `to_char`/`to_*`/`make_*`/`justify_*`.
  - `to_char`: evaluate both args; match the value Datum — temporal → fill `DateTimeFields`
    (using `ctx.time_zone` to resolve a `timestamptz` to its wall-clock + offset) and call
    `datetime::format_datetime`/`format_interval`; numeric/int/float → promote to
    `BigDecimal` and call `numeric::format_numeric`. A non-formattable type → `42883`
    (runtime, when the call sat in a non-projected position) / caught at plan time otherwise.
  - `to_timestamp(text,text)`: `datetime::parse_by_template` → assemble a civil `DateTime`,
    interpret in `ctx.time_zone` → `timestamptz` (PG: the parsed wall-clock is in the session
    zone unless the template carried an offset).
  - `to_timestamp(double)`: Unix-epoch seconds → `jiff::Timestamp` → `timestamptz`.
  - `to_date(text,text)`: `parse_by_template` → `date`.
  - `make_*`: convert int args (`int_arg`) + the `double precision` seconds; build via the
    `pgtypes::datetime` helpers; `make_timestamptz`'s optional zone arg resolves like
    `datetime_fn::zone_arg` (an unknown zone → `22023`), defaulting to `ctx.time_zone`.
  - `justify_*`: call the `pgtypes::datetime` value helpers.
- **Dispatch wiring** in `eval.rs`: a new arm
  `Expr::Func(fc) if crate::format_fn::is_format_func(&fc.name) => …` in both `eval` and
  `infer_type`, placed after the `is_scalar` and `is_datetime_func` arms (no name overlaps).
- **`agg.rs` traversals** (`contains_aggregate`, `collect_specs`, `validate_grouped`,
  `eval_grouped`) already recurse through `Expr::Func` arguments generically; `eval_grouped`
  dispatches function evaluation through the same `eval`-arm chain, so a format function may
  wrap or be wrapped by an aggregate (`to_char(max(ts), 'YYYY')`, `max(to_char(...))`). Verify
  + add a unit test.

## 6. Error surface (SQLSTATEs)

| Condition | SQLSTATE | Error |
|---|---|---|
| unparseable `to_timestamp`/`to_date` input | `22007` invalid_datetime_format | `TypeError::InvalidDatetimeFormat` (existing) |
| out-of-range parsed/constructed field (month 13, etc.) | `22008` datetime_field_overflow | `TypeError::DatetimeFieldOverflow` (existing) |
| `make_timestamptz` unknown zone | `22023` invalid_parameter_value | `ExecError::InvalidParameterValue` (existing) |
| bad name / arity / argument type | `42883` undefined_function | `ExecError::UndefinedFunction` (existing) |
| `to_char` on a non-formattable type (runtime, non-projected) | `42804` / `42883` | as the existing func.rs split |

No new error variant. (A malformed *format template* — e.g. an unterminated `"` quote — is,
in PG, treated leniently / as a literal; SP38 follows PG: it does not error on an
unrecognized pattern character, it emits it literally, matching PG.)

## 7. Storage / wire

No storage change (no new type; these functions consume and produce existing types). No wire
change beyond the result `ColumnType::oid` already carried by SP37's types and the existing
`text`/`numeric`/`float8` types. `to_char`'s result is `text` (OID 25); `to_timestamp` →
`timestamptz` (1184); `to_date` → `date` (1082); the `make_*` per their types.

## 8. Why no Stateright model (the pure-data / single-engine carve-out, restated)

Every SP38 function is a **pure, deterministic transform of one already-evaluated `Datum`**
(plus, for the clock-independent ones, the per-statement `EvalCtx.time_zone`), executed
inside one `eval` on one engine — a whole table lives on one range, so no cross-range
scatter is involved. There is **no new lock, write path, MVCC-visibility rule, leadership
interaction, or concurrent interleaving**; a template render / field constructor has an
interleaving-free state space. This is precisely CLAUDE.md's "pure-data / single-node
refactor with no concurrency/fault dimension may not warrant one" carve-out, invoked by
SP27–SP37. The subtle parts (template tokenization, sign/overflow rendering, interval
justification borrowing, PG's rounding) are **value** properties — proven exhaustively by
boundary-value unit tests + a mutation sweep + the oracle-diffed corpus, with no event
ordering to explore.

## 9. Testing strategy

- **`pgtypes::datetime` unit tests:** `format_datetime` for every in-scope pattern + the
  `FM`/`TH` modifiers + quoted literals + `timestamptz` zone patterns (byte-exact, oracle
  values); `format_interval`; `parse_by_template` (each pattern, defaults for absent fields,
  `22007`/`22008`); `make_*` (incl. field overflow); `justify_days`/`justify_hours`/
  `justify_interval` (incl. the sign-normalization quirk). `pgtypes` is a mutation-baseline
  crate — **drive new code to zero surviving mutants** (excluding any genuinely-equivalent
  mutant *with a rationale* in `.cargo/mutants.toml`, per the established discipline).
- **`pgtypes::numeric` unit tests:** `format_numeric` over a battery of (value, template)
  pairs — digit grids (`9` vs `0`), grouping, the decimal point, every sign decoration
  (`S`/`MI`/`PL`/`SG`/`PR`), `L`/`$`, `V` shift, `FM`, `B`, `TH`, and `#`-overflow. Mutation
  sweep to zero survivors.
- **`executor::format_fn` unit tests:** dispatch + arity + result types via `infer_type`; the
  `to_char` temporal-vs-numeric overload at eval; `to_timestamp`/`to_date` round-trips;
  `make_*` (incl. `make_timestamptz` zone + `22023`); `justify_*`; the `42883`/`22007`/
  `22008` error surface; NULL strictness; an aggregate-wrapping case
  (`to_char(max(ts),'YYYY')`).
- **New wire test `executor::formatting`** (UAC-safe name — §10): end-to-end over the wire —
  `to_char` for each datetime type + numeric, result OIDs, `to_timestamp`/`to_date`,
  `make_*`, `justify_*`, and the error surface. `to_char` of a `timestamptz` under a
  `SET timezone` (to confirm the session-zone path). Clock-dependent inputs use fixed
  literals (no wall-clock — deterministic, no `sleep`).
- **Conformance corpus** (diffed vs PG 18 in CI, **validated locally vs a real PG oracle
  before commit** per the corpus-validation memory): `to_char_datetime.sql`,
  `to_char_numeric.sql`, `make_justify.sql`. Pinned to `UTC` + a long-stable zone for any
  `timestamptz` case (SP37 precedent). English/C-locale names only (the deferred `TM`/locale
  set is excluded from the corpus, as documented).
- **No Stateright model** — §8.

## 10. Windows UAC-safe target name

The new test binary is **`executor::formatting`** — contains none of
`setup`/`install`/`update`/`patch`/`upgrad`, so it is UAC-safe. The guard
`git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` must
stay empty. Update the CLAUDE.md `executor` integration-test list to include `formatting`.

## 11. Implementation phases (for the plan)

1. **`pgtypes::datetime` formatting engine:** `DateTimeFields`, `format_datetime`,
   `format_interval`. Unit tests + mutation sweep.
2. **`pgtypes::datetime` parse + constructors:** `parse_by_template`, `make_*` value
   helpers, `justify_*`. Unit tests + mutation sweep.
3. **`pgtypes::numeric` formatting engine:** `format_numeric`. Unit tests + mutation sweep.
4. **`executor::format_fn`:** `FmtFunc`, `is_format_func`, `format_func_result_type`,
   `eval_format`; dispatch arms in `eval.rs` (`eval` + `infer_type`); `agg` traversal check.
   Unit tests.
5. **Wire test `executor::formatting`** + conformance corpus (validated locally vs PG) +
   CLAUDE.md slice summary. `cargo fmt` + clippy + nextest + doctests green; UAC guard empty.
