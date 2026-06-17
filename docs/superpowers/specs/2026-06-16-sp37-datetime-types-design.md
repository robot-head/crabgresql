# SP37 — Date/time types + core semantics (breadth wave 7, part 1 of 2)

- **Date:** 2026-06-16
- **Status:** Design — approved for spec review
- **Slice:** SP37 (this doc). SP38 (formatting + constructors) is a separate, later spec.
- **Guiding principle:** crabgresql is greenfield — match PostgreSQL's observable
  behavior as *identically as possible* (see the "Greenfield" section of `CLAUDE.md`).
  Every quirk PostgreSQL has is the spec; deviations are allowed only as explicitly
  documented, scoped non-goals.

## 1. Goal

Add PostgreSQL's core date/time **type system** and its **semantics** — five new runtime
types, their literals, input/output encodings, comparison/ordering, arithmetic, the full
cast matrix, the session-`timezone` machinery (`SET`/`SHOW`/`RESET`, transactional, as PG
does it), the `AT TIME ZONE` operator, the transaction-stable clock functions, and the
`extract`/`date_part`/`date_trunc`/`age` functions — so that date/time columns and
expressions behave like PostgreSQL.

### 1.1 In scope (SP37)

Five new types (the rarely-used `timetz`/`time with time zone` is excluded):

| Type | OID | Internal rep (jiff) | Binary wire (PG epoch 2000-01-01) |
|---|---|---|---|
| `date` | 1082 | `civil::Date` | `i32` days |
| `time` (without time zone) | 1083 | `civil::Time` | `i64` µs since midnight |
| `timestamp` (without time zone) | 1114 | `civil::DateTime` | `i64` µs since epoch |
| `timestamptz` (`timestamp with time zone`) | 1184 | `Timestamp` (UTC instant) | `i64` µs since epoch (UTC) |
| `interval` | 1186 | `Interval { months: i32, days: i32, micros: i64 }` | `i64` µs, `i32` days, `i32` months |

- **Literals:** `DATE '…'`, `TIME '…'`, `TIMESTAMP '…'`, `TIMESTAMPTZ '…'`, `INTERVAL '…'`.
- **Input parsing:** ISO-8601-ish (precise grammar in §6).
- **Output:** PostgreSQL `*_out` text (ISO `DateStyle`, `postgres` `IntervalStyle`) +
  binary `*_send`.
- **Comparison / ordering** for all five, plus grouping `Eq`/`Hash` (§5).
- **Arithmetic** (§8): the full PG operator matrix among date/time/interval/int.
- **Cast matrix** (§9): `text ↔` each, plus the cross-type temporal casts.
- **`EvalCtx { now, time_zone }`** threaded through `eval` + the wire-encode path (§7).
- **`SET timezone` / `SET TIME ZONE` / `SHOW timezone` / `RESET timezone`** — a faithful,
  *transactional* GUC (§11).
- **`AT TIME ZONE`** operator (§10).
- **Clock functions** (§12): `now()`, `current_timestamp`, `transaction_timestamp()`,
  `statement_timestamp()`, `clock_timestamp()`, `current_date`, `current_time`,
  `localtimestamp`, `localtime` — backed by an injectable clock.
- **Functions** (§13): `extract()` / `date_part()`, `date_trunc()`, `age()`.
- **Conformance corpus** (§15): `datetime.sql` + `interval.sql`, pinned to UTC and
  long-stable zones.

### 1.2 Deferred to SP38 (separate spec)

- `to_char()` / `to_timestamp()` / `to_date()` — the format-template mini-language.
- `make_date` / `make_time` / `make_timestamp` / `make_timestamptz` / `make_interval`.
- `justify_days` / `justify_hours` / `justify_interval`.

### 1.3 Non-goals (documented deviations)

- **`timetz` / `time with time zone`** — excluded entirely; the type name errors as
  unsupported (PG `42704`-style, matching the existing unknown-type path).
- **Fractional-second precision typmod** (`timestamp(p)`, `time(p)`, `interval(p)`) — SP37
  always uses microsecond precision, which *is* PostgreSQL's default for an unqualified
  `timestamp`/`time`/`interval`. The catalog encoding (§14) reserves an optional precision
  payload so adding typmod later needs no format bump. Parsing `timestamp(p)` errors for
  now.
- **`interval` field qualifiers** (`INTERVAL '…' DAY TO SECOND`, `INTERVAL(3)`) — the
  string form only.
- **`SET LOCAL` interaction with `SAVEPOINT`** — crabgresql has no savepoints; GUC
  transactionality is modeled at the `BEGIN`/`COMMIT`/`ROLLBACK` boundary (§11). Savepoint-
  level GUC stacking follows whenever savepoints land.
- **Non-default `DateStyle` / `IntervalStyle`** — output is fixed to PG's defaults
  (`ISO, MDY` / `postgres`); `SET datestyle`/`SET intervalstyle` to a non-default value is
  not supported.
- **Calendar range** — jiff's civil range is years −9999..=9999; PostgreSQL's is wider
  (to 294276 AD / 4713 BC). Out-of-range inputs error rather than store. The corpus stays
  within jiff's range.
- **Broad PG input formats** — only the §6 grammar is accepted; PostgreSQL accepts many
  more spellings. Inputs we accept produce identical results to PG (so the corpus only
  exercises accepted spellings); unaccepted spellings error.

## 2. Dependency: `jiff`

`jiff = { version = "0.2", features = ["tzdb-bundle-always"] }` — pure Rust (no `cc`,
keeping the shipped tree pure as `bigdecimal` did), and the bundled IANA tzdb is compiled
in so DST/zone rules are **identical on Windows-dev and Linux-CI** (no reliance on a system
zoneinfo). Chosen over `chrono`/`chrono-tz` because jiff's `Span` models months/days/time
as separate units — exactly PostgreSQL's `interval` model (`1 month ≠ 30 days`) — and its
civil-vs-`Timestamp` split maps cleanly onto `timestamp` vs `timestamptz`.

**Honest caveat:** our bundled tzdb version may differ from the CI PostgreSQL's tzdb, so a
DST-edge or historical date in some zone could diverge. Mitigation: the corpus pins to
`UTC` plus a couple of long-stable zones/dates (e.g. post-2007 `America/New_York` DST
rules). Documented in §15.

## 3. Type layer (`crates/pgtypes`)

### 3.1 `ColumnType` / `Datum`

Add to `ColumnType` (datum.rs): `Date`, `Time`, `Timestamp`, `Timestamptz`, `Interval`.
Add to `Datum`: `Date(civil::Date)`, `Time(civil::Time)`, `Timestamp(civil::DateTime)`,
`Timestamptz(jiff::Timestamp)`, `Interval(datetime::Interval)`.

Wire up the existing `ColumnType` methods:
- `from_sql_name` — `date`; `time` / `time without time zone`; `timestamp` /
  `timestamp without time zone`; `timestamptz` / `timestamp with time zone`; `interval`.
  (Multi-word names are normalized by the parser before reaching here, mirroring the SP30
  `double precision` precedent.) `timetz` / `time with time zone` → `None` (unsupported).
- `oid` / `name` / `type_size` — per the §1.1 table (`type_size`: `date`=4, `time`=8,
  `timestamp`=8, `timestamptz`=8, `interval`=16).
- `Datum::column_type` — the obvious mapping.

### 3.2 New module `pgtypes::datetime`

The single source of truth for date/time *values* (analogous to `pgtypes::numeric`):

- `struct Interval { months: i32, days: i32, micros: i64 }` (+ helpers:
  `canonical_micros()` for grouping/ordering — see §5; `checked_add`/`checked_sub`,
  `checked_mul_f64`/`checked_div_f64`, `neg`).
- **Parsing:** `parse_date`, `parse_time`, `parse_timestamp`, `parse_timestamptz`
  (needs the session `TimeZone` for offset-less input), `parse_interval`. Each returns
  `Result<_, TypeError>` (bad syntax → `22007`, field out of range → `22008`, see §16).
- **Output:** `date_to_text`, `time_to_text`, `timestamp_to_text`,
  `timestamptz_to_text` (needs the session `TimeZone`), `interval_to_text`. Byte-for-byte
  PG `*_out` (§6.2).
- **Binary:** `*_to_binary` / `*_from_binary` in PG `*_send`/`*_recv` form (PG epoch).
- **Epoch helpers:** conversions to/from PG's 2000-01-01 µs/day counts for the binary
  encodings.

## 4. Internal representation & epoch

- All internal arithmetic uses jiff's types; the **PG epoch (2000-01-01)** appears only at
  the binary-wire boundary. Sub-second resolution is **microseconds** (jiff is nanosecond;
  we truncate-toward-zero to µs on input, matching PG's microsecond storage).
- `timestamptz` stores an absolute instant (jiff `Timestamp`, UTC). The session
  `TimeZone` is applied **only** at (a) offset-less input parsing and (b) text output.
  Comparison and arithmetic operate on the instant and are timezone-independent.

## 5. Equality / ordering / grouping (`Datum` `PartialEq`/`Eq`/`Hash`)

Hand-written (as SP30 float and SP32 numeric already are), matching PostgreSQL btree
equality so `GROUP BY` / `DISTINCT` group like PG:

- `date`, `time`, `timestamp` — by value (their natural total order).
- `timestamptz` — by the **absolute instant** (two values at the same instant in different
  zones are equal).
- **`interval` — by PostgreSQL's canonical estimate**, *not* field-wise. PG's
  `interval_cmp` compares `((months·30 + days)·86_400_000_000 + micros)` (a 30-day month,
  24-hour day estimate). So **`INTERVAL '1 month' = INTERVAL '30 days'`** and they must
  group/hash together. `Eq` and `Hash` both use `canonical_micros()` (an `i128` to avoid
  overflow). This is a genuine PG quirk and is captured by dedicated unit tests.

`Ord`-style ordering (for `ORDER BY` / `MIN` / `MAX` / comparison operators) uses the same
keys via `ops::compare` (§8).

## 6. Input & output formats

### 6.1 Accepted input grammar (the §1.3 deviation: only these spellings)

- **date:** `YYYY-MM-DD` (ISO).
- **time:** `HH:MM[:SS[.ffffff]]`.
- **timestamp:** `YYYY-MM-DD[ T]HH:MM[:SS[.ffffff]]`.
- **timestamptz:** a timestamp optionally followed by an offset `Z` / `±HH[:MM[:SS]]`; with
  no offset, interpreted in the session `TimeZone`.
- **interval:** the PG verbose form — a sequence of signed `quantity unit` terms (`unit` ∈
  `year[s]`, `month[s]`/`mon[s]`, `week[s]`, `day[s]`, `hour[s]`/`hr[s]`,
  `minute[s]`/`min[s]`, `second[s]`/`sec[s]`, `microsecond[s]`/`millisecond[s]`) and/or a
  `[-]HH:MM[:SS[.ffffff]]` clock term; fractional quantities permitted (PG spills the
  fraction into the next-smaller unit). The SQL year-month form `'1-2'`, `ago`, and the
  ISO-8601 `P…` form are deferred (§1.3).

Out-of-range field (e.g. month 13, `2024-02-30`) → `22008`; malformed syntax → `22007`.

### 6.2 Output (PG `*_out`, default `DateStyle` ISO / `IntervalStyle` postgres)

- **date:** `YYYY-MM-DD`.
- **time:** `HH:MM:SS`, with `.ffffff` appended only if sub-second is nonzero (trailing
  zeros trimmed).
- **timestamp:** `YYYY-MM-DD HH:MM:SS[.ffffff]` (space separator).
- **timestamptz:** rendered in the session `TimeZone`, `YYYY-MM-DD HH:MM:SS[.ffffff]±TZ`
  where the offset prints hours, and minutes/seconds only when nonzero (e.g. `+00`, `-05`,
  `+05:30`).
- **interval:** `postgres` style — `years`/`mons` from `months`, `days`, then the clock
  `[-]HH:MM:SS[.ffffff]`; zero interval prints `00:00:00`; signs per PG (e.g.
  `-1 days +00:00:01` for mixed-sign fields).

Exact byte-level formatting is verified against the oracle.

## 7. Evaluation context (`EvalCtx`) — the one invasive change

`eval` and the wire-encode path are currently pure `Datum`-in/out. Two new dependencies on
*context* (not on a `Datum`) force a threaded context:

```
pub struct EvalCtx {
    pub now: jiff::Timestamp,        // transaction-start instant (injectable; §12)
    pub stmt_now: jiff::Timestamp,   // statement-start instant
    pub time_zone: jiff::tz::TimeZone, // effective session timezone (§11)
    pub clock: Arc<dyn Clock>,       // for clock_timestamp() real-time reads
}
```

- `eval(expr, row, ctx)` gains `ctx`. Existing pure call sites pass a context; the
  executor builds one per statement from the session (effective `TimeZone`) + the
  transaction clock.
- **Wire encoding:** `encode_text`/`encode_binary` need the session `TimeZone` for
  `timestamptz` rendering. To keep the `pgtypes::encoding` functions pure, the executor
  resolves `Datum::Timestamptz` to its session-tz text/binary at the projection boundary
  (where it already holds `EvalCtx`), so the generic DataRow path is unchanged for every
  other type. `encode_text`/`encode_binary` gain a `time_zone` parameter used only by the
  `timestamptz` arm (the other arms ignore it), and the executor is the sole caller that
  supplies it.

Decision: thread `EvalCtx` (small, `Copy`-ish except the `Arc`) rather than globals.

## 8. Arithmetic & comparison (`pgtypes::ops`)

`add`/`sub`/`mul`/`div` and `compare` gain temporal arms. `apply_binary`/`apply_unary` in
the executor route to them unchanged. The full matrix:

| Left | Op | Right | Result |
|---|---|---|---|
| `date` | `+` / `-` | `int4` | `date` (add/sub days) |
| `date` | `-` | `date` | `int4` (days) |
| `date` | `+` | `interval` | `timestamp` |
| `date` | `+` | `time` | `timestamp` |
| `timestamp[tz]` | `+` / `-` | `interval` | `timestamp[tz]` |
| `timestamp[tz]` | `-` | `timestamp[tz]` | `interval` |
| `interval` | `+` / `-` | `interval` | `interval` |
| `interval` | `*` / `/` | `int`/`numeric`/`float8` | `interval` |
| (unary) | `-` | `interval` | `interval` |
| `time` | `+` / `-` | `interval` | `time` (wraps mod 24h, as PG) |

- `date + interval → timestamp` (PG promotes; `date + integer → date`).
- `interval × number` distributes over months/days/micros with PG's
  spill-fraction-down rule; `/` likewise. Overflow → `22008`/`22003` as PG.
- `compare` returns a total order for each type (instant for `timestamptz`,
  canonical-µs for `interval`, §5). Cross-type comparison follows PG's implicit
  promotions (`date` vs `timestamp` promotes the date; `timestamp` vs `timestamptz`
  compares at the session tz, as PG does).
- `infer_type` learns these result-type rules (a `datetime_result_type` helper paralleling
  `numeric_result_type`); mismatched temporal operations are `42883`/`42804` at plan time.

## 9. Cast matrix (`pgtypes::cast`)

`cast_allowed` (plan-time) + `cast` (runtime) extend with:

- **`text ↔` each of the five** (parse / `*_out`), same error split as existing casts
  (`22007`/`22008` parse, `22003` overflow).
- **`date → timestamp`** / **`date → timestamptz`** (midnight; tz applied),
  **`timestamp → date`** / **`timestamp → time`** (truncate), **`timestamp ↔
  timestamptz`** (session tz), **`timestamptz → date`/`→ time`/`→ timestamp`** (session
  tz). 
- **No** numeric/bool ↔ temporal casts (PG has none; → `42846`), and **no** `interval ↔`
  date/time/timestamp (PG has none).

The matrix is enumerated exhaustively in unit tests, including every undefined pair → `42846`.

## 10. `AT TIME ZONE`

Operator syntax `expr AT TIME ZONE zone`, added to the Pratt loop as a postfix-style
operator at a precedence matching PG (binds tighter than comparison, looser than the
multiplicative operators), with `zone` an expression yielding `text` (an IANA name or
offset). Semantics (PG `timezone(zone, x)`):

- `timestamp AT TIME ZONE zone → timestamptz` (interpret the wall-clock time as being in
  `zone`).
- `timestamptz AT TIME ZONE zone → timestamp` (the wall-clock time in `zone`).

An unknown zone → `22023`. Implemented via jiff `tz::TimeZone::get` + conversion.

## 11. `SET` / `SHOW` / `RESET timezone` — transactional GUC

### 11.1 Grammar / AST

New `Statement` variants in `ast.rs`: `Set { local: bool, name: String, value: SetValue }`,
`Show { name: String }`, `Reset { name: String }`, where `SetValue ∈ { Default,
Literal(String) }`. Accept all PG spellings: `SET timezone = 'X'`, `SET timezone TO 'X'`,
`SET TIME ZONE 'X'`, `SET TIME ZONE LOCAL`/`DEFAULT`, `SET LOCAL timezone = 'X'`, `RESET
timezone`, `SHOW timezone`, `SHOW TIME ZONE`. New keywords as needed (`ZONE`, `AT`,
`WITHOUT`, `LOCAL`, `SHOW`, `RESET`, `EXTRACT`, type-name keywords). SP37 wires only the
`timezone`/`TimeZone` GUC (case-insensitive name). `datestyle`/`intervalstyle` set to
their PG-default value (`ISO, MDY` / `postgres`) are accepted as a no-op; any other value
is unsupported (§1.3). An unrecognized GUC name → `42704` (unrecognized configuration
parameter), as PG does.

### 11.2 State machine (the faithful PG behavior)

Per-session `GucState` (lives in the executor `SqlSession`, **not** in `TxnCtx`, since
`committed` outlives a transaction):

```
struct GucState {
    committed: TimeZone,                  // survives txn end; default UTC
    txn_session_override: Option<TimeZone>, // SET (session) inside a txn
    txn_local_override: Option<TimeZone>,   // SET LOCAL inside a txn
}
```

- **Effective** (what `EvalCtx.time_zone` reads) =
  `txn_local_override ?? txn_session_override ?? committed`.
- **`SET timezone = X`** outside a txn (autocommit) → `committed = X` (persists).
- **`SET timezone = X`** inside a txn → `txn_session_override = X`.
- **`SET LOCAL timezone = X`** (always inside a txn; outside, it affects only the implicit
  single-statement txn → effectively a no-op afterward, as PG) → `txn_local_override = X`.
- **`COMMIT`** → if `txn_session_override` is `Some`, `committed = it`; clear both
  overrides.
- **`ROLLBACK`** (and failed-txn rollback) → clear both overrides; `committed` unchanged.
- **`RESET timezone`** / `SET timezone = DEFAULT` → same as `SET timezone = UTC` (the
  startup default), and is itself transactional.
- **`SHOW timezone`** → a single-column (`text`, name `TimeZone`) one-row result with the
  effective value.

This hooks into `begin` (nothing to capture — overrides start `None`), `commit_cmd`
(promote), `rollback_cmd` + the `run_one` failed-txn path (revert). `SET`/`SHOW`/`RESET`
dispatch as new arms in `run_one`.

### 11.3 Why no Stateright model (sharpened)

The GUC transaction stack is a **single-session, single-threaded, in-memory** state machine:
no concurrent actors touch it, it is never written to the KV store, never replicated, never
made visible across sessions, and never participates in 2PC/MVCC/locking. Stateright
explores *concurrent interleavings*; with one actor there is nothing to interleave — the
right tool is **exhaustive sequence unit tests** over the operation alphabet
(`BEGIN`/`SET`/`SET LOCAL`/`COMMIT`/`ROLLBACK`/autocommit), asserting commit-keeps,
rollback-reverts, and local-always-reverts. This is the same "pure-data / single-node, no
new interleaving" carve-out SP27–SP32 invoked, made explicit for the new (but still local
and sequential) transactional state.

## 12. Clock functions

An injectable `trait Clock { fn now(&self) -> jiff::Timestamp; }` on the session (default:
system clock; tests inject a fixed clock). At transaction start the session captures
`txn_now = clock.now()`; each statement captures `stmt_now`. Functions (all reading
`EvalCtx`):

- `now()` = `current_timestamp` = `transaction_timestamp()` → `timestamptz` at `txn_now`
  (stable within a transaction, as PG).
- `statement_timestamp()` → `timestamptz` at `stmt_now`.
- `clock_timestamp()` → `timestamptz` at `clock.now()` (real-time, not stable).
- `current_date` → `date` of `txn_now` in the session tz.
- `current_time` → `time` of `txn_now` in the session tz; `localtime` likewise (`time`).
- `localtimestamp` → `timestamp` (no tz) of `txn_now` in the session tz.

`current_timestamp`/`current_date`/`current_time`/`localtimestamp`/`localtime` parse as
keyword-style niladic functions (no parens), per PG.

**Determinism:** these are **excluded from the conformance corpus** (wall-clock can't be
diffed against the oracle). They are proven by wire/unit tests with an **injected fixed
clock** (transaction-stability asserted by issuing two `now()`s in one txn and checking
equality; statement/clock variants by stepping the injected clock). No `sleep`, per the
project's determinism rule.

## 13. Functions: `extract` / `date_part` / `date_trunc` / `age`

- **`EXTRACT(field FROM source)`** — special parse form; `field` is an identifier. Returns
  **`numeric`** (matching PG 14+; SP32's numeric makes this exact). Fields: `year`,
  `month`, `day`, `hour`, `minute`, `second` (incl. fractional), `millennium`, `century`,
  `decade`, `quarter`, `week` (ISO), `dow`, `isodow`, `doy`, `isoyear`, `epoch`,
  `milliseconds`, `microseconds`, and (for `timestamptz`) `timezone`, `timezone_hour`,
  `timezone_minute`. Unknown field → `22023`.
- **`date_part(text, source)`** — the function form; same fields but returns **`double
  precision`** (PG quirk: `date_part` is float8, `EXTRACT` is numeric — both replicated).
- **`date_trunc(field, source[, tz])`** — fields `microseconds`…`millennium`; 2-arg uses
  the session tz for `timestamptz`; the 3-arg `tz` form is supported. Truncates
  `timestamp`/`timestamptz`/`interval`; `date` promotes to `timestamp` as PG.
- **`age(timestamp, timestamp)`** and **`age(timestamp)`** (vs `current_date` start of
  today) → a symbolic `interval` using PG's month-boundary borrowing algorithm (replicated
  exactly; covered by unit tests + corpus).

These slot into the SP27 `Expr::Func` machinery: `func.rs` (registry, arity/type checks,
combinators), `eval`'s `Expr::Func` arm, `infer_type` (`scalar_result_type`), and `agg`'s
four traversals recurse through their arguments (a date/time function may wrap or be
wrapped by an aggregate, e.g. `max(extract(year from ts))`).

## 14. Storage

- **`kv::rowenc`** (append-only tags after `NUMERIC=6`): `DATE=7` (`i32` BE days),
  `TIME=8` (`i64` BE µs), `TIMESTAMP=9` (`i64` BE µs), `TIMESTAMPTZ=10` (`i64` BE µs UTC),
  `INTERVAL=11` (`i64` µs, `i32` days, `i32` months, BE). `encode_row`/`decode_row` gain
  the arms; round-trip unit tests incl. negative/boundary values.
- **`catalog::serde`** (append-only tags after `NUMERIC=5`): `DATE=6`, `TIME=7`,
  `TIMESTAMP=8`, `TIMESTAMPTZ=9`, `INTERVAL=10`. Each of `time`/`timestamp`/`timestamptz`/
  `interval` writes a **reserved optional-precision payload byte** (`0` = none, for now),
  so the deferred typmod precision (§1.3) needs no future format bump. `write_type`/
  `read_type` gain the arms.

## 15. Wire protocol

- `RowDescription` type OIDs come from `ColumnType::oid` (§3.1) — no direct change beyond
  that.
- `DataRow` text/binary via the executor's projection boundary (§7), which supplies the
  session tz for `timestamptz`.
- Startup params already announce `TimeZone=UTC` and `DateStyle`; with SP37 these become
  *backed by* the GUC state (`SHOW timezone` and the effective rendering both read it).
  `ParameterStatus` on `SET timezone` change (PG reports `TimeZone` as a GUC_REPORT
  parameter) is **in scope** (emit a `ParameterStatus` when the committed `timezone`
  changes, matching PG).

## 16. Error surface (SQLSTATEs)

| Condition | SQLSTATE | `TypeError`/`ExecError` |
|---|---|---|
| malformed datetime literal/text | `22007` invalid_datetime_format | new `TypeError::InvalidDatetimeFormat` |
| field out of range (month 13, day 32) | `22008` datetime_field_overflow | new `TypeError::DatetimeFieldOverflow` |
| timestamp/interval value out of range | `22008` | (same) |
| unknown time zone (`AT TIME ZONE`, `SET`) | `22023` invalid_parameter_value | `ExecError::InvalidParameterValue` |
| unknown `extract`/`date_part`/`date_trunc` field | `22023` | (same) |
| undefined cast among temporals | `42846` cannot_coerce | existing `TypeError::CannotCast` |
| unsupported type name (`timetz`) | `42704` undefined_object | existing unknown-type path |
| interval `× / 0` etc. | `22012`/`22008` | as PG |

## 17. Testing strategy

- **`pgtypes` unit tests:** parsing (each grammar + error codes), output (each `*_out`,
  byte-exact), binary round-trip, the cast matrix (every defined pair + every `42846`),
  arithmetic (the §8 matrix incl. month-boundary, overflow), and the **interval grouping
  equality** quirk (`'1 month' == '30 days'`, hash equal). `pgtypes` is a mutation-baseline
  crate — drive new code to zero surviving mutants.
- **`kv::rowenc` / `catalog::serde`** round-trip tests (value + reserved payload).
- **`pgparser`** lexer/parser + libpg_query-oracle tests: typed literals, `EXTRACT(… FROM
  …)`, `AT TIME ZONE` precedence/associativity, the `SET`/`SHOW`/`RESET` grammar,
  multi-word type names.
- **`executor` unit tests:** `eval`/`infer_type` for every new arm; `func` for each
  function (incl. `extract` numeric vs `date_part` float8); the **GUC transaction stack**
  exhaustive-sequence tests (§11.3); the **injected-clock** transaction-stability tests
  (§12).
- **New wire test `executor::datetime`** (UAC-safe target name — see §18): end-to-end over
  the wire — column round-trip + result OIDs, literals, arithmetic, comparison/order,
  casts, `extract`/`date_trunc`/`age`, `AT TIME ZONE`, `SET`/`SHOW timezone` (incl.
  `ParameterStatus`), `timestamptz` rendering under a `SET timezone`, and the error
  surface. The clock funcs are tested here with an injected fixed clock.
- **Conformance corpus** `datetime.sql` + `interval.sql` (diffed vs PG 18 in CI), pinned to
  `UTC` and long-stable zones/dates (§2). Clock funcs excluded (§12).
- **No Stateright model** — §11.3 (pure-data / single-session carve-out, sharpened).

## 18. Windows UAC-safe target name

The new test binary is `executor::datetime` — contains none of
`setup`/`install`/`update`/`patch`/`upgrad`, so it is UAC-safe. Guard
`git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'` must
stay empty. Update the CLAUDE.md `executor` integration-test list to include `datetime`.

## 19. Implementation phases (for the plan)

1. **Dependency + type skeleton:** add `jiff`; `ColumnType`/`Datum` variants, OIDs, names,
   sizes, `from_sql_name`, `column_type`; `pgtypes::datetime` module skeleton +
   `Interval`. (Compiles; everything else stubbed.)
2. **Value layer:** parse + `*_out` text + binary + epoch helpers; `Eq`/`Hash`/`compare`
   (incl. interval canonical-µs). Unit tests + mutation sweep.
3. **Storage:** `rowenc` + `catalog::serde` tags + round-trip tests.
4. **Parser:** type-name keywords/multi-word, typed literals, `EXTRACT`, `AT TIME ZONE`,
   `SET`/`SHOW`/`RESET` AST + grammar. Parser + oracle tests.
5. **EvalCtx + eval/infer_type:** thread `EvalCtx`; literal/arithmetic/comparison/cast
   arms; `datetime_result_type`. Unit tests.
6. **Casts:** the full temporal cast matrix in `pgtypes::cast` + exhaustive tests.
7. **GUC + clock:** `GucState` transactional machine wired into begin/commit/rollback +
   `run_one`; injectable `Clock`; clock funcs; `SHOW`/`ParameterStatus`. Exhaustive-
   sequence + injected-clock tests.
8. **Functions:** `extract`/`date_part`/`date_trunc`/`age` in `func.rs` + `agg` traversals.
9. **Wire test + conformance corpus + CLAUDE.md slice summary.** `cargo fmt` + clippy +
   nextest + doctests green; guard empty.
