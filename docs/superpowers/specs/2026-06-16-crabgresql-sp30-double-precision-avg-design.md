# SP30 — `double precision` (float8) + `AVG` (SQL breadth wave 4)

**Date:** 2026-06-16
**Status:** Approved (design)

## Problem / motivation

SP27–SP29 were three SQL-breadth waves (aggregates, predicates, scalar functions).
Two of them hit the same wall and deferred the same feature to "a future
`numeric`/float-type slice": **`AVG`**. SP27's design says it plainly — "`avg` is
inherently fractional … crabgresql has no `numeric`/floating type yet … `AVG` is
therefore deferred to the slice that introduces a `numeric`/float type" — and SP29
repeats it. That deferral is the single most concrete, self-documented "next" signal in
the codebase, referenced from two CLAUDE.md audit paragraphs and two specs.

The blocker is the absence of *any* fractional type. crabgresql has exactly four runtime
types: `bool`, `int4`, `int8`, `text`. This slice adds the fifth — PostgreSQL
**`double precision`** (`float8`, OID 701, an IEEE-754 `f64`) — and, on top of it, the
`AVG` aggregate, plus float-aware `SUM`/`MIN`/`MAX` and arithmetic.

This slice adds:

- The **`float8` / `double precision`** column type (and the `float` spelling).
- **Float literals** in the lexer/parser: `3.14`, `1.5`, `.5`, `2.`, `1e10`, `1.5e-3`.
- **Numeric type promotion** in arithmetic and comparison: `int ⊕ float → float`,
  `float ⊕ float → float`; float division; float overflow (`22003`) / divide-by-zero
  (`22012`); IEEE special values (`Infinity`, `-Infinity`, `NaN`) with PostgreSQL's
  ordering (NaN is the largest value and equals itself).
- **Storage + wire** encoding for `float8` (the on-disk tagged row format, the catalog
  schema format, and the text + binary DataRow encodings).
- **`AVG(x)`** → `float8`, and **`SUM`/`MIN`/`MAX`** over `float8`.
- Float-aware **`GROUP BY` / `DISTINCT`** (group/dedup keys may now be floats).

## Scope decisions (and why)

### Why not a Stateright model

CLAUDE.md mandates an exhaustive model for "anything touching 2PC, replication,
recovery, leadership, locking, MVCC visibility, or cross-range consistency," and carves
out "a pure-data or single-node refactor with no concurrency/fault dimension may not
warrant one." This slice is squarely the carve-out, for exactly the reasons SP27–SP29
documented and one more specific to floats:

- A `float8` value is **data**. Adding a `Datum` variant, a literal token, two wire
  encodings, and arithmetic/comparison rules introduces no lock, no write path, no
  visibility rule, and no interleaving. Every operation is a deterministic transform of
  a single value or a single already-MVCC-resolved row.
- `AVG`/`SUM`/`MIN`/`MAX` over `float8` are pure folds over the already-correct visible
  row set inside one `execute_read` on one engine (a whole table lives on one range —
  `RangeMap::range_for_table`), identical in structure to SP27's aggregates.
- The one subtle bit — **float grouping equality** (`Datum`'s `Eq`/`Hash`, reopened
  below) — is still pure data. Its correctness is a *value* property (a reflexive,
  symmetric, transitive relation with a matching hash), proven exhaustively by unit
  tests over the boundary values (`NaN`, `-0.0`, `+0.0`); there is no ordering of events
  to explore, so a `Model` would have an interleaving-free state space and merely
  restate those unit tests.

So SP30 ships **no model**, consistent with SP27/SP28/SP29, and over-invests in
deterministic empirical proof (unit + integration + the differential conformance
oracle).

### `Datum: Eq + Hash` is reopened — deliberately, and kept sound

SP27 derived `Eq + Hash` on `Datum` *because* there was no float ("`Eq`/`Hash` are sound
here because no variant holds a float (no `NaN`)"). A raw `f64` is **not** `Eq`/`Hash`
(`NaN != NaN`; `-0.0` and `+0.0` have different bit patterns but compare equal), so the
derive can no longer stand. We **hand-implement** `PartialEq`/`Eq`/`Hash` with
PostgreSQL's *grouping* semantics for floats — the semantics of the `float8` btree
equality operator that `GROUP BY`/`DISTINCT` use:

- `NaN == NaN` (all NaN bit patterns are one group), and `NaN` is **greater** than every
  non-NaN (used by `ORDER BY`/`MIN`/`MAX`).
- `-0.0 == +0.0` (they group together).
- `Hash` canonicalizes before hashing: every NaN → one canonical bit pattern, `-0.0` →
  `+0.0`, so equal values always hash equally (the `Hash`/`Eq` contract).

For the four non-float variants the hand-written impls are byte-for-byte the derive's
behavior, so nothing else changes. The relation is reflexive (`NaN == NaN` now),
symmetric, and transitive, so `Eq` is sound. This is exactly CLAUDE.md's "never weaken a
property to make it pass" inverted: we *strengthen* `PartialEq` to the total relation
PostgreSQL grouping requires, rather than excluding floats from grouping.

### `AVG` returns `float8` (a documented deviation), and how it is proven

PostgreSQL's `avg(int)`/`avg(bigint)` return **`numeric`**; `avg(double precision)`
returns `double precision`. crabgresql has no `numeric`, so **`AVG` returns `float8` for
every numeric input** — the same class of documented deviation as SP27's
`SUM(int8) → int8` (PG: `numeric`). Consequences and how parity is proven:

- `avg(float8_col)` is **exact** PG parity (PG also returns `float8`) — this is what the
  conformance corpus diffs.
- `avg(int_col)` differs in *text rendering only*: PG prints the `numeric`
  `2.0000000000000000`, crabgresql prints the `float8` `2`. The **value** is correct;
  the deviation is the scale-padding of PG's `numeric`. Proven by `executor::agg` unit
  tests (asserting the `float8` value) — it is deliberately *not* in the conformance
  corpus, because the corpus diffs text and we have no `numeric` to match (same approach
  SP27 took for out-of-range `int8` sums).

### Float8 text rendering (the conformance-sensitive part)

PostgreSQL `float8out` (with the default `extra_float_digits`) prints the **shortest
round-tripping** decimal, switching to scientific notation for very large/small
magnitudes, and prints the specials as `Infinity` / `-Infinity` / `NaN`. crabgresql:

- **Specials** are rendered to match PG exactly: `Infinity`, `-Infinity`, `NaN`.
- **Finite** values use Rust's `f64` `Display`, which is *also* the shortest
  round-tripping decimal — so for moderate magnitudes it agrees with PG byte-for-byte
  (`1.5`→`1.5`, `2.0`→`2`, `0.1`→`0.1`, `0.3333333333333333`→`0.3333333333333333`,
  `-0.0`→`-0`). The **only** divergence is the fixed-vs-scientific choice for
  |x| ≥ 1e16 or 0 < |x| < 1e-4, where PG uses `e` notation and Rust stays fixed. This is
  a **documented deviation**; the conformance corpus keeps float magnitudes in the
  agreeing range (mirroring SP29's "the corpus stays ASCII" constraint). Binary format
  is the IEEE-754 big-endian `f64`, which matches PG exactly with no caveat.

### Decimal literals are `float8`, not `numeric` (documented)

In PostgreSQL a bare decimal literal (`1.5`, `1e3`) has type **`numeric`**. crabgresql
has no `numeric`, so a decimal/exponent literal is typed **`float8`**. For values where
`numeric` and `float8` render identically (`1.5`, `0.1`) this is invisible; for
scale-bearing literals it differs (`SELECT 1.0` → PG `1.0`, crabgresql `1`). When such a
literal is stored into a `float8` column or used in float arithmetic, **PG converts to
`float8` too and the outputs agree** — so the corpus exercises floats through `float8`
columns and float arithmetic, and avoids bare scale-bearing decimal-literal projections.
Documented, consistent with the "no `numeric`" reality.

## Components

- **A. Types (`pgtypes`).** `ColumnType::Float8` (OID 701, typlen 8, name
  `double precision`), `from_sql_name` accepting `float8`/`float`/`double precision`.
  `Datum::Float8(f64)` with hand-written `PartialEq`/`Eq`/`Hash` (float grouping
  semantics). `encoding`: text (specials + shortest finite) and binary (IEEE BE).
  `ops`: `float_literal`, `as_f64`, numeric promotion in `add`/`sub`/`mul`/`div`
  (overflow `22003`, divide-by-zero `22012`) and float ordering in `compare` (NaN
  largest/equal).
- **B. Storage (`kv`, `catalog`).** `rowenc` row-value tag `FLOAT8 = 5` (encode/decode).
  `catalog::serde` schema type tag `FLOAT8 = 4` (tag_of / type_of). Both formats are
  append-only (new tag, no version bump).
- **C. Parser/AST (`pgparser`).** `Token::FloatLit(String)`; the numeric lexer splits
  int vs float (fractional part, leading `.`, `e`/`E` exponent with optional sign).
  `Expr::FloatLiteral(String)`; `prefix()` emits it. `create_table` accepts the two-word
  `double precision`.
- **D. Executor.** `eval`/`infer_type` learn `Expr::FloatLiteral` and float arithmetic
  promotion; `unify_types` promotes int+float → float8; `coerce` adds int→float8,
  float8→float8, and float8→int (assignment cast, `round_ties_even`, range-checked
  `22003`). `agg`: `AggFunc::Avg`; `SUM`/`AVG` accept `float8`; an `Acc::SumF` (f64) and
  `Acc::Avg` (f64 sum + i64 count); `func_result_type` (`avg`→float8,
  `sum(float8)`→float8). `func`: `abs(float8)`→float8.
- **E. Conformance.** `crates/conformance/corpus/floating_point.sql` — float columns,
  literals, arithmetic, comparison/ordering (incl. specials), `avg(float8)`,
  `sum/min/max(float8)`, `GROUP BY`/`DISTINCT` over floats, and the error surface,
  diffed against real PG 18 in CI.

## Testing / traceability

| # | Claim | Proof |
|---|---|---|
| 1 | Lexer splits int vs float literals (fraction, leading `.`, exponent, sign); parser accepts `double precision`. | `pgparser` lexer + parser unit tests. |
| 2 | `float8` value semantics: literal parse (overflow→`22003`), promotion (`int⊕float→float`), float `/0`→`22012`, finite overflow→`22003`, NaN/±Inf compare+order, `-0.0==0.0`. | `pgtypes::ops` unit tests. |
| 3 | `Datum` `Eq`/`Hash` group floats per PG: `NaN`s together, `-0.0`/`+0.0` together, hashes consistent. | `pgtypes::datum` unit tests. |
| 4 | Text encoding: specials match PG; finite is shortest round-trip; binary is IEEE BE. | `pgtypes::encoding` unit tests. |
| 5 | Row + schema (de)serialization round-trip `float8` (incl. `NaN`/`-0.0`). | `kv::rowenc` + `catalog::serde` unit tests. |
| 6 | `AVG`→float8, `SUM`/`MIN`/`MAX`/`abs` over float8; `avg(int)`→float8 value; float-DISTINCT/GROUP BY. | `executor::agg` + `executor::func` unit tests. |
| 7 | End-to-end over the wire: create float8 table, insert, select, arithmetic, aggregates, result types (RowDescription OID 701), error SQLSTATEs. | `executor::floating_point` integration test. |
| 8 | Differential parity against PostgreSQL 18 for the float surface (agreeing magnitude range). | `conformance/corpus/floating_point.sql` (CI diff). |
| 9 | No regression of the existing scan/filter/sort/DML/aggregate/2PC suites. | full `cargo nextest run --workspace` + doctests. |

## Success criteria

1. `double precision` columns, float literals, float arithmetic/comparison, and
   `AVG`/`SUM`/`MIN`/`MAX`/`abs` over floats work end-to-end with PG-faithful semantics
   (within the documented `numeric`-absence deviations). — (A–D)
2. The error surface matches PostgreSQL SQLSTATEs (`22003`/`22012`/`42883`/`42804`). — (#2,#7)
3. The conformance corpus diffs clean against PG 18 for the in-range float surface. — (#8)
4. No regression. — (#9)

## Non-goals (deferred)

- **`real` / `float4`** (f32, OID 700) and **`numeric`/`decimal`** (arbitrary precision)
  — a later type slice; `numeric` is what would make `avg(int)` text-exact and decimal
  literals scale-faithful.
- **`CAST(expr AS type)` / `expr::type`** — not needed to *use* `float8` (float literals
  + `int⊕float` promotion suffice); its own breadth slice.
- **Math functions** beyond `abs` (`sqrt`, `round`, `trunc`, `ceil`, `floor`, `power`,
  trig, …) and **`mod`/`%` over float** (PG has no `float8` `mod`).
- Scientific-notation text output for extreme magnitudes (documented deviation above).
- Cross-range float aggregation (not reachable today — a table lives on one range).
