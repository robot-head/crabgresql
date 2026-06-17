# SP33 — Math & string function breadth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 21 scalar math & string functions (floor/ceil/round/trunc/sign, sqrt/power/exp/ln/log/pi, lpad/rpad/left/right/repeat/reverse/strpos/initcap/ascii/chr) to the executor.

**Architecture:** Purely additive. New value transforms (`floor`/`ceil`/`round`/`trunc`/`sign`) go in `pgtypes::numeric`; a new code-carrying `TypeError::Domain` variant carries the new SQLSTATEs (`2201E`/`2201F`/`54000`); all 21 functions register in `executor::func` via the existing `Expr::Func` node. No parser change, no new dependency, no Stateright model (pure single-row transforms — CLAUDE.md's pure-data carve-out).

**Tech Stack:** Rust 2024, `bigdecimal` 0.4 (`with_scale_round`/`RoundingMode`), `f64` built-in math, cargo-nextest, conformance corpus diffed against PostgreSQL 18.

**Spec:** `docs/superpowers/specs/2026-06-16-crabgresql-sp33-math-string-functions-design.md`

---

## File structure

- `crates/pgtypes/src/error.rs` — add `TypeError::Domain { sqlstate, message }` (Task 1)
- `crates/pgtypes/src/numeric.rs` — add `floor`/`ceil`/`round`/`trunc`/`sign` (Task 2)
- `crates/executor/src/func.rs` — register/dispatch all 21 functions (Tasks 3–5)
- `crates/executor/tests/math_string_functions.rs` — new wire test (Task 6)
- `crates/conformance/corpus/math_string_functions.sql` — new corpus (Task 7)
- `CLAUDE.md` — append the SP33 audit paragraph (Task 7)

---

## Task 1: `TypeError::Domain` variant in pgtypes

**Files:**
- Modify: `crates/pgtypes/src/error.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/pgtypes/src/error.rs` (inside `each_error_maps_to_its_postgres_sqlstate`, before the closing `}`):

```rust
        assert_eq!(
            TypeError::Domain {
                sqlstate: "2201E",
                message: "cannot take logarithm of a negative number",
            }
            .sqlstate(),
            "2201E"
        );
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo nextest run -p pgtypes each_error_maps_to_its_postgres_sqlstate`
Expected: FAIL to compile — `no variant named Domain`.

- [ ] **Step 3: Add the variant and its sqlstate arm**

In `crates/pgtypes/src/error.rs`, add this variant to the `TypeError` enum (after `CannotCast`):

```rust
    /// SP33: a math/string domain error carrying its own PostgreSQL SQLSTATE —
    /// e.g. `ln(0)` (2201E), `sqrt(-1)` (2201F), `chr(0)` (54000). One
    /// code-carrying variant rather than one per domain.
    #[error("{message}")]
    Domain {
        sqlstate: &'static str,
        message: &'static str,
    },
```

And add this arm to `sqlstate()` (after the `CannotCast` arm):

```rust
            TypeError::Domain { sqlstate, .. } => sqlstate,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo nextest run -p pgtypes each_error_maps_to_its_postgres_sqlstate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/pgtypes/src/error.rs
git commit -m "SP33: add code-carrying TypeError::Domain variant (math/string domain errors)"
```

---

## Task 2: numeric rounding primitives in pgtypes

**Files:**
- Modify: `crates/pgtypes/src/numeric.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module at the bottom of `crates/pgtypes/src/numeric.rs`:

```rust
    #[test]
    fn rounding_primitives_match_postgres() {
        let n = |s: &str| parse(s).expect("parse");
        // floor toward −∞, ceil toward +∞ (scale 0)
        assert_eq!(to_text(&floor(&n("2.9"))), "2");
        assert_eq!(to_text(&floor(&n("-2.1"))), "-3");
        assert_eq!(to_text(&ceil(&n("2.1"))), "3");
        assert_eq!(to_text(&ceil(&n("-2.9"))), "-2");
        // round half-away-from-zero; preserves requested scale
        assert_eq!(to_text(&round(&n("2.5"), 0)), "3");
        assert_eq!(to_text(&round(&n("-2.5"), 0)), "-3");
        assert_eq!(to_text(&round(&n("2.567"), 2)), "2.57");
        assert_eq!(to_text(&round(&n("1234"), -2)), "1200");
        // trunc toward zero
        assert_eq!(to_text(&trunc(&n("2.99"), 0)), "2");
        assert_eq!(to_text(&trunc(&n("-2.99"), 0)), "-2");
        assert_eq!(to_text(&trunc(&n("2.567"), 1)), "2.5");
        // sign
        assert_eq!(to_text(&sign(&n("-5.5"))), "-1");
        assert_eq!(to_text(&sign(&n("0"))), "0");
        assert_eq!(to_text(&sign(&n("0.3"))), "1");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo nextest run -p pgtypes rounding_primitives_match_postgres`
Expected: FAIL to compile — `cannot find function floor`.

- [ ] **Step 3: Implement the primitives**

In `crates/pgtypes/src/numeric.rs`, add after the existing `abs` function (around line 215):

```rust
/// `floor(x)` — round toward −∞ (PostgreSQL `numeric_floor`); scale 0.
pub fn floor(bd: &BigDecimal) -> BigDecimal {
    canonical(bd.with_scale_round(0, RoundingMode::Floor))
}

/// `ceil(x)` / `ceiling(x)` — round toward +∞ (PostgreSQL `numeric_ceil`); scale 0.
pub fn ceil(bd: &BigDecimal) -> BigDecimal {
    canonical(bd.with_scale_round(0, RoundingMode::Ceiling))
}

/// `round(x, n)` — round to `n` decimal places, half-away-from-zero (PostgreSQL
/// `numeric_round`). `n` may be negative (round to tens/hundreds/…). The result
/// carries scale `max(n, 0)`.
pub fn round(bd: &BigDecimal, n: i64) -> BigDecimal {
    canonical(bd.with_scale_round(n, RoundingMode::HalfUp))
}

/// `trunc(x, n)` — truncate to `n` decimal places, toward zero (PostgreSQL
/// `numeric_trunc`). `n` may be negative.
pub fn trunc(bd: &BigDecimal, n: i64) -> BigDecimal {
    canonical(bd.with_scale_round(n, RoundingMode::Down))
}

/// `sign(x)` — −1 / 0 / 1 as a numeric (PostgreSQL `numeric_sign`).
pub fn sign(bd: &BigDecimal) -> BigDecimal {
    use std::cmp::Ordering;
    match bd.cmp(&BigDecimal::from(0)) {
        Ordering::Less => BigDecimal::from(-1),
        Ordering::Equal => BigDecimal::from(0),
        Ordering::Greater => BigDecimal::from(1),
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo nextest run -p pgtypes rounding_primitives_match_postgres`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/pgtypes/src/numeric.rs
git commit -m "SP33: add numeric floor/ceil/round/trunc/sign value transforms"
```

---

## Task 3: executor — rounding family (floor/ceil/ceiling/round/trunc/sign)

**Files:**
- Modify: `crates/executor/src/func.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/executor/src/func.rs`:

```rust
    #[test]
    fn rounding_family_preserves_type() {
        let num = |s: &str| Datum::Numeric(pgtypes::numeric::parse(s).expect("n"));
        // int in → int out (unchanged)
        assert_eq!(ev("floor(5)"), Datum::Int4(5));
        assert_eq!(ev("ceil(5)"), Datum::Int4(5));
        assert_eq!(ev("trunc(5)"), Datum::Int4(5));
        assert_eq!(ev("round(5)"), Datum::Int4(5));
        assert_eq!(ev("sign(-7)"), Datum::Int4(-1));
        // numeric in → numeric out
        assert_eq!(ev("floor(2.9)"), num("2"));
        assert_eq!(ev("ceiling(2.1)"), num("3"));
        assert_eq!(ev("round(2.5)"), num("3"));
        assert_eq!(ev("round(2.567, 2)"), num("2.57"));
        assert_eq!(ev("trunc(2.99)"), num("2"));
        assert_eq!(ev("trunc(2.567, 1)"), num("2.5"));
        assert_eq!(ev("sign(-0.3)"), num("-1"));
        // float8 in → float8 out (round half-to-even)
        assert_eq!(ev("floor(2.9::float8)"), Datum::Float8(2.0));
        assert_eq!(ev("round(2.5::float8)"), Datum::Float8(2.0)); // half-to-even
        assert_eq!(ev("round(3.5::float8)"), Datum::Float8(4.0));
        assert_eq!(ev("sign(-3.0::float8)"), Datum::Float8(-1.0));
        // two-arg round/trunc on an int → numeric
        assert_eq!(ev("round(1234, -2)"), num("1200"));
        // strict NULL
        assert_eq!(ev("floor(null)"), Datum::Null);
    }

    #[test]
    fn rounding_family_types_and_errors() {
        let t = table();
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), Some(&t)).expect("ty");
        assert_eq!(ty("floor(n)"), ColumnType::Int4);
        assert_eq!(ty("round(2.5)"), ColumnType::Numeric(None));
        assert_eq!(ty("floor(2.5::float8)"), ColumnType::Float8);
        assert_eq!(ty("round(2.5, 1)"), ColumnType::Numeric(None));
        // two-arg round on a float8 first arg → 42883 (PG has no round(float8,int)).
        assert_eq!(err_code("round(2.5::float8, 1)", Some(&t)), "42883");
        // non-numeric arg → 42883.
        assert_eq!(err_code("floor(s)", Some(&t)), "42883");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo nextest run -p executor rounding_family`
Expected: FAIL to compile — `no variant named Floor`.

- [ ] **Step 3: Implement the rounding family**

In `crates/executor/src/func.rs`, add to the `ScalarFunc` enum (after `Least`):

```rust
    // SP33: rounding family (type-preserving).
    Floor,
    Ceil,
    Round,
    Trunc,
    Sign,
```

Add to `scalar_func` name resolution (before the `_ => return None` arm):

```rust
        "floor" => ScalarFunc::Floor,
        "ceil" | "ceiling" => ScalarFunc::Ceil,
        "round" => ScalarFunc::Round,
        "trunc" => ScalarFunc::Trunc,
        "sign" => ScalarFunc::Sign,
```

Add to `scalar_result_type`'s `match f` (after the `NullIf` arm):

```rust
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, n == 1)?;
            // preserves the input numeric type (int4/int8/float8/numeric).
            require_numeric(&args[0], table)
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, n == 1 || n == 2)?;
            if n == 1 {
                require_numeric(&args[0], table)
            } else {
                // two-arg: numeric (or int promoted to numeric) first arg, int
                // second arg, → numeric. A float8 first arg has no 2-arg form.
                let t0 = require_numeric(&args[0], table)?;
                if t0 == ColumnType::Float8 {
                    return Err(no_matching_function());
                }
                require_int(&args[1], table)?;
                Ok(ColumnType::Numeric(None))
            }
        }
```

Add to `eval_eager`'s `match f` (after the `Mod` arm, before the `_ => unreachable!` arm):

```rust
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, vals.len() == 1)?;
            round_family(f, &vals[0], None)
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let scale = match vals.get(1) {
                None => None,
                Some(s) => Some(int_arg(s)?),
            };
            round_family(f, &vals[0], scale)
        }
```

Add these helper functions to `crates/executor/src/func.rs` (near the other math helpers):

```rust
/// Rounding-family value transform. `scale` is `Some` only for the two-arg
/// `round`/`trunc` form, which always yields numeric (an int first arg is
/// promoted to numeric). The one-arg form preserves the input numeric type.
fn round_family(f: ScalarFunc, v: &Datum, scale: Option<i64>) -> Result<Datum, ExecError> {
    use pgtypes::numeric as num;
    if let Some(n) = scale {
        let bd = match v {
            Datum::Int4(i) => num::from_i64(i64::from(*i)),
            Datum::Int8(i) => num::from_i64(*i),
            Datum::Numeric(d) => d.clone(),
            other => return Err(type_error("function", other)),
        };
        return Ok(Datum::Numeric(match f {
            ScalarFunc::Round => num::round(&bd, n),
            ScalarFunc::Trunc => num::trunc(&bd, n),
            _ => unreachable!("scale is only set for round/trunc"),
        }));
    }
    match v {
        Datum::Int4(_) | Datum::Int8(_) => match f {
            ScalarFunc::Sign => sign_int(v),
            _ => Ok(v.clone()), // floor/ceil/round/trunc of an integer is itself
        },
        Datum::Float8(x) => Ok(Datum::Float8(match f {
            ScalarFunc::Floor => x.floor(),
            ScalarFunc::Ceil => x.ceil(),
            ScalarFunc::Round => x.round_ties_even(), // PG float8 round = half-to-even
            ScalarFunc::Trunc => x.trunc(),
            ScalarFunc::Sign => float_sign(*x),
            _ => unreachable!(),
        })),
        Datum::Numeric(d) => Ok(Datum::Numeric(match f {
            ScalarFunc::Floor => num::floor(d),
            ScalarFunc::Ceil => num::ceil(d),
            ScalarFunc::Round => num::round(d, 0),
            ScalarFunc::Trunc => num::trunc(d, 0),
            ScalarFunc::Sign => num::sign(d),
            _ => unreachable!(),
        })),
        other => Err(type_error("function", other)),
    }
}

/// `sign` of an integer, preserving its width.
fn sign_int(v: &Datum) -> Result<Datum, ExecError> {
    Ok(match v {
        Datum::Int4(n) => Datum::Int4(n.signum()),
        Datum::Int8(n) => Datum::Int8(n.signum()),
        other => return Err(type_error("sign", other)),
    })
}

/// `sign` of a float8: −1 / 0 / 1, with `NaN` → `NaN` (PostgreSQL `dsign`).
fn float_sign(x: f64) -> f64 {
    if x.is_nan() {
        f64::NAN
    } else if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo nextest run -p executor rounding_family`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/func.rs
git commit -m "SP33: executor rounding family (floor/ceil/ceiling/round/trunc/sign)"
```

---

## Task 4: executor — transcendental family (sqrt/power/pow/exp/ln/log/pi)

**Files:**
- Modify: `crates/executor/src/func.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/executor/src/func.rs`:

```rust
    #[test]
    fn transcendental_family_returns_float8() {
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("sqrt(2.25)"), Datum::Float8(1.5));
        assert_eq!(ev("power(2, 10)"), Datum::Float8(1024.0));
        assert_eq!(ev("pow(2, 0.5::float8)"), Datum::Float8(2.0_f64.sqrt()));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        assert_eq!(ev("ln(1)"), Datum::Float8(0.0));
        assert_eq!(ev("log(1000)"), Datum::Float8(3.0));
        assert_eq!(ev("pi()"), Datum::Float8(std::f64::consts::PI));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
    }

    #[test]
    fn transcendental_domain_errors() {
        let t = table();
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), Some(&t)).expect("ty");
        assert_eq!(ty("sqrt(n)"), ColumnType::Float8);
        assert_eq!(ty("pi()"), ColumnType::Float8);
        // sqrt(negative) → 2201F
        assert_eq!(ec_eval("sqrt(-1)"), "2201F");
        // ln/log of a non-positive number → 2201E
        assert_eq!(ec_eval("ln(0)"), "2201E");
        assert_eq!(ec_eval("ln(-1)"), "2201E");
        assert_eq!(ec_eval("log(0)"), "2201E");
        // zero to a negative power → 2201F
        assert_eq!(ec_eval("power(0, -1)"), "2201F");
        // wrong arity → 42883
        assert_eq!(err_code("pi(1)", Some(&t)), "42883");
        assert_eq!(err_code("power(2)", Some(&t)), "42883");
    }
```

Also add this test helper to the `tests` module (next to `err_code`):

```rust
    /// SQLSTATE of a runtime eval error (no row context).
    fn ec_eval(sql: &str) -> String {
        crate::eval::eval(&pexpr(sql).expect("parse"), None, &[])
            .expect_err("expected error")
            .into_pg()
            .code
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo nextest run -p executor transcendental`
Expected: FAIL to compile — `no variant named Sqrt`.

- [ ] **Step 3: Implement the transcendental family**

In `crates/executor/src/func.rs`, add to the `ScalarFunc` enum (after `Sign`):

```rust
    // SP33: transcendental family (always float8).
    Sqrt,
    Power,
    Exp,
    Ln,
    Log,
    Pi,
```

Add to `scalar_func` name resolution:

```rust
        "sqrt" => ScalarFunc::Sqrt,
        "power" | "pow" => ScalarFunc::Power,
        "exp" => ScalarFunc::Exp,
        "ln" => ScalarFunc::Ln,
        "log" => ScalarFunc::Log,
        "pi" => ScalarFunc::Pi,
```

Add to `scalar_result_type`'s `match f`:

```rust
        ScalarFunc::Sqrt | ScalarFunc::Exp | ScalarFunc::Ln | ScalarFunc::Log => {
            require_arity(fc, n == 1)?;
            require_numeric(&args[0], table)?;
            Ok(ColumnType::Float8)
        }
        ScalarFunc::Power => {
            require_arity(fc, n == 2)?;
            require_numeric(&args[0], table)?;
            require_numeric(&args[1], table)?;
            Ok(ColumnType::Float8)
        }
        ScalarFunc::Pi => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Float8)
        }
```

Add to `eval_eager`'s `match f`:

```rust
        ScalarFunc::Sqrt => {
            require_arity(fc, vals.len() == 1)?;
            let x = as_f64(&vals[0])?;
            if x < 0.0 {
                return Err(domain("2201F", "cannot take square root of a negative number"));
            }
            Ok(Datum::Float8(x.sqrt()))
        }
        ScalarFunc::Exp => {
            require_arity(fc, vals.len() == 1)?;
            finite_or_overflow(as_f64(&vals[0])?.exp())
        }
        ScalarFunc::Ln => {
            require_arity(fc, vals.len() == 1)?;
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain("2201E", "cannot take logarithm of a non-positive number"));
            }
            Ok(Datum::Float8(x.ln()))
        }
        ScalarFunc::Log => {
            require_arity(fc, vals.len() == 1)?;
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain("2201E", "cannot take logarithm of a non-positive number"));
            }
            Ok(Datum::Float8(x.log10()))
        }
        ScalarFunc::Power => {
            require_arity(fc, vals.len() == 2)?;
            power(as_f64(&vals[0])?, as_f64(&vals[1])?)
        }
        ScalarFunc::Pi => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Float8(std::f64::consts::PI))
        }
```

Add these helper functions to `crates/executor/src/func.rs`:

```rust
/// Coerce any numeric Datum (int4/int8/float8/numeric) to f64 for the
/// transcendental functions, which always compute in float8.
fn as_f64(d: &Datum) -> Result<f64, ExecError> {
    Ok(match d {
        Datum::Int4(n) => f64::from(*n),
        Datum::Int8(n) => *n as f64,
        Datum::Float8(x) => *x,
        Datum::Numeric(d) => pgtypes::numeric::to_f64(d),
        other => return Err(type_error("function", other)),
    })
}

/// Build a domain error carrying its PostgreSQL SQLSTATE.
fn domain(sqlstate: &'static str, message: &'static str) -> ExecError {
    ExecError::Type(pgtypes::TypeError::Domain { sqlstate, message })
}

/// Wrap an f64 result, mapping an overflow-to-infinity to 22003 (matching the
/// engine's float8 arithmetic, which treats a finite→∞ overflow as out-of-range).
fn finite_or_overflow(x: f64) -> Result<Datum, ExecError> {
    if x.is_infinite() {
        Err(ExecError::Type(pgtypes::TypeError::Overflow))
    } else {
        Ok(Datum::Float8(x))
    }
}

/// `power(base, exp)` with PostgreSQL's domain checks (2201F).
fn power(base: f64, exp: f64) -> Result<Datum, ExecError> {
    if base == 0.0 && exp < 0.0 {
        return Err(domain("2201F", "zero raised to a negative power is undefined"));
    }
    if base < 0.0 && exp.fract() != 0.0 {
        return Err(domain(
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    finite_or_overflow(base.powf(exp))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo nextest run -p executor transcendental`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/func.rs
git commit -m "SP33: executor transcendental family (sqrt/power/pow/exp/ln/log/pi)"
```

---

## Task 5: executor — string family (lpad/rpad/left/right/repeat/reverse/strpos/initcap/ascii/chr)

**Files:**
- Modify: `crates/executor/src/func.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/executor/src/func.rs`:

```rust
    #[test]
    fn string_family_values() {
        assert_eq!(ev("lpad('hi', 5)"), Datum::Text("   hi".into()));
        assert_eq!(ev("lpad('hi', 5, '*')"), Datum::Text("***hi".into()));
        assert_eq!(ev("lpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("rpad('hi', 5, 'ab')"), Datum::Text("hiaba".into()));
        assert_eq!(ev("rpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("left('abcdef', 2)"), Datum::Text("ab".into()));
        assert_eq!(ev("left('abcdef', -2)"), Datum::Text("abcd".into()));
        assert_eq!(ev("right('abcdef', 2)"), Datum::Text("ef".into()));
        assert_eq!(ev("right('abcdef', -2)"), Datum::Text("cdef".into()));
        assert_eq!(ev("repeat('ab', 3)"), Datum::Text("ababab".into()));
        assert_eq!(ev("repeat('ab', 0)"), Datum::Text("".into()));
        assert_eq!(ev("reverse('abc')"), Datum::Text("cba".into()));
        assert_eq!(ev("initcap('hello WORLD')"), Datum::Text("Hello World".into()));
        assert_eq!(ev("strpos('abcde', 'cd')"), Datum::Int4(3));
        assert_eq!(ev("strpos('abcde', 'xy')"), Datum::Int4(0));
        assert_eq!(ev("strpos('abc', '')"), Datum::Int4(1));
        assert_eq!(ev("ascii('A')"), Datum::Int4(65));
        assert_eq!(ev("ascii('')"), Datum::Int4(0));
        assert_eq!(ev("chr(65)"), Datum::Text("A".into()));
        // strict NULL
        assert_eq!(ev("lpad(null, 5)"), Datum::Null);
        assert_eq!(ev("reverse(null)"), Datum::Null);
    }

    #[test]
    fn string_family_types_and_errors() {
        let t = table();
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), Some(&t)).expect("ty");
        assert_eq!(ty("lpad(s, 5)"), ColumnType::Text);
        assert_eq!(ty("strpos(s, 'x')"), ColumnType::Int4);
        assert_eq!(ty("ascii(s)"), ColumnType::Int4);
        assert_eq!(ty("chr(n)"), ColumnType::Text);
        // chr(0) and an out-of-range code point → 54000.
        assert_eq!(ec_eval("chr(0)"), "54000");
        assert_eq!(ec_eval("chr(99999999999)"), "54000");
        // wrong arg type → 42883.
        assert_eq!(err_code("left(n, 2)", Some(&t)), "42883");
        assert_eq!(err_code("ascii(n)", Some(&t)), "42883");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo nextest run -p executor string_family`
Expected: FAIL to compile — `no variant named Lpad`.

- [ ] **Step 3: Implement the string family**

In `crates/executor/src/func.rs`, add to the `ScalarFunc` enum (after `Pi`):

```rust
    // SP33: string family.
    Lpad,
    Rpad,
    Left,
    Right,
    Repeat,
    Reverse,
    Strpos,
    Initcap,
    Ascii,
    Chr,
```

Add to `scalar_func` name resolution:

```rust
        "lpad" => ScalarFunc::Lpad,
        "rpad" => ScalarFunc::Rpad,
        "left" => ScalarFunc::Left,
        "right" => ScalarFunc::Right,
        "repeat" => ScalarFunc::Repeat,
        "reverse" => ScalarFunc::Reverse,
        "strpos" => ScalarFunc::Strpos,
        "initcap" => ScalarFunc::Initcap,
        "ascii" => ScalarFunc::Ascii,
        "chr" => ScalarFunc::Chr,
```

Add to `scalar_result_type`'s `match f`:

```rust
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], table)?;
            require_int(&args[1], table)?;
            if n == 3 {
                require_text(&args[2], table)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Left | ScalarFunc::Right | ScalarFunc::Repeat => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], table)?;
            require_int(&args[1], table)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Reverse | ScalarFunc::Initcap => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], table)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Strpos => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], table)?;
            require_text(&args[1], table)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Ascii => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], table)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Chr => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], table)?;
            Ok(ColumnType::Text)
        }
```

Add to `eval_eager`'s `match f`:

```rust
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            let width = int_arg(&vals[1])?;
            let fill = match vals.get(2) {
                None => " ",
                Some(d) => text_arg(d)?,
            };
            Ok(Datum::Text(pad(f, s, width, fill)))
        }
        ScalarFunc::Left | ScalarFunc::Right => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            Ok(Datum::Text(left_right(f, s, n)))
        }
        ScalarFunc::Repeat => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            repeat_str(s, n)
        }
        ScalarFunc::Reverse => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(text_arg(&vals[0])?.chars().rev().collect()))
        }
        ScalarFunc::Initcap => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(initcap(text_arg(&vals[0])?)))
        }
        ScalarFunc::Strpos => {
            require_arity(fc, vals.len() == 2)?;
            Ok(Datum::Int4(strpos(text_arg(&vals[0])?, text_arg(&vals[1])?)))
        }
        ScalarFunc::Ascii => {
            require_arity(fc, vals.len() == 1)?;
            let code = text_arg(&vals[0])?.chars().next().map_or(0, |c| c as i32);
            Ok(Datum::Int4(code))
        }
        ScalarFunc::Chr => {
            require_arity(fc, vals.len() == 1)?;
            chr(int_arg(&vals[0])?)
        }
```

Add these helper functions and the size guard to `crates/executor/src/func.rs`:

```rust
/// PostgreSQL's 1 GB field-size limit — guards `repeat`/`lpad` against minting an
/// adversarially huge string (mapped to 22003 rather than OOM).
const MAX_FIELD_SIZE: usize = 1 << 30;

/// `lpad`/`rpad`: pad `s` to `width` chars with `fill`; when `s` is longer than
/// `width`, truncate to its first `width` chars (both forms). A `width <= 0`
/// yields the empty string; an empty `fill` that cannot pad leaves `s` unchanged.
fn pad(f: ScalarFunc, s: &str, width: i64, fill: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    if width <= 0 {
        return String::new();
    }
    if len >= width {
        return chars[..width as usize].iter().collect();
    }
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return s.to_string();
    }
    let pad_len = (width - len) as usize;
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    match f {
        ScalarFunc::Lpad => format!("{padding}{s}"),
        _ => format!("{s}{padding}"), // Rpad
    }
}

/// `left`/`right`: first/last `n` chars; a negative `n` drops `|n|` chars from the
/// far end (PostgreSQL semantics).
fn left_right(f: ScalarFunc, s: &str, n: i64) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    let take = if n < 0 { (len + n).max(0) } else { n.min(len) };
    match f {
        ScalarFunc::Left => chars[..take as usize].iter().collect(),
        _ => chars[(len - take) as usize..].iter().collect(), // Right
    }
}

/// `repeat(s, n)`: `n <= 0` → empty; guarded against exceeding the field-size limit.
fn repeat_str(s: &str, n: i64) -> Result<Datum, ExecError> {
    if n <= 0 {
        return Ok(Datum::Text(String::new()));
    }
    let n = n as usize;
    match s.len().checked_mul(n) {
        Some(total) if total <= MAX_FIELD_SIZE => Ok(Datum::Text(s.repeat(n))),
        _ => Err(ExecError::Type(pgtypes::TypeError::Overflow)),
    }
}

/// `initcap(s)`: uppercase the first alphanumeric of each word, lowercase the rest.
fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if prev_alnum {
                out.extend(c.to_lowercase());
            } else {
                out.extend(c.to_uppercase());
            }
            prev_alnum = true;
        } else {
            out.push(c);
            prev_alnum = false;
        }
    }
    out
}

/// `strpos(s, sub)`: 1-based char index of the first `sub` in `s`; `0` if absent;
/// an empty `sub` matches at position `1` (PostgreSQL).
fn strpos(s: &str, sub: &str) -> i32 {
    if sub.is_empty() {
        return 1;
    }
    match s.find(sub) {
        None => 0,
        Some(byte_idx) => (s[..byte_idx].chars().count() + 1) as i32,
    }
}

/// `chr(n)`: the one-character string for Unicode code point `n`. `0` or an
/// out-of-range / surrogate code point is 54000.
fn chr(n: i64) -> Result<Datum, ExecError> {
    if n == 0 {
        return Err(domain("54000", "null character not permitted"));
    }
    match u32::try_from(n).ok().and_then(char::from_u32) {
        Some(c) => Ok(Datum::Text(c.to_string())),
        None => Err(domain("54000", "requested character too large for encoding")),
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo nextest run -p executor string_family`
Expected: PASS (both tests).

- [ ] **Step 5: Run the whole func module + fmt and commit**

```bash
cargo nextest run -p executor --lib
cargo fmt -p executor -p pgtypes
git add crates/executor/src/func.rs
git commit -m "SP33: executor string family (lpad/rpad/left/right/repeat/reverse/strpos/initcap/ascii/chr)"
```

Expected: all executor unit tests PASS.

---

## Task 6: end-to-end wire test

**Files:**
- Create: `crates/executor/tests/math_string_functions.rs`

- [ ] **Step 1: Write the test**

Create `crates/executor/tests/math_string_functions.rs` with the standard harness (copied from `scalar_functions.rs`) plus the SP33 assertions:

```rust
//! SP33: math & string functions — end-to-end over the wire. Rounding family
//! (floor/ceil/round/trunc/sign), transcendental (sqrt/power/exp/ln/log/pi),
//! string (lpad/rpad/left/right/repeat/reverse/strpos/initcap/ascii/chr), the
//! result type OIDs, and the domain-error SQLSTATEs (2201E/2201F/54000).

use std::sync::Arc;

use executor::SqlEngine;
use pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            return row.get(0).map(|s| s.to_string());
        }
    }
    panic!("no row for `{sql}`");
}

async fn err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .expect_err("expected error")
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

#[tokio::test]
async fn math_and_string_functions_over_the_wire() {
    let port = spawn().await;
    let client = connect(port).await;

    // rounding family — type-preserving text output
    assert_eq!(scalar(&client, "SELECT floor(2.9)").await.as_deref(), Some("2"));
    assert_eq!(scalar(&client, "SELECT ceil(2.1)").await.as_deref(), Some("3"));
    assert_eq!(scalar(&client, "SELECT round(2.567, 2)").await.as_deref(), Some("2.57"));
    assert_eq!(scalar(&client, "SELECT trunc(2.99)").await.as_deref(), Some("2"));
    assert_eq!(scalar(&client, "SELECT sign(-5)").await.as_deref(), Some("-1"));
    assert_eq!(scalar(&client, "SELECT round(2.5::float8)").await.as_deref(), Some("2"));

    // transcendental — float8
    assert_eq!(scalar(&client, "SELECT sqrt(4)").await.as_deref(), Some("2"));
    assert_eq!(scalar(&client, "SELECT power(2, 10)").await.as_deref(), Some("1024"));
    assert_eq!(scalar(&client, "SELECT ln(1)").await.as_deref(), Some("0"));
    assert_eq!(scalar(&client, "SELECT log(1000)").await.as_deref(), Some("3"));

    // string
    assert_eq!(scalar(&client, "SELECT lpad('hi', 5, '*')").await.as_deref(), Some("***hi"));
    assert_eq!(scalar(&client, "SELECT rpad('hi', 5, 'ab')").await.as_deref(), Some("hiaba"));
    assert_eq!(scalar(&client, "SELECT left('abcdef', 2)").await.as_deref(), Some("ab"));
    assert_eq!(scalar(&client, "SELECT right('abcdef', -2)").await.as_deref(), Some("cdef"));
    assert_eq!(scalar(&client, "SELECT repeat('ab', 3)").await.as_deref(), Some("ababab"));
    assert_eq!(scalar(&client, "SELECT reverse('abc')").await.as_deref(), Some("cba"));
    assert_eq!(scalar(&client, "SELECT initcap('hello world')").await.as_deref(), Some("Hello World"));
    assert_eq!(scalar(&client, "SELECT strpos('abcde', 'cd')").await.as_deref(), Some("3"));
    assert_eq!(scalar(&client, "SELECT ascii('A')").await.as_deref(), Some("65"));
    assert_eq!(scalar(&client, "SELECT chr(65)").await.as_deref(), Some("A"));

    // domain errors
    assert_eq!(err_code(&client, "SELECT sqrt(-1)").await, "2201F");
    assert_eq!(err_code(&client, "SELECT ln(0)").await, "2201E");
    assert_eq!(err_code(&client, "SELECT power(0, -1)").await, "2201F");
    assert_eq!(err_code(&client, "SELECT chr(0)").await, "54000");
    assert_eq!(err_code(&client, "SELECT round(2.5::float8, 1)").await, "42883");
}

#[tokio::test]
async fn function_result_type_oids() {
    let port = spawn().await;
    let client = connect(port).await;
    // sqrt → float8 (OID 701); floor(numeric) → numeric (1700); ascii → int4 (23).
    let rows = client.query("SELECT sqrt(4), floor(2.5), ascii('A')", &[]).await.expect("q");
    assert_eq!(rows[0].columns()[0].type_().oid(), 701);
    assert_eq!(rows[0].columns()[1].type_().oid(), 1700);
    assert_eq!(rows[0].columns()[2].type_().oid(), 23);
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo nextest run -p executor --test math_string_functions`
Expected: PASS (both tests).

- [ ] **Step 3: Confirm the test-binary name is UAC-safe**

Run: `git ls-files 'crates/*/tests/*.rs' | grep -iE 'setup|install|update|patch|upgrad'`
Expected: empty (the new file `math_string_functions.rs` contains none of the forbidden substrings).

- [ ] **Step 4: Commit**

```bash
git add crates/executor/tests/math_string_functions.rs
git commit -m "SP33: end-to-end wire test for math & string functions"
```

---

## Task 7: conformance corpus + CLAUDE.md audit + final gauntlet

**Files:**
- Create: `crates/conformance/corpus/math_string_functions.sql`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Create the corpus file**

Create `crates/conformance/corpus/math_string_functions.sql`. Every result query is
ORDER BY-stable and ASCII-only so the row diff against PostgreSQL is deterministic.
Transcendental functions are exercised through `float8` columns/casts (where PG also
computes in float8), and the deviations noted in the spec are kept out of the corpus.

```sql
-- SP33: math & string functions, diffed against PostgreSQL 18. Rounding family
-- (floor/ceil/round/trunc/sign), transcendental (sqrt/power/exp/ln/log/pi) via
-- float8, and string utilities. Transcendental functions return float8 here
-- (a documented deviation from PG's numeric-in/numeric-out); the corpus drives
-- them through float8 values so PG also computes in float8.
CREATE TABLE m (id int4, x float8, q numeric);
INSERT INTO m VALUES (1, 2.5, 2.567), (2, -2.5, -1.5), (3, 9.0, 1234.0);

-- rounding family (numeric: half-away-from-zero; preserves type)
SELECT floor(2.9), ceil(2.1), trunc(2.99), round(2.5), sign(-3);
SELECT round(2.567, 2), trunc(2.567, 1), round(1234, -2);
SELECT id, floor(q), round(q, 1) FROM m ORDER BY id;

-- rounding family (float8: round half-to-even)
SELECT round(0.5::float8), round(1.5::float8), round(2.5::float8), round(3.5::float8);
SELECT id, floor(x), ceil(x), trunc(x), sign(x) FROM m ORDER BY id;

-- transcendental (float8)
SELECT sqrt(4.0::float8), power(2.0::float8, 10.0::float8), exp(0.0::float8);
SELECT ln(1.0::float8), log(1000.0::float8);
SELECT sqrt(x) FROM m WHERE x > 0 ORDER BY id;

-- string padding / slicing
SELECT lpad('hi', 5, '*'), rpad('hi', 5, 'ab');
SELECT lpad('hello', 3), rpad('hello', 3);
SELECT left('abcdef', 2), left('abcdef', -2), right('abcdef', 2), right('abcdef', -2);
SELECT repeat('ab', 3), repeat('x', 0);

-- string transforms / search
SELECT reverse('abcdef'), initcap('hello WORLD foo');
SELECT strpos('abcde', 'cd'), strpos('abcde', 'xy'), strpos('abc', '');
SELECT ascii('A'), ascii('z'), chr(65), chr(97);
```

- [ ] **Step 2: Run the conformance harness**

Run: `cargo nextest run -p conformance`
Expected: PASS — the `math_string_functions.sql` corpus diffs clean against the
PostgreSQL oracle. (If the oracle is offline locally, the corpus is diffed in CI;
verify the queries produce the expected output via the wire test from Task 6 and by
manual inspection against the spec's documented values.)

- [ ] **Step 3: Append the SP33 audit paragraph to CLAUDE.md**

Add a new paragraph after the SP32 paragraph in `CLAUDE.md` (the "Windows UAC-safe target names" section), matching the style of the SP27–SP32 entries:

```markdown
**SP33 (2026-06-16):** breadth wave 7 — **math & string functions**: rounding
`floor`/`ceil`/`ceiling`/`round`/`trunc`/`sign` (type-preserving across
int4/int8/float8/numeric, mirroring `abs`; two-arg `round`/`trunc` → numeric, a
float8 first arg with two args is 42883 like PG); transcendental
`sqrt`/`power`/`pow`/`exp`/`ln`/`log`/`pi` (always float8); string
`lpad`/`rpad`/`left`/`right`/`repeat`/`reverse`/`strpos`/`initcap`/`ascii`/`chr`.
One new test binary — `executor::math_string_functions` (end-to-end over the wire:
the three families, result type OIDs, and the 2201E/2201F/54000/42883 error
surface) — UAC-safe (no `setup/install/update/patch/upgrad` substring). No new
`cluster`/`crabgresql` binary; **no new dependency** (`f64` built-in;
`bigdecimal` already supplies `with_scale_round`/`RoundingMode`). Types:
`pgtypes::numeric` gains `floor`/`ceil`/`round(bd,n)`/`trunc(bd,n)`/`sign`; a new
code-carrying `pgtypes::TypeError::Domain { sqlstate, message }` carries the new
SQLSTATEs (2201E invalid-arg-for-log, 2201F invalid-arg-for-power/sqrt, 54000
chr). Parser: NO change — every function is a plain identifier via SP27's
`Expr::Func(FuncCall)` node (`pi()` zero-arg, `power`/`pow` two-arg). Executor:
`executor::func` registers all 21 in `ScalarFunc` with arity/type validation;
the transcendental family computes in `f64` (`as_f64` promotes int/float8/numeric)
and returns float8, the rounding family preserves the input numeric type
(float8 round = half-to-even via `round_ties_even`, numeric round =
half-away-from-zero via `numeric::round`), and the string family operates over
`Vec<char>` (Unicode-aware), with `repeat`/`lpad` guarded by a 1 GB field-size
limit (→ 22003). **NO Stateright model — deliberate and justified (identical to
SP27–SP32):** every function is a pure, deterministic scalar transform over the
already-correct, MVCC-visible, single-range row set inside one
`execute_read`/`eval` on one engine — no lock, write path, visibility rule, or
interleaving (CLAUDE.md's "pure-data / single-node refactor" carve-out); even the
subtle bits (per-type rounding mode, domain errors) are *value* properties with
no event ordering to explore. Proven instead by `pgtypes::{numeric,error}` unit
tests, `executor::func` unit tests (every function, NULL strictness, result
types, the error surface), the `executor::math_string_functions` wire test, and
`conformance/corpus/math_string_functions.sql` (diffed against PG 18 in CI). The
`executor` integration-test list now reads `{aggregates, casts, concurrency,
durability, end_to_end, floating_point, linearizable_reads, math_string_functions,
mutation_semantics, numeric, predicates, recovery, scalar_functions,
transactions}`. **Documented deviations:** transcendental functions return float8
for every numeric input (PG returns numeric for numeric input — magnitude-
equivalent, only the type/scale differs; the corpus drives them through float8);
`round(float8, int)` is 42883 (PG-faithful — no two-arg float8 form); a bare
decimal literal is numeric (SP32), so float8 rounding is reached via a `::float8`
cast / float8 column; function result columns are named `?column?`. **Non-goals
(deferred):** numeric-precision transcendentals, two-arg `log(base, x)`,
`width_bucket`/`gcd`/`lcm`/`factorial`, regex/`to_char`/`split_part`/`translate`,
`real`/`float4`, date/time types. The full guard `git ls-files 'crates/*/tests/*.rs'
| grep -iE 'setup|install|update|patch|upgrad'` returns empty.
```

- [ ] **Step 4: Run the full gauntlet**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --workspace --doc
```

Expected: fmt clean, clippy clean (no warnings), all nextest tests PASS, doctests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/conformance/corpus/math_string_functions.sql CLAUDE.md
git commit -m "SP33: math/string functions conformance corpus + CLAUDE.md audit"
```

---

## Self-review notes

- **Spec coverage:** all 21 functions (Tasks 3–5), the new SQLSTATEs (Task 1), the
  numeric primitives (Task 2), the wire test (Task 6), the corpus + audit (Task 7)
  are each implemented by a task.
- **Type consistency:** `ScalarFunc` variants `Floor`/`Ceil`/`Round`/`Trunc`/`Sign`/
  `Sqrt`/`Power`/`Exp`/`Ln`/`Log`/`Pi`/`Lpad`/`Rpad`/`Left`/`Right`/`Repeat`/
  `Reverse`/`Strpos`/`Initcap`/`Ascii`/`Chr`; helpers `round_family`/`sign_int`/
  `float_sign`/`as_f64`/`domain`/`finite_or_overflow`/`power`/`pad`/`left_right`/
  `repeat_str`/`initcap`/`strpos`/`chr` are defined once and referenced
  consistently. `pgtypes::numeric::{floor,ceil,round,trunc,sign}` and
  `TypeError::Domain { sqlstate, message }` match across tasks.
- **agg.rs untouched:** scalar functions dispatch through `crate::func::is_scalar`
  (wired by SP29); new names are picked up automatically, so the grouped-context
  traversals need no change.
```

