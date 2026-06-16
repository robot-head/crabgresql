//! SP27: aggregate functions + `GROUP BY` / `HAVING`.
//!
//! A whole table lives on a single range (`RangeMap::range_for_table`), so an
//! aggregate query executes entirely inside one `execute_read` on one engine.
//! This module is therefore a pure, deterministic fold over the already-correct
//! MVCC-visible row set — no cross-range scatter/gather, no new lock, no new
//! visibility rule, no new interleaving (see the SP27 design doc for why this
//! single-range/pure-data feature warrants no Stateright model).
//!
//! Supported: `COUNT(*)`, `COUNT(x)`, `SUM(x)`, `MIN(x)`, `MAX(x)`, their
//! `DISTINCT` forms, multi-key `GROUP BY`, and `HAVING`. `AVG` is deferred until
//! a `numeric`/float type exists.

use std::collections::{HashMap, HashSet};

use catalog::Table;
use pgparser::ast::{Expr, FuncArgs, FuncCall, SelectItem, SelectStmt};
use pgtypes::{ColumnType, Datum, TypeError, ops};
use pgwire::engine::{Cell, QueryResult};

use crate::error::ExecError;

/// The aggregate functions crabgresql supports. SP30 added `Avg` (returns float8,
/// since there is no `numeric`) and float8 support for `Sum`/`Min`/`Max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Classify a (lowercased — the lexer lowercases unquoted idents) function name.
/// `None` means "not a known aggregate" (the caller then tries the scalar-function
/// path / reports an undefined function).
fn aggregate_func(name: &str) -> Option<AggFunc> {
    match name {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        _ => None,
    }
}

/// Does `e` (or any subexpression) call a known aggregate function?
pub(crate) fn contains_aggregate(e: &Expr) -> bool {
    match e {
        Expr::Func(fc) => {
            aggregate_func(&fc.name).is_some()
                || match &fc.args {
                    FuncArgs::Star => false,
                    FuncArgs::Exprs(args) => args.iter().any(contains_aggregate),
                }
        }
        Expr::Unary { expr, .. } => contains_aggregate(expr),
        Expr::Binary { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        // SP28: recurse through predicate + conditional expressions.
        Expr::IsNull { expr, .. } => contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Like { expr, pattern, .. } => contains_aggregate(expr) || contains_aggregate(pattern),
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || whens
                    .iter()
                    .any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
                || else_result.as_deref().is_some_and(contains_aggregate)
        }
        // SP31: a cast over an aggregate is an aggregate (`sum(x)::int8`).
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}

/// A `SELECT` is an *aggregate query* iff it groups, has `HAVING`, or any
/// aggregate call appears in the projection or `ORDER BY`.
pub(crate) fn is_aggregate_query(s: &SelectStmt) -> bool {
    !s.group_by.is_empty()
        || s.having.is_some()
        || s.projection.iter().any(|item| match item {
            SelectItem::Expr { expr, .. } => contains_aggregate(expr),
            SelectItem::Wildcard => false,
        })
        || s.order_by.iter().any(|o| contains_aggregate(&o.expr))
}

/// Error for a function call reached by scalar `eval` (i.e. NOT a resolved
/// aggregate position): a known aggregate there is misplaced/nested (42803);
/// anything else is an undefined function (42883).
pub(crate) fn func_in_scalar_context_error(fc: &FuncCall) -> ExecError {
    if aggregate_func(&fc.name).is_some() {
        ExecError::Grouping(format!(
            "aggregate function \"{}\" is not allowed here \
             (aggregates cannot be nested)",
            fc.name
        ))
    } else {
        undefined_function(&fc.name)
    }
}

/// The result column type of an aggregate call, for RowDescription — also
/// validating name, arity, and argument type (all mapped to 42883).
pub(crate) fn func_result_type(
    fc: &FuncCall,
    table: Option<&Table>,
) -> Result<ColumnType, ExecError> {
    let Some(func) = aggregate_func(&fc.name) else {
        return Err(undefined_function(&fc.name));
    };
    match func {
        AggFunc::Count => {
            count_arity(fc)?;
            Ok(ColumnType::Int8) // count(*) / count(x) -> bigint
        }
        AggFunc::Sum => {
            let arg = single_value_arg(fc)?;
            match crate::eval::infer_type(arg, table)? {
                // sum(int4) -> bigint; sum(int8) -> bigint here (PG: numeric — a
                // documented deviation until a numeric type exists). sum(float8)
                // -> float8 (exact PG parity).
                ColumnType::Int4 | ColumnType::Int8 => Ok(ColumnType::Int8),
                ColumnType::Float8 => Ok(ColumnType::Float8),
                other => Err(undefined_for_arg("sum", other)),
            }
        }
        // SP30: avg always yields float8 (PG: numeric for integer input — a
        // documented deviation; exact parity for float8 input).
        AggFunc::Avg => {
            let arg = single_value_arg(fc)?;
            match crate::eval::infer_type(arg, table)? {
                ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float8 => Ok(ColumnType::Float8),
                other => Err(undefined_for_arg("avg", other)),
            }
        }
        // min/max preserve the argument's type.
        AggFunc::Min | AggFunc::Max => {
            let arg = single_value_arg(fc)?;
            crate::eval::infer_type(arg, table)
        }
    }
}

fn undefined_function(name: &str) -> ExecError {
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

fn undefined_for_arg(name: &str, t: ColumnType) -> ExecError {
    ExecError::UndefinedFunction(format!("function {}({}) does not exist", name, t.name()))
}

/// `count` accepts `*` or exactly one argument.
fn count_arity(fc: &FuncCall) -> Result<(), ExecError> {
    match &fc.args {
        FuncArgs::Star => Ok(()),
        FuncArgs::Exprs(args) if args.len() == 1 => Ok(()),
        _ => Err(undefined_function("count")),
    }
}

/// The single value argument of `sum`/`min`/`max` (and `count(x)`); errors
/// (42883) for the wrong arity or the `*` form.
fn single_value_arg(fc: &FuncCall) -> Result<&Expr, ExecError> {
    match &fc.args {
        FuncArgs::Exprs(args) if args.len() == 1 => Ok(&args[0]),
        _ => Err(undefined_function(&fc.name)),
    }
}

/// A resolved aggregate to compute: the function, its argument (`None` only for
/// `count(*)`), the argument's static type (SP30 — picks the int vs float
/// accumulator for `sum`/`avg`; `None` for `count(*)`), and whether `DISTINCT`.
/// `PartialEq` lets identical aggregates share a single accumulator (deduped at
/// collection time).
#[derive(Debug, Clone, PartialEq)]
struct AggSpec {
    func: AggFunc,
    arg: Option<Expr>,
    arg_type: Option<ColumnType>,
    distinct: bool,
}

/// Build the spec for one aggregate call, validating arity, argument type, and
/// the no-nested-aggregate rule.
fn spec_of(fc: &FuncCall, table: Option<&Table>) -> Result<AggSpec, ExecError> {
    let func = aggregate_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    match func {
        AggFunc::Count => match &fc.args {
            FuncArgs::Star => Ok(AggSpec {
                func,
                arg: None,
                arg_type: None,
                distinct: fc.distinct,
            }),
            FuncArgs::Exprs(args) if args.len() == 1 => {
                reject_nested_aggregate(&args[0])?;
                let arg_type = crate::eval::infer_type(&args[0], table)?;
                Ok(AggSpec {
                    func,
                    arg: Some(args[0].clone()),
                    arg_type: Some(arg_type),
                    distinct: fc.distinct,
                })
            }
            _ => Err(undefined_function("count")),
        },
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
            let arg = single_value_arg(fc)?;
            reject_nested_aggregate(arg)?;
            // Type-check the argument now so RowDescription and folding agree.
            let arg_type = crate::eval::infer_type(arg, table)?;
            // sum/avg accept only numeric arguments (int4/int8/float8).
            if matches!(func, AggFunc::Sum | AggFunc::Avg)
                && !matches!(
                    arg_type,
                    ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float8
                )
            {
                return Err(undefined_for_arg(&fc.name, arg_type));
            }
            Ok(AggSpec {
                func,
                arg: Some(arg.clone()),
                arg_type: Some(arg_type),
                distinct: fc.distinct,
            })
        }
    }
}

fn reject_nested_aggregate(arg: &Expr) -> Result<(), ExecError> {
    if contains_aggregate(arg) {
        return Err(ExecError::Grouping(
            "aggregate function calls cannot be nested".into(),
        ));
    }
    Ok(())
}

/// Collect (deduped) every aggregate spec in `e`. A non-aggregate function call
/// is an undefined function (42883).
fn collect_specs(
    e: &Expr,
    table: Option<&Table>,
    specs: &mut Vec<AggSpec>,
) -> Result<(), ExecError> {
    match e {
        Expr::Func(fc) if aggregate_func(&fc.name).is_some() => {
            let spec = spec_of(fc, table)?;
            if !specs.contains(&spec) {
                specs.push(spec);
            }
        }
        // SP29: a scalar function may wrap aggregates / grouped columns — gather
        // aggregates from its arguments (the call itself is not an aggregate).
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            if let FuncArgs::Exprs(args) = &fc.args {
                for a in args {
                    collect_specs(a, table, specs)?;
                }
            }
        }
        Expr::Func(fc) => return Err(undefined_function(&fc.name)),
        Expr::Unary { expr, .. } => collect_specs(expr, table, specs)?,
        Expr::Binary { left, right, .. } => {
            collect_specs(left, table, specs)?;
            collect_specs(right, table, specs)?;
        }
        // SP28: gather aggregates appearing inside predicate / CASE expressions.
        Expr::IsNull { expr, .. } => collect_specs(expr, table, specs)?,
        Expr::InList { expr, list, .. } => {
            collect_specs(expr, table, specs)?;
            for e in list {
                collect_specs(e, table, specs)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_specs(expr, table, specs)?;
            collect_specs(low, table, specs)?;
            collect_specs(high, table, specs)?;
        }
        Expr::Like { expr, pattern, .. } => {
            collect_specs(expr, table, specs)?;
            collect_specs(pattern, table, specs)?;
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_specs(o, table, specs)?;
            }
            for (c, r) in whens {
                collect_specs(c, table, specs)?;
                collect_specs(r, table, specs)?;
            }
            if let Some(e) = else_result {
                collect_specs(e, table, specs)?;
            }
        }
        // SP31: gather aggregates from a cast's operand (`avg(x)::int8`).
        Expr::Cast { expr, .. } => collect_specs(expr, table, specs)?,
        _ => {}
    }
    Ok(())
}

/// Data-independent validation: every projection / `HAVING` / `ORDER BY`
/// expression must be built from aggregate calls, `GROUP BY` expressions, and
/// constants. A bare ungrouped column → 42803 (even on an empty table).
fn validate_grouped(e: &Expr, group_by: &[Expr]) -> Result<(), ExecError> {
    if let Expr::Func(fc) = e
        && aggregate_func(&fc.name).is_some()
    {
        return Ok(()); // an aggregate may reference any column in its argument
    }
    if group_by.iter().any(|g| g == e) {
        return Ok(()); // matches a grouping expression structurally
    }
    match e {
        Expr::Column(name) => Err(ungrouped_column(name)),
        Expr::Unary { expr, .. } => validate_grouped(expr, group_by),
        Expr::Binary { left, right, .. } => {
            validate_grouped(left, group_by)?;
            validate_grouped(right, group_by)
        }
        // SP29: every argument of a scalar function must itself be grouped-valid.
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => {
            if let FuncArgs::Exprs(args) = &fc.args {
                for a in args {
                    validate_grouped(a, group_by)?;
                }
            }
            Ok(())
        }
        Expr::Func(fc) => Err(undefined_function(&fc.name)),
        // SP28: every child of a predicate / CASE must itself be grouped-valid.
        Expr::IsNull { expr, .. } => validate_grouped(expr, group_by),
        Expr::InList { expr, list, .. } => {
            validate_grouped(expr, group_by)?;
            for e in list {
                validate_grouped(e, group_by)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_grouped(expr, group_by)?;
            validate_grouped(low, group_by)?;
            validate_grouped(high, group_by)
        }
        Expr::Like { expr, pattern, .. } => {
            validate_grouped(expr, group_by)?;
            validate_grouped(pattern, group_by)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => {
            if let Some(o) = operand {
                validate_grouped(o, group_by)?;
            }
            for (c, r) in whens {
                validate_grouped(c, group_by)?;
                validate_grouped(r, group_by)?;
            }
            if let Some(e) = else_result {
                validate_grouped(e, group_by)?;
            }
            Ok(())
        }
        // SP31: a cast is grouped-valid iff its operand is (and an entire cast
        // expression matching a GROUP BY key was already accepted above).
        Expr::Cast { expr, .. } => validate_grouped(expr, group_by),
        _ => Ok(()), // literals / params are constants
    }
}

fn ungrouped_column(name: &str) -> ExecError {
    ExecError::Grouping(format!(
        "column \"{name}\" must appear in the GROUP BY clause or be used in an aggregate function"
    ))
}

/// Evaluate an expression in a group's context: aggregate calls resolve to their
/// finalized per-group result; subexpressions matching a `GROUP BY` expression
/// resolve to the group key; everything else recurses. (Validation already
/// guarantees no ungrouped column reaches the `Column` arm.)
fn eval_grouped(
    e: &Expr,
    table: Option<&Table>,
    group_by: &[Expr],
    key: &[Datum],
    specs: &[AggSpec],
    results: &[Datum],
) -> Result<Datum, ExecError> {
    if let Expr::Func(fc) = e
        && aggregate_func(&fc.name).is_some()
    {
        let spec = spec_of(fc, table)?;
        let i = specs
            .iter()
            .position(|s| *s == spec)
            .ok_or_else(|| ExecError::Grouping("aggregate not resolved".into()))?;
        return Ok(results[i].clone());
    }
    if let Some(i) = group_by.iter().position(|g| g == e) {
        return Ok(key[i].clone());
    }
    match e {
        Expr::IntLiteral(s) => Ok(ops::int_literal(s)?),
        Expr::FloatLiteral(s) => Ok(ops::float_literal(s)?),
        Expr::StringLiteral(s) => Ok(Datum::Text(s.clone())),
        Expr::BoolLiteral(b) => Ok(Datum::Bool(*b)),
        Expr::NullLiteral => Ok(Datum::Null),
        Expr::Param(_) => Err(ExecError::Unsupported(
            "query parameters ($n) are not supported".into(),
        )),
        Expr::Column(name) => Err(ungrouped_column(name)),
        Expr::Unary { op, expr } => {
            let v = eval_grouped(expr, table, group_by, key, specs, results)?;
            crate::eval::apply_unary(*op, &v)
        }
        Expr::Binary { op, left, right } => {
            let l = eval_grouped(left, table, group_by, key, specs, results)?;
            let r = eval_grouped(right, table, group_by, key, specs, results)?;
            crate::eval::apply_binary(*op, &l, &r)
        }
        // SP28: predicate + conditional expressions in a grouped context — same
        // combinators as scalar `eval`, recursing through `eval_grouped`.
        Expr::IsNull { expr, negated } => {
            let v = eval_grouped(expr, table, group_by, key, specs, results)?;
            Ok(Datum::Bool(v.is_null() ^ *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let x = eval_grouped(expr, table, group_by, key, specs, results)?;
            crate::eval::eval_in_list(&x, list, *negated, |e| {
                eval_grouped(e, table, group_by, key, specs, results)
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let x = eval_grouped(expr, table, group_by, key, specs, results)?;
            let lo = eval_grouped(low, table, group_by, key, specs, results)?;
            let hi = eval_grouped(high, table, group_by, key, specs, results)?;
            crate::eval::eval_between(&x, &lo, &hi, *negated)
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let s = eval_grouped(expr, table, group_by, key, specs, results)?;
            let p = eval_grouped(pattern, table, group_by, key, specs, results)?;
            crate::eval::eval_like(&s, &p, *negated, *case_insensitive)
        }
        Expr::Case {
            operand,
            whens,
            else_result,
        } => crate::eval::eval_case(operand.as_deref(), whens, else_result.as_deref(), |e| {
            eval_grouped(e, table, group_by, key, specs, results)
        }),
        // SP29: a scalar function over grouped/aggregate arguments — evaluate it
        // with the grouped evaluator as its child-eval closure.
        Expr::Func(fc) if crate::func::is_scalar(&fc.name) => crate::func::eval_scalar(fc, |e| {
            eval_grouped(e, table, group_by, key, specs, results)
        }),
        Expr::Func(fc) => Err(undefined_function(&fc.name)),
        // SP31: cast in a grouped context — convert the grouped-evaluated operand.
        Expr::Cast { expr, ty } => {
            let v = eval_grouped(expr, table, group_by, key, specs, results)?;
            Ok(pgtypes::cast::cast(&v, *ty)?)
        }
    }
}

/// One group's running accumulator for one aggregate. The optional `seen` set is
/// present iff the spec is `DISTINCT`. SP30 splits `Sum` into an integer (`SumI`,
/// accumulated in a checked i64 so `sum(int4)` never overflows prematurely) and a
/// float (`SumF`, accumulated in f64) variant, and adds `Avg` (float8 result).
enum Acc {
    Count {
        n: i64,
        seen: Option<HashSet<Datum>>,
    },
    SumI {
        acc: Option<i64>,
        seen: Option<HashSet<Datum>>,
    },
    SumF {
        acc: f64,
        any: bool,
        seen: Option<HashSet<Datum>>,
    },
    MinMax {
        best: Option<Datum>,
        seen: Option<HashSet<Datum>>,
    },
    Avg {
        sum: f64,
        n: i64,
        seen: Option<HashSet<Datum>>,
    },
}

impl Acc {
    fn new(spec: &AggSpec) -> Acc {
        let seen = spec.distinct.then(HashSet::new);
        match spec.func {
            AggFunc::Count => Acc::Count { n: 0, seen },
            AggFunc::Sum => {
                if spec.arg_type == Some(ColumnType::Float8) {
                    Acc::SumF {
                        acc: 0.0,
                        any: false,
                        seen,
                    }
                } else {
                    Acc::SumI { acc: None, seen }
                }
            }
            AggFunc::Avg => Acc::Avg {
                sum: 0.0,
                n: 0,
                seen,
            },
            AggFunc::Min | AggFunc::Max => Acc::MinMax { best: None, seen },
        }
    }

    fn seen_mut(&mut self) -> Option<&mut HashSet<Datum>> {
        match self {
            Acc::Count { seen, .. }
            | Acc::SumI { seen, .. }
            | Acc::SumF { seen, .. }
            | Acc::MinMax { seen, .. }
            | Acc::Avg { seen, .. } => seen.as_mut(),
        }
    }

    /// Fold one source row into this accumulator.
    fn fold_row(
        &mut self,
        spec: &AggSpec,
        table: Option<&Table>,
        row: &[Datum],
    ) -> Result<(), ExecError> {
        // count(*) counts every row, ignoring NULL/DISTINCT.
        if let (AggFunc::Count, None) = (spec.func, &spec.arg) {
            if let Acc::Count { n, .. } = self {
                *n += 1;
            }
            return Ok(());
        }
        let arg = spec
            .arg
            .as_ref()
            .expect("non-star aggregate has an argument");
        let v = crate::eval::eval(arg, table, row)?;
        // count(x)/sum/min/max ignore NULL arguments.
        if v.is_null() {
            return Ok(());
        }
        // DISTINCT: fold only the first occurrence of each value.
        if spec.distinct
            && let Some(seen) = self.seen_mut()
            && !seen.insert(v.clone())
        {
            return Ok(());
        }
        match self {
            Acc::Count { n, .. } => *n += 1,
            Acc::SumI { acc, .. } => {
                let add = as_i64(&v).ok_or_else(|| {
                    undefined_for_arg("sum", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                let next = match acc {
                    Some(cur) => cur
                        .checked_add(add)
                        .ok_or(ExecError::Type(TypeError::Overflow))?,
                    None => add,
                };
                *acc = Some(next);
            }
            Acc::SumF { acc, any, .. } => {
                *acc += as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("sum", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                *any = true;
            }
            Acc::Avg { sum, n, .. } => {
                *sum += as_f64(&v).ok_or_else(|| {
                    undefined_for_arg("avg", v.column_type().unwrap_or(ColumnType::Text))
                })?;
                *n += 1;
            }
            Acc::MinMax { best, .. } => {
                let take = match best {
                    None => true,
                    Some(cur) => {
                        let ord = ops::compare(&v, cur)?; // both non-null
                        matches!(
                            (spec.func, ord),
                            (AggFunc::Min, Some(std::cmp::Ordering::Less))
                                | (AggFunc::Max, Some(std::cmp::Ordering::Greater))
                        )
                    }
                };
                if take {
                    *best = Some(v);
                }
            }
        }
        Ok(())
    }

    fn finish(&self) -> Datum {
        match self {
            Acc::Count { n, .. } => Datum::Int8(*n),
            Acc::SumI { acc, .. } => acc.map(Datum::Int8).unwrap_or(Datum::Null),
            // An empty / all-null float sum is NULL (matches the integer sum).
            Acc::SumF { acc, any, .. } => {
                if *any {
                    Datum::Float8(*acc)
                } else {
                    Datum::Null
                }
            }
            Acc::MinMax { best, .. } => best.clone().unwrap_or(Datum::Null),
            // avg over zero non-null rows is NULL; otherwise the float8 mean.
            Acc::Avg { sum, n, .. } => {
                if *n == 0 {
                    Datum::Null
                } else {
                    Datum::Float8(*sum / *n as f64)
                }
            }
        }
    }
}

fn as_i64(d: &Datum) -> Option<i64> {
    match d {
        Datum::Int4(n) => Some(i64::from(*n)),
        Datum::Int8(n) => Some(*n),
        _ => None,
    }
}

fn as_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int4(n) => Some(f64::from(*n)),
        Datum::Int8(n) => Some(*n as f64),
        Datum::Float8(f) => Some(*f),
        _ => None,
    }
}

/// Execute an aggregate query over the already-`WHERE`-filtered `rows`.
pub(crate) fn execute_aggregate(
    s: &SelectStmt,
    table: Option<&Table>,
    rows: Vec<Vec<Datum>>,
) -> Result<QueryResult, ExecError> {
    // Output columns: field names + types via the shared projection resolver
    // (infer_type now understands aggregate result types).
    let (fields, out_exprs) = crate::exec::resolve_projection(&s.projection, table)?;

    // GROUP BY expressions may not themselves be aggregates.
    for g in &s.group_by {
        if contains_aggregate(g) {
            return Err(ExecError::Grouping(
                "aggregate functions are not allowed in GROUP BY".into(),
            ));
        }
    }

    // Collect (deduped) the aggregates to compute, then validate every output /
    // HAVING / ORDER BY expression is grouped-valid (data-independent).
    let mut specs: Vec<AggSpec> = Vec::new();
    for e in out_exprs
        .iter()
        .chain(s.having.iter())
        .chain(s.order_by.iter().map(|o| &o.expr))
    {
        collect_specs(e, table, &mut specs)?;
        validate_grouped(e, &s.group_by)?;
    }

    // Fold rows into groups, preserving first-appearance order.
    let has_group_by = !s.group_by.is_empty();
    let mut keys: Vec<Vec<Datum>> = Vec::new();
    let mut accs: Vec<Vec<Acc>> = Vec::new();
    let mut index: HashMap<Vec<Datum>, usize> = HashMap::new();
    for row in &rows {
        let mut key = Vec::with_capacity(s.group_by.len());
        for g in &s.group_by {
            key.push(crate::eval::eval(g, table, row)?);
        }
        let gi = match index.get(&key) {
            Some(&i) => i,
            None => {
                let i = keys.len();
                index.insert(key.clone(), i);
                keys.push(key);
                accs.push(specs.iter().map(Acc::new).collect());
                i
            }
        };
        for (spec, acc) in specs.iter().zip(accs[gi].iter_mut()) {
            acc.fold_row(spec, table, row)?;
        }
    }
    // A bare aggregate (no GROUP BY) over zero rows still yields ONE group.
    if !has_group_by && keys.is_empty() {
        keys.push(Vec::new());
        accs.push(specs.iter().map(Acc::new).collect());
    }

    // Finalize each group: HAVING filter, ORDER BY keys, projected output Datums.
    let mut out: Vec<(Vec<Datum>, Vec<Datum>)> = Vec::with_capacity(keys.len());
    for (key, group_accs) in keys.iter().zip(accs.iter()) {
        let results: Vec<Datum> = group_accs.iter().map(Acc::finish).collect();
        if let Some(h) = &s.having {
            match eval_grouped(h, table, &s.group_by, key, &specs, &results)? {
                Datum::Bool(true) => {}
                Datum::Bool(false) | Datum::Null => continue,
                _ => {
                    return Err(ExecError::TypeMismatch(
                        "argument of HAVING must be type boolean".into(),
                    ));
                }
            }
        }
        let mut order_keys = Vec::with_capacity(s.order_by.len());
        for o in &s.order_by {
            order_keys.push(eval_grouped(
                &o.expr,
                table,
                &s.group_by,
                key,
                &specs,
                &results,
            )?);
        }
        let mut projected = Vec::with_capacity(out_exprs.len());
        for e in &out_exprs {
            projected.push(eval_grouped(e, table, &s.group_by, key, &specs, &results)?);
        }
        out.push((order_keys, projected));
    }

    // SP28: SELECT DISTINCT dedups identical projected rows (first appearance).
    if s.distinct {
        let mut seen: HashSet<Vec<Datum>> = HashSet::new();
        out.retain(|(_, proj)| seen.insert(proj.clone()));
    }
    if !s.order_by.is_empty() {
        out.sort_by(|a, b| crate::exec::order_cmp(&a.0, &b.0, s));
    }
    // SP28: OFFSET then LIMIT.
    crate::exec::apply_offset_limit(&mut out, s.offset, s.limit);

    let rows_out: Vec<Vec<Option<Cell>>> = out
        .into_iter()
        .map(|(_, proj)| proj.iter().map(crate::exec::datum_to_cell).collect())
        .collect();
    let tag = format!("SELECT {}", rows_out.len());
    Ok(QueryResult::Rows {
        fields,
        rows: rows_out,
        tag,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{Column, Table};
    use pgparser::ast::{SelectStmt, Statement};

    fn table() -> Table {
        Table {
            id: 1,
            name: "t".into(),
            columns: vec![
                Column {
                    name: "k".into(),
                    ty: ColumnType::Int4,
                },
                Column {
                    name: "v".into(),
                    ty: ColumnType::Int4,
                },
            ],
        }
    }

    /// Parse one SELECT and run it over the given (already-WHERE-filtered) rows.
    fn agg(
        sql: &str,
        t: Option<&Table>,
        rows: Vec<Vec<Datum>>,
    ) -> Result<Vec<Vec<Datum>>, ExecError> {
        let stmt = pgparser::parse(sql).expect("parse").pop().expect("one");
        let Statement::Select(s) = stmt else {
            panic!("not a select");
        };
        assert!(
            is_aggregate_query(&s),
            "test sql must be an aggregate query"
        );
        match execute_aggregate(&s, t, rows)? {
            QueryResult::Rows { rows, .. } => Ok(rows
                .into_iter()
                .map(|r| r.into_iter().map(cell_to_datum).collect())
                .collect()),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    /// Decode a result cell back to a (typed-enough) Datum for assertions: we
    /// compare text-format payloads, so map back to Text/Null and ints by parse.
    fn cell_to_datum(c: Option<Cell>) -> Datum {
        match c {
            None => Datum::Null,
            Some(cell) => {
                let s = String::from_utf8(cell.text.to_vec()).expect("utf8");
                match s.parse::<i64>() {
                    Ok(n) => Datum::Int8(n),
                    Err(_) => Datum::Text(s),
                }
            }
        }
    }

    /// Like `agg`, but returns the raw text-format cells (so float results — which
    /// `cell_to_datum` cannot round-trip cleanly — can be asserted directly).
    fn agg_text(
        sql: &str,
        t: Option<&Table>,
        rows: Vec<Vec<Datum>>,
    ) -> Result<Vec<Vec<Option<String>>>, ExecError> {
        let stmt = pgparser::parse(sql).expect("parse").pop().expect("one");
        let Statement::Select(s) = stmt else {
            panic!("not a select");
        };
        match execute_aggregate(&s, t, rows)? {
            QueryResult::Rows { rows, .. } => Ok(rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|c| c.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8")))
                        .collect()
                })
                .collect()),
            other => panic!("expected Rows, got {other:?}"),
        }
    }

    fn r(vals: &[Datum]) -> Vec<Datum> {
        vals.to_vec()
    }

    fn float_table() -> Table {
        let mut t = table();
        t.columns[1].ty = ColumnType::Float8;
        t
    }

    fn int(n: i64) -> Datum {
        Datum::Int8(n)
    }

    #[test]
    fn count_star_counts_all_rows_including_nulls() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Null]),
            r(&[Datum::Int4(2), Datum::Int4(30)]),
        ];
        assert_eq!(
            agg("SELECT count(*) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![int(3)]]
        );
    }

    #[test]
    fn count_and_sum_ignore_nulls() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Null]),
            r(&[Datum::Int4(1), Datum::Int4(5)]),
        ];
        // count(v) = 2 (nulls skipped); sum(v) = 15.
        assert_eq!(
            agg("SELECT count(v), sum(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![int(2), int(15)]]
        );
    }

    #[test]
    fn min_max_over_text_and_int() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Text("b".into())]),
            r(&[Datum::Int4(1), Datum::Text("a".into())]),
            r(&[Datum::Int4(1), Datum::Text("c".into())]),
        ];
        assert_eq!(
            agg("SELECT min(v), max(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![Datum::Text("a".into()), Datum::Text("c".into())]]
        );
    }

    #[test]
    fn count_distinct_dedups_non_null() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(7)]),
            r(&[Datum::Int4(1), Datum::Int4(7)]),
            r(&[Datum::Int4(1), Datum::Int4(8)]),
            r(&[Datum::Int4(1), Datum::Null]),
        ];
        assert_eq!(
            agg(
                "SELECT count(DISTINCT v), sum(DISTINCT v) FROM t",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(2), int(15)]]
        );
    }

    #[test]
    fn group_by_groups_with_null_as_its_own_group() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
            r(&[Datum::Int4(1), Datum::Int4(5)]),
            r(&[Datum::Null, Datum::Int4(99)]),
        ];
        // ORDER BY k makes output deterministic; NULLS LAST for ASC.
        let got = agg(
            "SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k",
            Some(&t),
            rows,
        )
        .expect("agg");
        assert_eq!(
            got,
            vec![
                vec![int(1), int(2), int(15)],
                vec![int(2), int(1), int(20)],
                vec![Datum::Null, int(1), int(99)], // the NULL group, last
            ]
        );
    }

    #[test]
    fn having_filters_groups() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
        ];
        // only k=1 has count(*) > 1.
        assert_eq!(
            agg(
                "SELECT k FROM t GROUP BY k HAVING count(*) > 1 ORDER BY k",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(1)]]
        );
    }

    #[test]
    fn grouping_expression_in_projection() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Int4(20)]),
        ];
        // k+1 is built from the grouping column k -> valid.
        assert_eq!(
            agg("SELECT k + 1, sum(v) FROM t GROUP BY k", Some(&t), rows).expect("agg"),
            vec![vec![int(2), int(30)]]
        );
    }

    #[test]
    fn bare_aggregate_over_empty_table_yields_one_row() {
        let t = table();
        // count = 0, sum/min/max = NULL.
        assert_eq!(
            agg("SELECT count(*), sum(v), min(v) FROM t", Some(&t), vec![]).expect("agg"),
            vec![vec![int(0), Datum::Null, Datum::Null]]
        );
    }

    #[test]
    fn grouped_over_empty_table_yields_zero_rows() {
        let t = table();
        assert!(
            agg("SELECT k, count(*) FROM t GROUP BY k", Some(&t), vec![])
                .expect("agg")
                .is_empty()
        );
    }

    #[test]
    fn ungrouped_column_is_42803() {
        let t = table();
        let err =
            agg("SELECT v, count(*) FROM t GROUP BY k", Some(&t), vec![]).expect_err("ungrouped v");
        assert_eq!(err.into_pg().code, "42803");
    }

    #[test]
    fn unknown_function_is_42883() {
        let t = table();
        // Not an aggregate query unless an aggregate is present, so pair with count(*).
        let stmt = pgparser::parse("SELECT frobnicate(v), count(*) FROM t GROUP BY v")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let err = execute_aggregate(&s, Some(&t), vec![]).expect_err("unknown fn");
        assert_eq!(err.into_pg().code, "42883");
    }

    #[test]
    fn sum_of_text_is_42883() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let stmt = pgparser::parse("SELECT sum(v) FROM t")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let err = execute_aggregate(&s, Some(&t), vec![]).expect_err("sum(text)");
        assert_eq!(err.into_pg().code, "42883");
    }

    #[test]
    fn nested_aggregate_is_42803() {
        let t = table();
        let stmt = pgparser::parse("SELECT sum(count(v)) FROM t")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let err = execute_aggregate(&s, Some(&t), vec![]).expect_err("nested");
        assert_eq!(err.into_pg().code, "42803");
    }

    #[test]
    fn sum_overflow_is_22003() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Int8;
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int8(i64::MAX)]),
            r(&[Datum::Int4(1), Datum::Int8(1)]),
        ];
        let err = agg("SELECT sum(v) FROM t", Some(&t), rows).expect_err("overflow");
        assert_eq!(err.into_pg().code, "22003");
    }

    #[test]
    fn count_star_no_from_is_one() {
        // SELECT count(*) with no FROM -> one (empty) row folded -> 1, like PG.
        assert_eq!(
            agg("SELECT count(*)", None, vec![vec![]]).expect("agg"),
            vec![vec![int(1)]]
        );
    }

    #[test]
    fn aggregate_result_types_are_inferred_for_row_description() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let stmt = pgparser::parse("SELECT count(*), sum(k), min(v), max(k) FROM t GROUP BY k")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let (fields, _) = crate::exec::resolve_projection(&s.projection, Some(&t)).expect("fields");
        // count -> int8, sum(int4) -> int8, min(text) -> text, max(int4) -> int4
        assert_eq!(fields[0].type_oid, ColumnType::Int8.oid());
        assert_eq!(fields[1].type_oid, ColumnType::Int8.oid());
        assert_eq!(fields[2].type_oid, ColumnType::Text.oid());
        assert_eq!(fields[3].type_oid, ColumnType::Int4.oid());
        assert_eq!(fields[0].name, "count");
    }

    // ---- SP28: predicate / CASE expressions in a grouped context ----

    #[test]
    fn case_in_having_filters_groups() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
        ];
        // A CASE over an aggregate in HAVING keeps only k=1 (count(*) > 1).
        assert_eq!(
            agg(
                "SELECT k FROM t GROUP BY k \
                 HAVING CASE WHEN count(*) > 1 THEN true ELSE false END ORDER BY k",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(1)]]
        );
    }

    #[test]
    fn in_list_over_grouped_column_projection() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(20)]),
            r(&[Datum::Int4(3), Datum::Int4(30)]),
        ];
        // `k IN (1, 3)` is built from the grouping column -> valid; bool text "t"/"f".
        assert_eq!(
            agg(
                "SELECT k IN (1, 3) FROM t GROUP BY k ORDER BY k",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![
                vec![Datum::Text("t".into())],
                vec![Datum::Text("f".into())],
                vec![Datum::Text("t".into())],
            ]
        );
    }

    #[test]
    fn ungrouped_column_inside_case_is_42803() {
        let t = table();
        // `v` is neither grouped nor aggregated, even nested inside a CASE.
        let err = agg(
            "SELECT CASE WHEN v > 0 THEN 1 ELSE 0 END FROM t GROUP BY k",
            Some(&t),
            vec![],
        )
        .expect_err("ungrouped v in CASE");
        assert_eq!(err.into_pg().code, "42803");
    }

    #[test]
    fn distinct_aggregate_output_dedups() {
        let t = table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(10)]),
            r(&[Datum::Int4(2), Datum::Int4(10)]),
            r(&[Datum::Int4(3), Datum::Int4(20)]),
        ];
        // Per-group sum(v) is {10, 10, 20}; SELECT DISTINCT collapses to {10, 20}.
        assert_eq!(
            agg(
                "SELECT DISTINCT sum(v) FROM t GROUP BY k ORDER BY sum(v)",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![vec![int(10)], vec![int(20)]]
        );
    }

    // ---- SP30: float8 aggregates (avg, and sum/min/max over float8) ----

    #[test]
    fn avg_over_float8_is_the_float_mean() {
        let t = float_table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Float8(1.0)]),
            r(&[Datum::Int4(1), Datum::Float8(2.0)]),
            r(&[Datum::Int4(1), Datum::Null]), // NULL skipped
        ];
        assert_eq!(
            agg_text("SELECT avg(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![Some("1.5".to_string())]]
        );
    }

    #[test]
    fn avg_over_integers_returns_float8_value() {
        // SP30 deviation: avg(int) is float8 (PG: numeric). The VALUE is exact.
        let t = table(); // v is int4
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Int4(1)]),
            r(&[Datum::Int4(1), Datum::Int4(2)]),
        ];
        assert_eq!(
            agg_text("SELECT avg(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![Some("1.5".to_string())]]
        );
        // avg over zero rows is NULL.
        assert_eq!(
            agg_text("SELECT avg(v) FROM t", Some(&t), vec![]).expect("agg"),
            vec![vec![None]]
        );
    }

    #[test]
    fn sum_min_max_over_float8() {
        let t = float_table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Float8(1.5)]),
            r(&[Datum::Int4(1), Datum::Float8(2.0)]),
            r(&[Datum::Int4(1), Datum::Float8(-0.5)]),
        ];
        assert_eq!(
            agg_text("SELECT sum(v), min(v), max(v) FROM t", Some(&t), rows).expect("agg"),
            vec![vec![
                Some("3".to_string()), // 1.5 + 2.0 - 0.5 = 3.0 → "3"
                Some("-0.5".to_string()),
                Some("2".to_string()),
            ]]
        );
    }

    #[test]
    fn float8_result_types_for_row_description() {
        let t = float_table();
        let stmt = pgparser::parse("SELECT avg(v), sum(v), min(v) FROM t")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let (fields, _) = crate::exec::resolve_projection(&s.projection, Some(&t)).expect("fields");
        assert_eq!(fields[0].type_oid, ColumnType::Float8.oid()); // avg
        assert_eq!(fields[1].type_oid, ColumnType::Float8.oid()); // sum(float8)
        assert_eq!(fields[2].type_oid, ColumnType::Float8.oid()); // min(float8)
        // avg(int) also types as float8 for RowDescription.
        let it = table();
        let stmt = pgparser::parse("SELECT avg(v) FROM t")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let (fields, _) =
            crate::exec::resolve_projection(&s.projection, Some(&it)).expect("fields");
        assert_eq!(fields[0].type_oid, ColumnType::Float8.oid());
        assert_eq!(fields[0].name, "avg");
    }

    #[test]
    fn group_by_and_distinct_over_float8_keys() {
        let t = float_table();
        let rows = vec![
            r(&[Datum::Int4(1), Datum::Float8(1.5)]),
            r(&[Datum::Int4(2), Datum::Float8(1.5)]),
            r(&[Datum::Int4(3), Datum::Float8(2.5)]),
        ];
        // GROUP BY a float-valued expression groups equal floats together.
        assert_eq!(
            agg_text(
                "SELECT v, count(*) FROM t GROUP BY v ORDER BY v",
                Some(&t),
                rows
            )
            .expect("agg"),
            vec![
                vec![Some("1.5".to_string()), Some("2".to_string())],
                vec![Some("2.5".to_string()), Some("1".to_string())],
            ]
        );
    }

    #[test]
    fn avg_of_text_is_42883() {
        let mut t = table();
        t.columns[1].ty = ColumnType::Text;
        let stmt = pgparser::parse("SELECT avg(v) FROM t")
            .expect("parse")
            .pop()
            .expect("one");
        let Statement::Select(s) = stmt else { panic!() };
        let err = execute_aggregate(&s, Some(&t), vec![]).expect_err("avg(text)");
        assert_eq!(err.into_pg().code, "42883");
    }

    #[test]
    fn is_aggregate_query_detection() {
        fn sel(sql: &str) -> SelectStmt {
            match pgparser::parse(sql).expect("parse").pop().expect("one") {
                Statement::Select(s) => s,
                _ => panic!(),
            }
        }
        assert!(is_aggregate_query(&sel("SELECT count(*) FROM t")));
        assert!(is_aggregate_query(&sel("SELECT k FROM t GROUP BY k")));
        assert!(is_aggregate_query(&sel(
            "SELECT 1 FROM t HAVING count(*) > 0"
        )));
        assert!(is_aggregate_query(&sel(
            "SELECT k FROM t ORDER BY count(*)"
        )));
        assert!(!is_aggregate_query(&sel("SELECT k, v FROM t")));
        assert!(!is_aggregate_query(&sel(
            "SELECT k FROM t WHERE v > 1 ORDER BY k"
        )));
    }
}
