use clap::Parser;
use conformance::{CaseResult, Report, diff, run_one, split_statements};
use tokio_postgres::NoTls;

/// Differential conformance runner: oracle (real PostgreSQL) vs subject (crabgresql).
#[derive(Parser)]
struct Args {
    /// e.g. "host=127.0.0.1 port=54320 user=postgres dbname=postgres"
    #[arg(long)]
    oracle_url: String,
    /// e.g. "host=127.0.0.1 port=5433 user=crab dbname=crab"
    #[arg(long)]
    subject_url: String,
    /// Directory of .sql corpus files.
    #[arg(long, default_value = "crates/conformance/corpus")]
    corpus: std::path::PathBuf,
    #[arg(long, default_value = "parity.json")]
    out: std::path::PathBuf,
    #[arg(long, default_value = "parity.md")]
    summary: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (oracle, oracle_conn) = tokio_postgres::connect(&args.oracle_url, NoTls).await?;
    tokio::spawn(oracle_conn);
    let (subject, subject_conn) = tokio_postgres::connect(&args.subject_url, NoTls).await?;
    tokio::spawn(subject_conn);

    let mut files: Vec<_> = std::fs::read_dir(&args.corpus)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();

    let mut cases = Vec::new();
    for path in files {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let sql = std::fs::read_to_string(&path)?;
        for stmt in split_statements(&sql) {
            let o = run_one(&oracle, &stmt).await;
            let s = run_one(&subject, &stmt).await;
            let d = diff(&o, &s);
            cases.push(CaseResult {
                file: name.clone(),
                sql: stmt,
                matched: d.matched,
                detail: d.detail,
            });
        }
    }

    let report = Report::new(cases);
    std::fs::write(&args.out, serde_json::to_string_pretty(&report)?)?;
    std::fs::write(&args.summary, report.markdown_summary())?;
    println!(
        "parity: {:.1}% ({} / {}) -> {} / {}",
        report.parity_percent,
        report.matched,
        report.total,
        args.out.display(),
        args.summary.display()
    );
    Ok(())
}
