use std::time::Duration;

use clap::Parser;
use kafkalag::lag::{describe_group, LagReport};

#[derive(Parser)]
#[command(
    name = "kafkalag",
    about = "Report Kafka consumer-group lag over the raw wire protocol — no JVM, no Kafka distribution",
    version
)]
struct Cli {
    /// Consumer group to describe.
    #[arg(short, long)]
    group: String,

    /// Bootstrap broker, host:port (port defaults to 9092).
    #[arg(short, long, default_value = "localhost:9092")]
    broker: String,

    /// Only report this topic. Without it, every topic the group has a
    /// committed offset for is reported.
    #[arg(short, long)]
    topic: Option<String>,

    /// Socket connect/read timeout, in seconds.
    #[arg(long, default_value_t = 10)]
    timeout: u64,

    /// Emit JSON instead of a table.
    #[arg(long)]
    json: bool,
}

fn print_table(report: &LagReport) {
    if report.partitions.is_empty() {
        println!(
            "group {} has no committed offsets (coordinator {})",
            report.group, report.coordinator
        );
        return;
    }

    let group_w = report.group.len().max("GROUP".len());
    let topic_w = report
        .partitions
        .iter()
        .map(|p| p.topic.len())
        .max()
        .unwrap_or(0)
        .max("TOPIC".len());

    println!(
        "{:<group_w$}  {:<topic_w$}  {:>9}  {:>14}  {:>14}  {:>10}",
        "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG"
    );
    for p in &report.partitions {
        let current = match p.committed_offset {
            Some(o) => o.to_string(),
            None => "-".to_string(),
        };
        let lag = match p.lag {
            Some(l) => l.to_string(),
            None => "-".to_string(),
        };
        println!(
            "{:<group_w$}  {:<topic_w$}  {:>9}  {:>14}  {:>14}  {:>10}",
            report.group, p.topic, p.partition, current, p.high_watermark, lag
        );
    }
    println!();
    println!(
        "total lag: {} across {} partition(s); coordinator {}",
        report.total_lag(),
        report.partitions.len(),
        report.coordinator
    );
}

/// Hand-rolled rather than pulling in serde for six fields.
fn print_json(report: &LagReport) {
    println!("{{");
    println!("  \"group\": {},", json_string(&report.group));
    println!("  \"coordinator\": {},", json_string(&report.coordinator));
    println!("  \"totalLag\": {},", report.total_lag());
    println!("  \"partitions\": [");
    for (i, p) in report.partitions.iter().enumerate() {
        let comma = if i + 1 == report.partitions.len() {
            ""
        } else {
            ","
        };
        let current = match p.committed_offset {
            Some(o) => o.to_string(),
            None => "null".to_string(),
        };
        let lag = match p.lag {
            Some(l) => l.to_string(),
            None => "null".to_string(),
        };
        println!(
            "    {{\"topic\": {}, \"partition\": {}, \"currentOffset\": {}, \
             \"logEndOffset\": {}, \"lag\": {}}}{}",
            json_string(&p.topic),
            p.partition,
            current,
            p.high_watermark,
            lag,
            comma
        );
    }
    println!("  ]");
    println!("}}");
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let report = describe_group(
        &cli.broker,
        &cli.group,
        cli.topic.as_deref(),
        Duration::from_secs(cli.timeout),
    )?;
    if cli.json {
        print_json(&report);
    } else {
        print_table(&report);
    }
    Ok(())
}
