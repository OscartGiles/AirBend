use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
/// Welcome to the airbend-ingest tool. Query the London Air Quality Network API (LAQN) and ingest into a databend database.
pub struct Cli {
    /// Start date to request data from. Ensure it is format 'yyyy-mm-dd'.
    #[arg(short, long)]
    pub start_date: String,

    /// End date to request data from. Ensure it is format 'yyyy-mm-dd'.
    #[arg(short, long)]
    pub end_date: String,

    /// Maximum number of concurrent connections
    #[arg(short, long, default_value_t = 5)]
    pub max_concurrent_connections: usize,

    /// Databend connection string
    #[arg(short, long)]
    pub connection_string: Option<String>,
}
