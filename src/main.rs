use ci_pipeline::cli::*;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    match args.command {
        Commands::Run(run_args) => handle_run(run_args).await,
        Commands::Validate(validate_args) => handle_validate(validate_args),
        Commands::Graph(graph_args) => handle_graph(graph_args),
        Commands::Clean(clean_args) => handle_clean(clean_args),
        Commands::Cache(cache_args) => handle_cache(cache_args).await,
    }
}
