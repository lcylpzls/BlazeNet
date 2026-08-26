use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let path = agent::config::Config::parse_args(&args)?;
    let config = agent::config::Config::load(&path)?;
    let _handle = agent::start(config).await?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}
