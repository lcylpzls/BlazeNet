use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config = scheduler::load_from_args(&args)?;
    scheduler::run(config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
