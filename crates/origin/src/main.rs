use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let config = origin::load_from_args(&args)?;
    origin::run(config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
