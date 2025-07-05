mod broker;
mod storage;
mod consumer_group;

use tracing::{info, Level};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("Starting Rustka - Minimal Kafka Protocol Implementation");

    let broker = broker::KafkaBroker::new("0.0.0.0:9092");
    broker.run().await?;

    Ok(())
}
