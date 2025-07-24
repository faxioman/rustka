use rustka::{broker, dashboard, metrics::MetricsCollector};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_env = "msvc"))]
    {
        std::env::set_var("MALLOC_CONF", "dirty_decay_ms:10000,muzzy_decay_ms:10000");
    }
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("Starting Rustka - Minimal Kafka Protocol Implementation");
    
    let metrics = Arc::new(MetricsCollector::new());
    let broker = broker::KafkaBroker::new_with_metrics("0.0.0.0:9092", metrics.clone());
    let dashboard_metrics = metrics.clone();
    let dashboard_storage = broker.storage.clone();
    let dashboard_group_manager = broker.group_manager.clone();
    tokio::spawn(async move {
        dashboard::start_dashboard(dashboard_metrics, dashboard_storage, dashboard_group_manager).await;
    });

    broker.run().await?;

    Ok(())
}