use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post, Router},
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use crate::metrics::{MetricsCollector, AllMetrics};
use serde::Serialize;

#[derive(Clone)]
struct DashboardState {
    metrics: Arc<MetricsCollector>,
    storage: Arc<tokio::sync::Mutex<crate::storage::InMemoryStorage>>,
}

pub async fn start_dashboard(
    metrics: Arc<MetricsCollector>, 
    storage: Arc<tokio::sync::Mutex<crate::storage::InMemoryStorage>>,
    _group_manager: Arc<tokio::sync::Mutex<crate::consumer_group::ConsumerGroupManager>>,
) {
    let state = DashboardState { metrics, storage };
    
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/metrics", get(metrics_handler))
        .route("/api/cleanup/empty-topics", post(cleanup_empty_topics_handler))
        .route("/api/cleanup/all-messages", post(cleanup_all_messages_handler))
        .route("/api/memory-stats", get(memory_stats_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);
    
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();
    
    println!("📊 Dashboard available at http://127.0.0.1:8080");
    
    axum::serve(listener, app).await.unwrap();
}

async fn index_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = include_str!("../static/dashboard.html");

async fn metrics_handler(
    State(state): State<DashboardState>,
) -> Result<Json<AllMetrics>, StatusCode> {
    let all_metrics = state.metrics.get_all_metrics().await;
    Ok(Json(all_metrics))
}

#[derive(Serialize)]
struct CleanupResponse {
    success: bool,
    message: String,
    items_removed: usize,
}

async fn cleanup_empty_topics_handler(
    State(state): State<DashboardState>,
) -> Result<Json<CleanupResponse>, StatusCode> {
    let mut storage = state.storage.lock().await;
    let count = storage.cleanup_empty_topics();
    
    // Get remaining topics and update storage stats
    let remaining_topics = storage.get_all_topics();
    let stats = storage.get_storage_stats();
    drop(storage); // Release lock before async operations
    
    // Update storage stats
    state.metrics.update_storage_stats(stats).await;
    
    // Clean up topic metrics for removed topics
    state.metrics.cleanup_topic_metrics(&remaining_topics).await;
    
    Ok(Json(CleanupResponse {
        success: true,
        message: format!("Removed {} empty topics", count),
        items_removed: count,
    }))
}

async fn cleanup_all_messages_handler(
    State(state): State<DashboardState>,
) -> Result<Json<CleanupResponse>, StatusCode> {
    let mut storage = state.storage.lock().await;
    
    // Count messages before clearing
    let stats = storage.get_storage_stats();
    let message_count = stats.total_messages;
    
    // Clear all messages from all topics
    storage.clear_all_messages();
    
    // Update storage stats in metrics immediately after cleanup
    let new_stats = storage.get_storage_stats();
    drop(storage); // Release lock before async operation
    
    state.metrics.update_storage_stats(new_stats).await;
    
    Ok(Json(CleanupResponse {
        success: true,
        message: format!("Cleared {} messages from all topics", message_count),
        items_removed: message_count,
    }))
}

#[derive(Serialize)]
struct MemoryStats {
    allocated_mb: f64,
    resident_mb: f64,
    active_mb: f64,
    mapped_mb: f64,
    metadata_mb: f64,
    retained_mb: f64,
}

async fn memory_stats_handler(
    State(_state): State<DashboardState>,
) -> Result<Json<MemoryStats>, StatusCode> {
    let mut stats = MemoryStats {
        allocated_mb: 0.0,
        resident_mb: 0.0,
        active_mb: 0.0,
        mapped_mb: 0.0,
        metadata_mb: 0.0,
        retained_mb: 0.0,
    };
    
    // Get process RSS
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("ps")
            .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
        {
            if let Ok(rss_str) = String::from_utf8(output.stdout) {
                if let Ok(rss_kb) = rss_str.trim().parse::<f64>() {
                    stats.resident_mb = rss_kb / 1024.0;
                }
            }
        }
    }
    
    #[cfg(not(target_env = "msvc"))]
    {
        unsafe {
            let mut sz = std::mem::size_of::<usize>();
            
            // Allocated bytes
            let mut allocated: usize = 0;
            tikv_jemalloc_sys::mallctl(
                b"stats.allocated\0".as_ptr() as *const _,
                &mut allocated as *mut _ as *mut _,
                &mut sz as *mut _,
                std::ptr::null_mut(),
                0,
            );
            stats.allocated_mb = allocated as f64 / 1024.0 / 1024.0;
            
            // Active bytes
            let mut active: usize = 0;
            tikv_jemalloc_sys::mallctl(
                b"stats.active\0".as_ptr() as *const _,
                &mut active as *mut _ as *mut _,
                &mut sz as *mut _,
                std::ptr::null_mut(),
                0,
            );
            stats.active_mb = active as f64 / 1024.0 / 1024.0;
            
            // Mapped bytes
            let mut mapped: usize = 0;
            tikv_jemalloc_sys::mallctl(
                b"stats.mapped\0".as_ptr() as *const _,
                &mut mapped as *mut _ as *mut _,
                &mut sz as *mut _,
                std::ptr::null_mut(),
                0,
            );
            stats.mapped_mb = mapped as f64 / 1024.0 / 1024.0;
            
            // Metadata bytes
            let mut metadata: usize = 0;
            tikv_jemalloc_sys::mallctl(
                b"stats.metadata\0".as_ptr() as *const _,
                &mut metadata as *mut _ as *mut _,
                &mut sz as *mut _,
                std::ptr::null_mut(),
                0,
            );
            stats.metadata_mb = metadata as f64 / 1024.0 / 1024.0;
            
            // Retained bytes
            let mut retained: usize = 0;
            tikv_jemalloc_sys::mallctl(
                b"stats.retained\0".as_ptr() as *const _,
                &mut retained as *mut _ as *mut _,
                &mut sz as *mut _,
                std::ptr::null_mut(),
                0,
            );
            stats.retained_mb = retained as f64 / 1024.0 / 1024.0;
        }
    }
    
    Ok(Json(stats))
}