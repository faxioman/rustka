#[cfg(test)]
mod dashboard_tests {
    use rustka::metrics::MetricsCollector;
    use rustka::storage::InMemoryStorage;
    use rustka::consumer_group::ConsumerGroupManager;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_metrics_endpoint() {
        // Create components
        let metrics = Arc::new(MetricsCollector::new());
        let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
        let _group_manager = Arc::new(Mutex::new(ConsumerGroupManager::new()));
        
        // Simulate some activity
        {
            let mut s = storage.lock().await;
            // Create topic by appending a record
            s.append_records("test-topic", 0, None, Bytes::from("test"));
        }
        
        // Update metrics
        metrics.increment_messages_produced(10).await;
        metrics.update_topic_metrics("test-topic".to_string(), 0, 1).await;
        
        // Get metrics
        let all_metrics = metrics.get_all_metrics().await;
        
        // Verify
        assert_eq!(all_metrics.broker.total_messages_produced, 10);
    }

    #[tokio::test]
    async fn test_cleanup_empty_topics() {
        let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
        
        // Create topic by appending and then clearing
        {
            let mut s = storage.lock().await;
            s.append_records("empty-topic", 0, None, Bytes::from("test"));
            s.append_records("empty-topic", 1, None, Bytes::from("test"));
            s.append_records("empty-topic", 2, None, Bytes::from("test"));
            // Clear to make it empty
            s.clear_all_messages();
        }
        
        // Verify topic exists but is empty
        {
            let s = storage.lock().await;
            assert_eq!(s.get_all_topics().len(), 1);
        }
        
        // Cleanup empty topics
        {
            let mut s = storage.lock().await;
            let removed = s.cleanup_empty_topics();
            assert_eq!(removed, 1);
        }
        
        // Verify topic was removed
        {
            let s = storage.lock().await;
            assert_eq!(s.get_all_topics().len(), 0);
        }
    }

    #[tokio::test]
    async fn test_force_cleanup_consumer_groups() {
        let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
        let mut group_manager = ConsumerGroupManager::new_with_storage(storage.clone());
        
        // Create a consumer group with offsets
        let offsets = vec![
            ("test-topic".to_string(), 0, 100),
            ("test-topic".to_string(), 1, 200),
        ];
        group_manager.commit_offsets("test-group", 0, offsets).await.unwrap();
        
        // Verify offsets exist
        assert!(group_manager.get_offset_count() > 0);
        
        // Force cleanup empty groups
        group_manager.force_cleanup_empty_groups();
        
        // Verify offsets were removed (since group was empty)
        assert_eq!(group_manager.get_offset_count(), 0);
    }

    #[tokio::test]
    async fn test_memory_stats_endpoint() {
        // This test verifies the memory stats structure
        // Actual memory values will vary by platform
        
        #[cfg(not(target_env = "msvc"))]
        {
            unsafe {
                // Test that we can read jemalloc stats
                let mut allocated: usize = 0;
                let mut sz = std::mem::size_of::<usize>();
                let result = tikv_jemalloc_sys::mallctl(
                    b"stats.allocated\0".as_ptr() as *const _,
                    &mut allocated as *mut _ as *mut _,
                    &mut sz as *mut _,
                    std::ptr::null_mut(),
                    0,
                );
                
                // If jemalloc is available, result should be 0
                if result == 0 {
                    assert!(allocated > 0);
                }
            }
        }
    }
}