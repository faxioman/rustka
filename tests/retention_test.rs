#[cfg(test)]
mod retention_tests {
    use rustka::storage::InMemoryStorage;
    use rustka::consumer_group::ConsumerGroupManager;
    use std::time::Duration;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_message_cleanup() {
        let mut storage = InMemoryStorage::new();
        
        // Add messages to topic
        for i in 0..20 {
            let value = Bytes::from(format!("message-{}", i));
            storage.append_records("test-topic", 0, None, value);
        }
        
        // Get initial stats
        let initial_stats = storage.get_storage_stats();
        assert_eq!(initial_stats.total_messages, 20);
        
        // Clear all messages (simulating retention)
        storage.clear_all_messages();
        
        // Verify messages were cleared
        let final_stats = storage.get_storage_stats();
        assert_eq!(final_stats.total_messages, 0);
    }

    #[tokio::test]
    async fn test_cleanup_old_messages() {
        let mut storage = InMemoryStorage::new();
        
        // Add messages
        for i in 0..10 {
            let value = Bytes::from(format!("message-{}", i));
            storage.append_records("test-topic", 0, None, value);
        }
        
        // Verify messages exist
        let initial_stats = storage.get_storage_stats();
        assert_eq!(initial_stats.total_messages, 10);
        
        // Call cleanup (which checks message age)
        storage.cleanup_old_messages();
        
        // Since messages were just created, they should still exist
        let stats_after_cleanup = storage.get_storage_stats();
        assert_eq!(stats_after_cleanup.total_messages, 10);
    }

    #[tokio::test]
    async fn test_consumer_group_offset_retention() {
        let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
        let mut group_manager = ConsumerGroupManager::new_with_storage(storage.clone());
        
        // Set very short retention for testing
        group_manager.set_retention_times(100, 50); // 100ms offset retention, 50ms empty group retention
        
        // Create offsets
        let offsets = vec![
            ("test-topic".to_string(), 0, 100),
            ("test-topic".to_string(), 1, 200),
        ];
        group_manager.commit_offsets("test-group", 0, offsets).await.unwrap();
        
        // Verify offsets exist
        let initial_count = group_manager.get_offset_count();
        assert!(initial_count > 0);
        
        // Wait for retention period
        tokio::time::sleep(Duration::from_millis(150)).await;
        
        // Cleanup orphaned offsets
        group_manager.cleanup_orphaned_offsets();
        
        // For groups that don't exist, offsets should be cleaned up faster
        let final_count = group_manager.get_offset_count();
        assert_eq!(final_count, 0, "Offsets should be cleaned up after retention period");
    }

    #[tokio::test]
    async fn test_retention_with_active_consumers() {
        let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
        let mut group_manager = ConsumerGroupManager::new_with_storage(storage.clone());
        
        // Join a consumer group
        let member_id = group_manager.generate_member_id("test-client");
        let protocols = vec![("range".to_string(), Bytes::new())];
        
        let _join_result = group_manager.join_group(
            "active-group".to_string(),
            Some(member_id.clone()),
            "test-client".to_string(),
            "localhost".to_string(),
            30000,
            60000,
            "consumer".to_string(),
            protocols,
        );
        
        // Commit offsets
        let offsets = vec![
            ("test-topic".to_string(), 0, 100),
        ];
        group_manager.commit_offsets("active-group", 0, offsets).await.unwrap();
        
        // Verify offsets exist before cleanup
        let initial_offset_count = group_manager.get_offset_count();
        assert!(initial_offset_count > 0, "Should have offsets after commit");
        
        // Set retention times (note: offsets with active groups have longer retention)
        // The offset_retention_ms is used for groups that exist
        // For orphaned offsets (no group), it's divided by 12
        group_manager.set_retention_times(1000, 500); // 1 second for offsets, 500ms for empty groups
        
        // Wait a short time (less than retention)
        tokio::time::sleep(Duration::from_millis(100)).await;
        group_manager.cleanup_orphaned_offsets();
        
        // Group with active members should not be cleaned up
        assert!(group_manager.get_all_group_ids().contains(&"active-group".to_string()));
        
        // Offsets for active groups should still exist (retention is 1 second)
        assert!(group_manager.get_offset_count() > 0, "Offsets should still exist for active groups");
    }
}