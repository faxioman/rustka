use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tokio::sync::Mutex;
use rustka::storage::InMemoryStorage;
use rustka::consumer_group::ConsumerGroupManager;

fn measure_memory() -> usize {
    // Force allocator to update stats
    std::thread::yield_now();
    
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        let output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .expect("Failed to execute ps");
        
        let rss_kb = String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse::<usize>()
            .unwrap_or(0);
        
        rss_kb * 1024 // Convert to bytes
    }
    
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        let status = fs::read_to_string("/proc/self/status").unwrap();
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    return parts[1].parse::<usize>().unwrap_or(0) * 1024;
                }
            }
        }
        0
    }
    
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

fn consumer_group_memory_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    c.bench_function("consumer_group_offset_accumulation", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
                let mut group_manager = ConsumerGroupManager::new_with_storage(storage.clone());
                
                let mem_before = measure_memory();
                
                // Create and destroy 100 consumer groups
                for i in 0..100 {
                    let group_id = format!("bench-group-{}", i);
                    
                    // Commit offsets for 10 topics, 3 partitions each
                    for j in 0..10 {
                        let topic = format!("bench-topic-{}", j);
                        let offsets = vec![
                            (topic.clone(), 0, 100),
                            (topic.clone(), 1, 200),
                            (topic.clone(), 2, 300),
                        ];
                        
                        group_manager.commit_offsets(&group_id, 0, offsets).await.unwrap();
                    }
                    
                    // Simulate group removal
                    if let Ok(result) = group_manager.join_group(
                        group_id.clone(),
                        None,
                        "bench-client".to_string(),
                        "localhost".to_string(),
                        30000,
                        60000,
                        "consumer".to_string(),
                        vec![("range".to_string(), bytes::Bytes::new())],
                    ) {
                        let _ = group_manager.leave_group(&group_id, &result.member_id);
                    }
                }
                
                let mem_after = measure_memory();
                let memory_growth = mem_after.saturating_sub(mem_before);
                
                println!("Memory growth: {} bytes ({} KB)", memory_growth, memory_growth / 1024);
                println!("Stored offsets: {}", group_manager.get_offset_count());
                println!("Active groups: {}", group_manager.get_all_group_ids().len());
                
                // Cleanup and measure again
                group_manager.cleanup_orphaned_offsets();
                
                let mem_after_cleanup = measure_memory();
                let cleanup_reduction = mem_after.saturating_sub(mem_after_cleanup);
                
                println!("After cleanup - offsets: {}", group_manager.get_offset_count());
                println!("Memory freed by cleanup: {} bytes", cleanup_reduction);
            })
        })
    });
}

fn storage_memory_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    c.bench_function("storage_message_accumulation", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut storage = InMemoryStorage::new();
                
                let mem_before = measure_memory();
                
                // Produce messages to multiple topics
                for i in 0..50 {
                    let topic = format!("storage-topic-{}", i % 10);
                    
                    // Produce 100 messages per topic
                    for j in 0..100 {
                        let key = Some(bytes::Bytes::from(format!("key-{}", j)));
                        let value = bytes::Bytes::from(vec![0u8; 1024]); // 1KB message
                        
                        storage.append_records(&topic, j % 3, key, value);
                    }
                }
                
                let mem_after = measure_memory();
                let memory_growth = mem_after.saturating_sub(mem_before);
                
                println!("Storage memory growth: {} bytes ({} KB)", memory_growth, memory_growth / 1024);
                
                // Trigger cleanup
                storage.cleanup_old_messages();
                
                let mem_after_cleanup = measure_memory();
                println!("After cleanup: {} bytes", mem_after_cleanup.saturating_sub(mem_before));
            })
        })
    });
}

criterion_group!(benches, consumer_group_memory_benchmark, storage_memory_benchmark);
criterion_main!(benches);