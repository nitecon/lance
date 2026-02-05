//! NUMA (Non-Uniform Memory Access) awareness utilities.
//!
//! Provides topology detection, thread-to-NUMA-node affinity, and
//! NUMA-local memory allocation for optimal performance on multi-socket systems.

#[cfg(target_os = "linux")]
use crate::LanceError;
use crate::Result;
use std::sync::OnceLock;

/// Cached NUMA topology
static NUMA_TOPOLOGY: OnceLock<NumaTopology> = OnceLock::new();

/// NUMA topology information
#[derive(Debug, Clone)]
pub struct NumaTopology {
    /// Number of NUMA nodes
    pub node_count: usize,
    /// CPUs per NUMA node
    pub cpus_per_node: Vec<Vec<usize>>,
    /// Total CPU count
    pub cpu_count: usize,
}

impl NumaTopology {
    /// Detect NUMA topology from the system
    #[cfg(target_os = "linux")]
    pub fn detect() -> Self {
        let node_count = Self::detect_node_count();
        let cpu_count = Self::detect_cpu_count();
        let cpus_per_node = Self::detect_cpus_per_node(node_count, cpu_count);

        Self {
            node_count,
            cpus_per_node,
            cpu_count,
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[must_use]
    #[allow(clippy::redundant_closure_for_method_calls)] // Can't use method ref due to MSRV
    pub fn detect() -> Self {
        // Non-Linux: assume single NUMA node with all CPUs
        let cpu_count = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        Self {
            node_count: 1,
            cpus_per_node: vec![(0..cpu_count).collect()],
            cpu_count,
        }
    }

    #[cfg(target_os = "linux")]
    fn detect_node_count() -> usize {
        // Try reading from sysfs
        if let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| s.starts_with("node"))
                        .unwrap_or(false)
                })
                .count()
                .max(1)
        } else {
            1
        }
    }

    #[cfg(target_os = "linux")]
    fn detect_cpu_count() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    }

    #[cfg(target_os = "linux")]
    fn detect_cpus_per_node(node_count: usize, cpu_count: usize) -> Vec<Vec<usize>> {
        let mut result = vec![Vec::new(); node_count];

        for node in 0..node_count {
            let path = format!("/sys/devices/system/node/node{}/cpulist", node);
            if let Ok(content) = std::fs::read_to_string(&path) {
                result[node] = Self::parse_cpu_list(&content);
            }
        }

        // Fallback: distribute CPUs evenly if sysfs reading failed
        if result.iter().all(|v| v.is_empty()) {
            let cpus_per = cpu_count / node_count.max(1);
            for (node, cpus) in result.iter_mut().enumerate() {
                let start = node * cpus_per;
                let end = if node == node_count - 1 {
                    cpu_count
                } else {
                    start + cpus_per
                };
                *cpus = (start..end).collect();
            }
        }

        result
    }

    /// Parse CPU list format like "0-3,5,7-9" into individual CPU numbers
    #[must_use]
    pub fn parse_cpu_list(s: &str) -> Vec<usize> {
        let mut cpus = Vec::new();
        for part in s.trim().split(',') {
            if let Some((start, end)) = part.split_once('-') {
                if let (Ok(s), Ok(e)) = (start.parse::<usize>(), end.parse::<usize>()) {
                    cpus.extend(s..=e);
                }
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            }
        }
        cpus
    }

    /// Get cached topology (lazily initialized)
    pub fn get() -> &'static NumaTopology {
        NUMA_TOPOLOGY.get_or_init(Self::detect)
    }

    /// Get the NUMA node for a given CPU
    #[must_use]
    pub fn cpu_to_node(&self, cpu: usize) -> usize {
        for (node, cpus) in self.cpus_per_node.iter().enumerate() {
            if cpus.contains(&cpu) {
                return node;
            }
        }
        0 // Default to node 0
    }

    /// Get CPUs for a specific NUMA node
    #[must_use]
    pub fn node_cpus(&self, node: usize) -> &[usize] {
        self.cpus_per_node.get(node).map_or(&[], Vec::as_slice)
    }
}

/// NUMA-aware allocator for buffer pools
pub struct NumaAllocator {
    preferred_node: usize,
}

impl NumaAllocator {
    /// Create allocator preferring a specific NUMA node
    #[must_use]
    pub fn new(node: usize) -> Self {
        Self {
            preferred_node: node,
        }
    }

    /// Create allocator for the current thread's NUMA node
    #[must_use]
    pub fn for_current_thread() -> Self {
        let node = get_current_numa_node();
        Self::new(node)
    }

    /// Allocate a NUMA-local buffer
    ///
    /// # Errors
    /// Returns an error if buffer allocation fails.
    pub fn allocate(&self, size: usize) -> Result<crate::buffer::NumaAlignedBuffer> {
        crate::buffer::NumaAlignedBuffer::new(size, self.preferred_node)
    }

    /// Get the preferred NUMA node
    #[must_use]
    pub fn preferred_node(&self) -> usize {
        self.preferred_node
    }
}

/// Get the NUMA node for the current thread
#[cfg(target_os = "linux")]
pub fn get_current_numa_node() -> usize {
    // SAFETY: getcpu syscall with valid output pointers returns current CPU/node
    let cpu = unsafe {
        let mut cpu: u32 = 0;
        let mut node: u32 = 0;
        if libc::syscall(
            libc::SYS_getcpu,
            &mut cpu as *mut u32,
            &mut node as *mut u32,
            std::ptr::null::<libc::c_void>(),
        ) == 0
        {
            node as usize
        } else {
            0
        }
    };
    cpu
}

#[cfg(not(target_os = "linux"))]
#[must_use]
pub fn get_current_numa_node() -> usize {
    0
}

/// Pin the current thread to a specific CPU
#[cfg(target_os = "linux")]
pub fn pin_thread_to_cpu(cpu: usize) -> Result<()> {
    use std::mem::MaybeUninit;

    // SAFETY: cpu_set_t is safely zeroed, CPU_ZERO/CPU_SET are safe macros,
    // sched_setaffinity with pid=0 targets current thread
    unsafe {
        let mut cpuset = MaybeUninit::<libc::cpu_set_t>::zeroed();
        let cpuset = cpuset.assume_init_mut();
        libc::CPU_ZERO(cpuset);
        libc::CPU_SET(cpu, cpuset);

        let result = libc::sched_setaffinity(
            0, // current thread
            std::mem::size_of::<libc::cpu_set_t>(),
            cpuset,
        );

        if result == 0 {
            Ok(())
        } else {
            Err(LanceError::PinFailed(cpu))
        }
    }
}

#[cfg(not(target_os = "linux"))]
/// Pin the current thread to a specific CPU.
///
/// # Errors
/// Returns an error if thread pinning fails (non-Linux always succeeds).
pub fn pin_thread_to_cpu(_cpu: usize) -> Result<()> {
    // Thread pinning not supported on non-Linux
    Ok(())
}

/// Pin the current thread to CPUs on a specific NUMA node.
///
/// # Errors
/// Returns an error if thread pinning fails.
pub fn pin_thread_to_numa_node(node: usize) -> Result<()> {
    let topology = NumaTopology::get();
    let cpus = topology.node_cpus(node);

    if cpus.is_empty() {
        return Ok(()); // No pinning if no CPUs found
    }

    // Pin to first CPU on the node
    pin_thread_to_cpu(cpus[0])
}

/// Thread pool configuration for NUMA-aware workloads
#[derive(Debug, Clone)]
pub struct NumaThreadPoolConfig {
    /// Threads per NUMA node
    pub threads_per_node: usize,
    /// Whether to pin threads to CPUs
    pub pin_threads: bool,
}

impl Default for NumaThreadPoolConfig {
    fn default() -> Self {
        Self {
            threads_per_node: 2,
            pin_threads: true,
        }
    }
}

impl NumaThreadPoolConfig {
    /// Calculate total thread count based on NUMA topology
    #[must_use]
    pub fn total_threads(&self) -> usize {
        let topology = NumaTopology::get();
        topology.node_count * self.threads_per_node
    }

    /// Get CPU affinity for a thread index
    #[must_use]
    pub fn thread_cpu(&self, thread_idx: usize) -> Option<usize> {
        if !self.pin_threads {
            return None;
        }

        let topology = NumaTopology::get();
        let node = thread_idx / self.threads_per_node;
        let node_thread = thread_idx % self.threads_per_node;

        let cpus = topology.node_cpus(node);
        cpus.get(node_thread % cpus.len()).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_topology_detect() {
        let topology = NumaTopology::detect();
        assert!(topology.node_count >= 1);
        assert!(topology.cpu_count >= 1);
    }

    #[test]
    fn test_numa_topology_cached() {
        let t1 = NumaTopology::get();
        let t2 = NumaTopology::get();
        assert_eq!(t1.node_count, t2.node_count);
    }

    #[test]
    fn test_parse_cpu_list() {
        let cpus = NumaTopology::parse_cpu_list("0-3,8-11");
        assert_eq!(cpus, vec![0, 1, 2, 3, 8, 9, 10, 11]);

        let cpus = NumaTopology::parse_cpu_list("0,2,4");
        assert_eq!(cpus, vec![0, 2, 4]);
    }

    #[test]
    fn test_numa_allocator() {
        let allocator = NumaAllocator::new(0);
        assert_eq!(allocator.preferred_node(), 0);
    }

    #[test]
    fn test_thread_pool_config() {
        let config = NumaThreadPoolConfig::default();
        assert_eq!(config.threads_per_node, 2);
    }
}
