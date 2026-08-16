//! Thread-safe bump allocator implementing `allocator-api2`'s `Allocator` trait.
//!
//! ## Why not `bumpalo`?
//!
//! [`bumpalo::Bump`] is `!Send + !Sync` — bump allocation is inherently
//! single-threaded because the bump pointer is a plain `Cell<usize>`. BEAM's
//! tokio multi-threaded runtime sends `Arc<Put>` between worker threads via
//! channels, which requires `Put: Send`. If the `BTreeMap` inside `Put` uses
//! a `!Send` allocator, the entire `Put` becomes `!Send` and cannot cross
//! thread boundaries.
//!
//! ## Design
//!
//! `SyncBumpArena` wraps a `Mutex<SyncBumpInner>` behind an `Arc`. The mutex
//! is only contended during **allocation** (construction of BTreeMap nodes).
//! Read-only access to the BTreeMap (lookups, iteration, serialization) does
//! not touch the allocator at all — the `Allocator` trait's `allocate` and
//! `deallocate` methods are only called during insertion and drop.
//!
//! The inner state holds a chunk list (`Vec<Chunk>`) and a cursor pointing
//! into the current chunk. When the current chunk is exhausted, a new chunk
//! is allocated from the global allocator (doubling in size, starting at
//! 4 KiB). `deallocate` is a no-op — all memory is freed at once when the
//! last `Arc` reference drops.
//!
//! ## Performance
//!
//! - **Allocation**: `Mutex::lock` + pointer bump. The lock is held for
//!   nanoseconds (just advancing a cursor). In practice, contention is rare
//!   because BTreeMap construction happens in a single actor's context.
//! - **Drop**: O(chunks) — free each chunk. O(1) relative to the number of
//!   entries in the BTreeMap. This is the primary win: std's BTreeMap drop
//!   walks every node; ours just frees a few chunks.
//! - **Clone**: `Arc::clone` — one atomic increment.
//!
//! ## Safety
//!
//! The `unsafe impl Allocator` delegates to `SyncBumpInner::allocate`,
//! which returns valid, aligned memory from a chunk. The memory remains
//! valid until the `Arc<SyncBumpInner>` is dropped (i.e., until all clones
//! of the arena are gone). `deallocate` is a no-op, which is sound for bump
//! allocators.

use allocator_api2::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

// ───────────────────────────────────────────────────────────────────────
// Chunk — a single contiguous block of memory
// ───────────────────────────────────────────────────────────────────────

/// A contiguous block of memory used by the bump allocator.
///
/// Memory is allocated from the global allocator and freed on drop.
struct Chunk {
    /// The backing memory, allocated via `Global`.
    /// Stored as `Vec<u8>` for automatic drop — when the `Chunk` is dropped,
    /// the `Vec` returns its memory to the global allocator.
    data: Vec<u8>,
}

impl Chunk {
    /// Allocates a new chunk of the given size.
    fn new(size: usize) -> Self {
        // Use `Layout` for proper alignment on the Vec's backing allocation.
        // We align to 16 to cover most BTreeMap node types.
        let layout = Layout::from_size_align(size, 16).expect("invalid layout");
        let ptr = Global.allocate(layout).expect("global alloc failed").cast();
        // SAFETY: we just allocated `size` bytes with alignment 16.
        let data = unsafe { Vec::from_raw_parts(ptr.as_ptr(), 0, size) };
        Self { data }
    }

    /// Returns the usable capacity of this chunk.
    #[inline]
    fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Returns a raw pointer to the start of the chunk's unused region.
    #[inline]
    fn start(&self) -> *const u8 {
        self.data.as_ptr()
    }
}

// ───────────────────────────────────────────────────────────────────────
// SyncBumpInner — the actual bump allocator state (behind Mutex)
// ───────────────────────────────────────────────────────────────────────

/// Internal state of the bump allocator, protected by a `Mutex`.
struct SyncBumpInner {
    /// Chunks of backing memory. The last chunk is the "current" one being
    /// bumped. Previous chunks are full.
    chunks: Vec<Chunk>,
    /// Offset (in bytes) into the current chunk where the next allocation
    /// will start.
    cursor: usize,
    /// The capacity of the current (last) chunk.
    current_cap: usize,
}

impl SyncBumpInner {
    /// Creates a new empty inner state.
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            cursor: 0,
            current_cap: 0,
        }
    }

    /// Creates a new inner state with an initial chunk of the given size.
    fn with_capacity(cap: usize) -> Self {
        let chunk = Chunk::new(cap);
        let cap = chunk.capacity();
        Self {
            chunks: vec![chunk],
            cursor: 0,
            current_cap: cap,
        }
    }

    /// Allocates `layout.size()` bytes with `layout.align()` alignment from
    /// the bump arena. Returns a `NonNull<[u8]>` slice.
    ///
    /// If the current chunk doesn't have enough space, a new chunk is
    /// allocated (doubling in size, minimum 4 KiB).
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size();
        let align = layout.align();

        // Handle zero-sized allocations — return a dangling but aligned pointer.
        if size == 0 {
            return Ok(NonNull::slice_from_raw_parts(NonNull::dangling(), 0));
        }

        // Try the current chunk first.
        if let Some(ptr) = self.try_alloc_in_current(align, size) {
            return Ok(NonNull::slice_from_raw_parts(
                NonNull::new(ptr).expect("non-null from valid chunk"),
                size,
            ));
        }

        // Current chunk is full — grow.
        self.grow(align, size);

        // Try again with the new chunk.
        let ptr = self
            .try_alloc_in_current(align, size)
            .expect("freshly grown chunk must have room");
        Ok(NonNull::slice_from_raw_parts(
            NonNull::new(ptr).expect("non-null from valid chunk"),
            size,
        ))
    }

    /// Attempts to allocate from the current chunk, aligning `cursor` to
    /// `align` and checking that `size` bytes fit. Returns `Some(ptr)` on
    /// success, `None` if the chunk is full.
    #[inline]
    fn try_alloc_in_current(&mut self, align: usize, size: usize) -> Option<*mut u8> {
        let chunk = self.chunks.last()?;
        let base = chunk.start() as usize;
        let offset = self.cursor;

        // Align the cursor upward.
        let aligned_offset = (base + offset + align - 1) & !(align - 1);
        let padding = aligned_offset - base - offset;
        let new_cursor = offset + padding + size;

        if new_cursor > self.current_cap {
            return None;
        }

        self.cursor = new_cursor;
        Some(aligned_offset as *mut u8)
    }

    /// Allocates a new chunk large enough for the requested allocation.
    /// Chunk size doubles each time, starting at 4 KiB.
    fn grow(&mut self, _align: usize, size: usize) {
        // Minimum chunk size is 4 KiB. Double from the last chunk, but ensure
        // we have at least `size` bytes available.
        let next_size = (self.current_cap * 2).max(4096).max(size);

        let chunk = Chunk::new(next_size);
        self.current_cap = chunk.capacity();
        self.cursor = 0;
        self.chunks.push(chunk);
    }
}

// ───────────────────────────────────────────────────────────────────────
// SyncBumpArena — the public Allocator impl (Arc<Mutex<SyncBumpInner>>)
// ───────────────────────────────────────────────────────────────────────

/// A thread-safe bump arena allocator implementing `allocator-api2`'s
/// `Allocator` trait.
///
/// Holds an `Arc<Mutex<SyncBumpInner>>` so it is `Clone + Send + Sync + 'static`.
/// All clones share the same arena — when the last clone drops, all memory is
/// freed at once (the chunks' `Vec` drop returns memory to the global allocator).
///
/// See the [module-level documentation](self) for the full design rationale
/// and safety model.
#[derive(Clone)]
pub struct SyncBumpArena {
    inner: Arc<Mutex<SyncBumpInner>>,
}

impl SyncBumpArena {
    /// Creates a new `SyncBumpArena` with no initial capacity.
    ///
    /// The first allocation will trigger a 4 KiB chunk allocation.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SyncBumpInner::new())),
        }
    }

    /// Creates a new `SyncBumpArena` with an initial chunk of the given size.
    ///
    /// This avoids a growth step on the first allocations if the approximate
    /// total size is known in advance.
    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SyncBumpInner::with_capacity(capacity))),
        }
    }
}

impl Default for SyncBumpArena {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SyncBumpArena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncBumpArena")
            .field("inner", &Arc::as_ptr(&self.inner))
            .finish()
    }
}

// SAFETY: `allocate` delegates to `SyncBumpInner::allocate` which returns
// valid, aligned memory from a chunk. The memory remains valid until the
// `Arc<Mutex<SyncBumpInner>>` is dropped (when all clones of the arena are
// gone). `deallocate` is a no-op — sound for bump allocators because memory
// is freed in bulk on drop, never per-allocation.
unsafe impl Allocator for SyncBumpArena {
    #[inline]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let mut inner = self.inner.lock().expect("mutex poisoned");
        inner.allocate(layout)
    }

    #[inline]
    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {
        // No-op: all memory is freed when the arena drops.
    }

    #[inline]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ptr = self.allocate(layout)?;
        // SAFETY: the memory is valid and we own it exclusively (just allocated).
        unsafe { ptr.cast::<u8>().as_ptr().write_bytes(0, layout.size()) };
        Ok(ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocator_api2::alloc::Layout;

    #[test]
    fn test_allocate_basic() {
        let arena = SyncBumpArena::new();
        let layout = Layout::from_size_align(64, 8).unwrap();
        let ptr = arena.allocate(layout).unwrap();
        assert_eq!(ptr.len(), 64);
        // Verify the memory is usable
        unsafe {
            ptr.cast::<u8>().as_ptr().write_bytes(0xAB, 64);
        }
    }

    #[test]
    fn test_allocate_alignment() {
        let arena = SyncBumpArena::new();
        for &align in &[1usize, 2, 4, 8, 16, 32, 64, 128] {
            let layout = Layout::from_size_align(1, align).unwrap();
            let ptr = arena.allocate(layout).unwrap();
            let addr = ptr.cast::<u8>().as_ptr() as usize;
            assert_eq!(addr % align, 0, "alignment {} not respected", align);
        }
    }

    #[test]
    fn test_allocate_zeroed() {
        let arena = SyncBumpArena::new();
        let layout = Layout::from_size_align(128, 8).unwrap();
        let ptr = arena.allocate_zeroed(layout).unwrap();
        let slice = unsafe { ptr.as_ref() };
        assert!(slice.iter().all(|&b| b == 0), "allocate_zeroed returned non-zero memory");
    }

    #[test]
    fn test_zero_size_allocation() {
        let arena = SyncBumpArena::new();
        let layout = Layout::from_size_align(0, 1).unwrap();
        let ptr = arena.allocate(layout).unwrap();
        assert_eq!(ptr.len(), 0);
    }

    #[test]
    fn test_clone_shares_arena() {
        let arena1 = SyncBumpArena::new();
        let arena2 = arena1.clone();

        let layout = Layout::from_size_align(8, 8).unwrap();
        let ptr1 = arena1.allocate(layout).unwrap();
        let ptr2 = arena2.allocate(layout).unwrap();

        // Both allocations from the same arena — distinct addresses
        let addr1 = ptr1.cast::<u8>().as_ptr() as usize;
        let addr2 = ptr2.cast::<u8>().as_ptr() as usize;
        assert_ne!(addr1, addr2);
    }

    #[test]
    fn test_deallocate_is_noop() {
        let arena = SyncBumpArena::new();
        let layout = Layout::from_size_align(32, 8).unwrap();
        let ptr = arena.allocate(layout).unwrap();

        // deallocate should not crash
        unsafe { arena.deallocate(ptr.cast(), layout) };

        // Memory is still valid after deallocate
        unsafe {
            ptr.cast::<u8>().as_ptr().write_bytes(0xCD, 32);
        }
    }

    #[test]
    fn test_chunk_growth() {
        // Start with a tiny capacity — force growth
        let arena = SyncBumpArena::with_capacity(64);

        // Allocate more than the initial capacity
        let layout = Layout::from_size_align(128, 8).unwrap();
        let ptr = arena.allocate(layout).unwrap();
        assert_eq!(ptr.len(), 128);

        // Further allocations should work after growth
        let layout2 = Layout::from_size_align(256, 8).unwrap();
        let ptr2 = arena.allocate(layout2).unwrap();
        assert_eq!(ptr2.len(), 256);
    }

    #[test]
    fn test_many_allocations() {
        let arena = SyncBumpArena::with_capacity(4096);
        let layout = Layout::from_size_align(48, 8).unwrap();

        // Allocate many times — should trigger multiple chunk growths
        let mut ptrs = Vec::new();
        for _ in 0..1000 {
            ptrs.push(arena.allocate(layout).unwrap());
        }

        // All pointers should be distinct
        let addrs: Vec<usize> = ptrs.iter().map(|p| p.cast::<u8>().as_ptr() as usize).collect();
        let unique: std::collections::HashSet<_> = addrs.iter().collect();
        assert_eq!(unique.len(), 1000, "all 1000 allocations should be at distinct addresses");
    }

    #[test]
    fn test_drop_frees_memory() {
        // Verify Arc refcounting works — dropping all clones frees the arena.
        let arena1 = SyncBumpArena::new();
        let arena2 = arena1.clone();

        drop(arena1);
        // arena2 still alive — allocation should work
        let layout = Layout::from_size_align(16, 8).unwrap();
        let _ = arena2.allocate(layout).unwrap();

        drop(arena2);
        // No way to test memory freeing directly, but no crash = success
    }

    #[test]
    fn test_with_capacity() {
        let arena = SyncBumpArena::with_capacity(8192);
        let layout = Layout::from_size_align(4096, 8).unwrap();
        let ptr = arena.allocate(layout).unwrap();
        assert_eq!(ptr.len(), 4096);
    }

    #[test]
    fn test_send_sync_bounds() {
        // Compile-time verification that SyncBumpArena is Send + Sync
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SyncBumpArena>();
    }

    #[test]
    fn test_concurrent_allocation() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let arena = StdArc::new(SyncBumpArena::with_capacity(4096 * 4));
        let layout = Layout::from_size_align(64, 8).unwrap();

        // Spawn multiple threads that allocate from the same arena
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let arena = StdArc::clone(&arena);
                thread::spawn(move || {
                    let mut ptrs = Vec::new();
                    for _ in 0..100 {
                        ptrs.push(arena.allocate(layout).unwrap());
                    }
                    // Verify all allocations are distinct
                    let addrs: Vec<usize> =
                        ptrs.iter().map(|p| p.cast::<u8>().as_ptr() as usize).collect();
                    let unique: std::collections::HashSet<_> = addrs.iter().collect();
                    assert_eq!(unique.len(), 100);
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("thread should not panic");
        }
    }

    #[test]
    fn test_btreemap_with_arena() {
        use arena_btreemap::BTreeMap;

        let arena = SyncBumpArena::with_capacity(4096);
        let mut map: BTreeMap<String, i32, SyncBumpArena> = BTreeMap::new_in(arena);

        // Insert entries — each BTreeMap node is allocated from the arena
        for i in 0..100 {
            map.insert(format!("key_{:04}", i), i);
        }

        // Verify entries are correct
        for i in 0..100 {
            assert_eq!(map.get(&format!("key_{:04}", i)), Some(&i));
        }

        // Verify iteration order (sorted by key)
        let keys: Vec<_> = map.keys().take(5).collect();
        assert_eq!(keys[0], "key_0000");
        assert_eq!(keys[4], "key_0004");
    }

    #[test]
    fn test_btreemap_clone_shares_arena() {
        use arena_btreemap::BTreeMap;

        let arena = SyncBumpArena::with_capacity(4096);
        let mut map: BTreeMap<String, i32, SyncBumpArena> = BTreeMap::new_in(arena.clone());
        map.insert("a".to_string(), 1);
        map.insert("b".to_string(), 2);

        // Clone the map — should share the same arena
        let map2 = map.clone();
        assert_eq!(map2.get("a"), Some(&1));
        assert_eq!(map2.get("b"), Some(&2));

        // Original map still works
        assert_eq!(map.get("a"), Some(&1));
    }
}
