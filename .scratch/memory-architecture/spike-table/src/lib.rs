//! THROWAWAY spike for `.scratch/memory-architecture/PRD.md` ruling R5 — a
//! Dashtable-shaped segmented extendible-hash table with tagged, partly-inline slot
//! words. Answers the PRD's open question "exact inline-value threshold in table
//! slots", fixes the SCAN cursor scheme, and reserves R9's eviction bits in the
//! segment header.
//!
//! Not production code and not a workspace member. Findings live in
//! `../spike-report-table.md`.

pub mod baseline;
pub mod heap;
pub mod measure;
pub mod segment;
pub mod table;
pub mod word;
pub mod workload;

use table::Table;
use word::{W8Int, W8Ptr, W16, W8};

/// The control: nothing inlines, both words are pointers. 16-byte slot, 14 per bucket.
pub type TablePtr = Table<W8Ptr, W8Ptr, 14>;
/// Integers inline in the value word only. 16-byte slot, 14 per bucket.
pub type TableInt = Table<W8Ptr, W8Int, 14>;
/// Integers and ≤ 7-byte strings inline in both words. 16-byte slot, 14 per bucket.
pub type TableStr7 = Table<W8, W8, 14>;
/// Wide words both sides: ≤ 15-byte strings inline. 32-byte slot, **7 per bucket**.
pub type TableStr15 = Table<W16, W16, 7>;
/// Hybrid: narrow key word, wide value word. 24-byte slot, 9 per bucket.
pub type TableHybrid = Table<W8, W16, 9>;

/// Slot-layout variants the sweep reports, in report order.
pub const VARIANTS: [&str; 5] = ["ptr8", "int8", "str7", "str15w", "hybrid"];
