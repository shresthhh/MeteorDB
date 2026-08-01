//! Public configuration, write-batch, error, and clock contracts for MeteorDB.
//!
//! This crate currently defines the stable vocabulary used by later storage-engine
//! components. It intentionally does not expose the internal module layout, so
//! applications can import every supported contract directly from the crate root.

#![deny(missing_docs)]

mod batch;
mod clock;
mod error;
mod options;

pub use batch::{WriteBatch, WriteOp};
pub use clock::{Clock, SystemClock};
pub use error::{Error, Result};
pub use options::{Compression, Durability, Options};
