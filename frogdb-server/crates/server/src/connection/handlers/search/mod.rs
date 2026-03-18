//! Search scatter-gather command handlers (FT.*, ES.ALL).
//!
//! Split by command family:
//! - [`create`] - FT.CREATE handler
//! - [`search`] - FT.SEARCH handler
//! - [`aggregate`] - FT.AGGREGATE handler
//! - [`hybrid`] - FT.HYBRID handler
//! - [`cursor`] - FT.CURSOR READ/DEL
//! - [`index_mgmt`] - FT.DROPINDEX, FT.INFO, FT.LIST, FT.ALTER
//! - [`aliases`] - FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE
//! - [`dict`] - FT.DICTADD, FT.DICTDEL, FT.DICTDUMP
//! - [`config`] - FT.CONFIG SET/GET
//! - [`tagvals`] - FT.TAGVALS
//! - [`spellcheck`] - FT.SPELLCHECK
//! - [`explain`] - FT.EXPLAIN, FT.EXPLAINCLI
//! - [`profile`] - FT.PROFILE
//! - [`synonyms`] - FT.SYNUPDATE, FT.SYNDUMP
//! - [`es`] - ES.ALL
//! - [`helpers`] - Common scatter-gather helper methods

mod aggregate;
mod aliases;
mod config;
mod create;
mod cursor;
mod dict;
mod es;
mod explain;
mod helpers;
mod hybrid;
mod index_mgmt;
mod profile;
mod query;
mod spellcheck;
mod synonyms;
mod tagvals;
