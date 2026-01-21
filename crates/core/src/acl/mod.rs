//! ACL (Access Control List) module.
//!
//! Provides Redis-compatible authentication and authorization for FrogDB.
//!
//! # Overview
//!
//! This module implements the Redis ACL system including:
//! - User management (create, modify, delete users)
//! - Password authentication (SHA256 hashed)
//! - Command permissions (allow/deny commands and categories)
//! - Key permissions (pattern-based access control)
//! - Channel permissions (for pub/sub)
//! - Security logging
//!
//! # Architecture
//!
//! - [`AclManager`] - Central user store and authentication service
//! - [`AclChecker`] - Trait for permission checking (allows noop or full ACL)
//! - [`User`] / [`AuthenticatedUser`] - User definitions and session state
//! - [`AclLog`] - Security event logging
//!
//! # Example
//!
//! ```rust
//! use frogdb_core::acl::{AclManager, AclConfig};
//!
//! // Create manager with requirepass
//! let config = AclConfig {
//!     requirepass: Some("secret".to_string()),
//!     ..Default::default()
//! };
//! let manager = AclManager::new(config);
//!
//! // Authenticate default user
//! let user = manager.authenticate_default("secret", "127.0.0.1:12345").unwrap();
//!
//! // Create a restricted user
//! manager.set_user("reader", &["on", ">readpass", "~data:*", "+@read"]).unwrap();
//! ```

pub mod categories;
pub mod checker;
pub mod error;
pub mod log;
pub mod manager;
pub mod parser;
pub mod permissions;
pub mod user;

// Re-exports
pub use categories::CommandCategory;
pub use checker::{AclChecker, AllowAllChecker, FullAclChecker, PermissionResult};
pub use error::AclError;
pub use log::{AclLog, AclLogEntry, AclLogEntryType, AclLogValue, DEFAULT_ACL_LOG_MAX_LEN};
pub use manager::{AclConfig, AclManager};
pub use parser::{generate_password, hash_password, parse_acl_line, parse_and_apply_rules, AclRule};
pub use permissions::{
    ChannelPattern, CommandPermissions, KeyAccessType, KeyPattern, PermissionSet, SubcommandRule,
};
pub use user::{AuthenticatedUser, User, UserInfoValue, UserPermissions};
