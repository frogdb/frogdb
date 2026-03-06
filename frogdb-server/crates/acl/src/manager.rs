//! ACL Manager - centralized user store.
//!
//! The AclManager is responsible for:
//! - Storing and managing users
//! - Authenticating users
//! - Loading/saving ACL files
//! - Managing the ACL log

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use super::error::AclError;
use super::log::{AclLog, DEFAULT_ACL_LOG_MAX_LEN};
use super::parser::{AclRule, hash_password, parse_acl_line};
use super::user::{AuthenticatedUser, User, UserPermissions};
use frogdb_types::sync::RwLockExt;

/// Configuration for the ACL manager.
#[derive(Debug, Clone)]
pub struct AclConfig {
    /// Path to the ACL file for SAVE/LOAD.
    pub aclfile: Option<PathBuf>,
    /// Maximum entries in the ACL log.
    pub log_max_len: usize,
    /// Legacy password for the default user (requirepass).
    pub requirepass: Option<String>,
}

impl Default for AclConfig {
    fn default() -> Self {
        Self {
            aclfile: None,
            log_max_len: DEFAULT_ACL_LOG_MAX_LEN,
            requirepass: None,
        }
    }
}

impl AclConfig {
    /// Create a new config with defaults.
    pub fn new() -> Self {
        Self::default()
    }
}

/// The centralized ACL manager.
pub struct AclManager {
    /// User store (username -> user).
    users: RwLock<HashMap<String, User>>,
    /// ACL security log.
    log: AclLog,
    /// Path to ACL file.
    aclfile: Option<PathBuf>,
    /// Whether authentication is required.
    requires_auth: bool,
}

impl AclManager {
    /// Create a new ACL manager with the given configuration.
    pub fn new(config: AclConfig) -> Arc<Self> {
        let mut users = HashMap::new();

        // Create default user
        let default_user = if let Some(ref password) = config.requirepass {
            if password.is_empty() {
                User::default_user()
            } else {
                User::default_with_password(hash_password(password))
            }
        } else {
            User::default_user()
        };

        let requires_auth = !default_user.nopass;
        users.insert("default".to_string(), default_user);

        Arc::new(Self {
            users: RwLock::new(users),
            log: AclLog::new(config.log_max_len),
            aclfile: config.aclfile,
            requires_auth,
        })
    }

    /// Create a new ACL manager with default settings.
    pub fn default_manager() -> Arc<Self> {
        Self::new(AclConfig::new())
    }

    /// Check if authentication is required.
    pub fn requires_auth(&self) -> bool {
        self.requires_auth
    }

    /// Get the ACL log.
    pub fn log(&self) -> &AclLog {
        &self.log
    }

    /// Authenticate a user.
    ///
    /// Returns an AuthenticatedUser if successful, or an error if authentication fails.
    pub fn authenticate(
        &self,
        username: &str,
        password: &str,
        client_info: &str,
    ) -> Result<AuthenticatedUser, AclError> {
        let users = self.users.try_read_err()?;

        let user = users.get(username).ok_or_else(|| {
            self.log.log_auth_failure(username, client_info);
            AclError::WrongPassword
        })?;

        if !user.enabled {
            self.log.log_auth_failure(username, client_info);
            return Err(AclError::UserDisabled);
        }

        let password_hash = hash_password(password);
        if !user.verify_password(&password_hash) {
            self.log.log_auth_failure(username, client_info);
            return Err(AclError::WrongPassword);
        }

        Ok(AuthenticatedUser::new(
            username.to_string(),
            UserPermissions::from_user(user),
        ))
    }

    /// Authenticate with the default user (legacy AUTH command with just password).
    pub fn authenticate_default(
        &self,
        password: &str,
        client_info: &str,
    ) -> Result<AuthenticatedUser, AclError> {
        self.authenticate("default", password, client_info)
    }

    /// Set or create a user with the given rules.
    pub fn set_user(&self, username: &str, rules: &[&str]) -> Result<(), AclError> {
        let mut users = self.users.try_write_err()?;

        let user = users
            .entry(username.to_string())
            .or_insert_with(|| User::new(username));

        for rule_str in rules {
            let rule = AclRule::parse(rule_str)?;
            rule.apply(user);
        }

        Ok(())
    }

    /// Delete a user.
    pub fn delete_user(&self, username: &str) -> Result<(), AclError> {
        if username == "default" {
            return Err(AclError::CannotDeleteDefaultUser);
        }

        let mut users = self.users.try_write_err()?;
        users.remove(username).ok_or(AclError::UserNotFound {
            username: username.to_string(),
        })?;

        Ok(())
    }

    /// Delete multiple users.
    pub fn delete_users(&self, usernames: &[&str]) -> Result<u64, AclError> {
        let mut count = 0;

        for username in usernames {
            if *username == "default" {
                return Err(AclError::CannotDeleteDefaultUser);
            }
        }

        let mut users = self.users.try_write_err()?;
        for username in usernames {
            if users.remove(*username).is_some() {
                count += 1;
            }
        }

        Ok(count)
    }

    /// List all usernames.
    pub fn list_users(&self) -> Vec<String> {
        let users = self.users.read_or_panic("AclManager::list_users");
        users.keys().cloned().collect()
    }

    /// Get user details (for ACL LIST).
    pub fn list_users_detailed(&self) -> Vec<String> {
        let users = self.users.read_or_panic("AclManager::list_users_detailed");
        users.values().map(|u| u.to_acl_string()).collect()
    }

    /// Get a specific user's info (for ACL GETUSER).
    pub fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read_or_panic("AclManager::get_user");
        users.get(username).cloned()
    }

    /// Check if a user exists.
    pub fn user_exists(&self, username: &str) -> bool {
        let users = self.users.read_or_panic("AclManager::user_exists");
        users.contains_key(username)
    }

    /// Save ACL to file.
    pub fn save(&self) -> Result<(), AclError> {
        let aclfile = self.aclfile.as_ref().ok_or(AclError::NoAclFile)?;

        let users = self.users.try_read_err()?;
        let mut content = String::new();

        for user in users.values() {
            content.push_str(&user.to_acl_string());
            content.push('\n');
        }

        let mut file = File::create(aclfile).map_err(|e| AclError::FileError {
            message: format!("Cannot create file: {}", e),
        })?;

        file.write_all(content.as_bytes())
            .map_err(|e| AclError::FileError {
                message: format!("Cannot write file: {}", e),
            })?;

        Ok(())
    }

    /// Load ACL from file.
    pub fn load(&self) -> Result<(), AclError> {
        let aclfile = self.aclfile.as_ref().ok_or(AclError::NoAclFile)?;

        let file = File::open(aclfile).map_err(|e| AclError::FileError {
            message: format!("Cannot open file: {}", e),
        })?;

        let reader = BufReader::new(file);
        let mut new_users = HashMap::new();

        for (line_num, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| AclError::FileError {
                message: format!("Error reading line {}: {}", line_num + 1, e),
            })?;

            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let (username, rules) = parse_acl_line(line).map_err(|_| AclError::FileError {
                message: format!("Parse error on line {}: {}", line_num + 1, line),
            })?;

            let mut user = User::new(&username);
            for rule in rules {
                rule.apply(&mut user);
            }
            new_users.insert(username, user);
        }

        // Ensure default user exists
        if !new_users.contains_key("default") {
            new_users.insert("default".to_string(), User::default_user());
        }

        // Replace users atomically
        let mut users = self.users.try_write_err()?;
        *users = new_users;

        Ok(())
    }

    /// Get the path to the ACL file.
    pub fn aclfile(&self) -> Option<&PathBuf> {
        self.aclfile.as_ref()
    }
}

impl std::fmt::Debug for AclManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AclManager")
            .field("requires_auth", &self.requires_auth)
            .field("aclfile", &self.aclfile)
            .field(
                "users_count",
                &self.users.read_or_panic("AclManager::fmt").len(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_manager() {
        let manager = AclManager::default_manager();
        assert!(!manager.requires_auth());
        assert!(manager.user_exists("default"));
    }

    #[test]
    fn test_manager_with_requirepass() {
        let config = AclConfig {
            requirepass: Some("secret".to_string()),
            ..Default::default()
        };
        let manager = AclManager::new(config);
        assert!(manager.requires_auth());
    }

    #[test]
    fn test_authenticate_default_user_nopass() {
        let manager = AclManager::default_manager();
        let result = manager.authenticate_default("anything", "127.0.0.1:12345");
        assert!(result.is_ok());
        assert_eq!(&*result.unwrap().username, "default");
    }

    #[test]
    fn test_authenticate_with_requirepass() {
        let config = AclConfig {
            requirepass: Some("secret".to_string()),
            ..Default::default()
        };
        let manager = AclManager::new(config);

        // Wrong password
        let result = manager.authenticate_default("wrong", "127.0.0.1:12345");
        assert!(matches!(result, Err(AclError::WrongPassword)));

        // Correct password
        let result = manager.authenticate_default("secret", "127.0.0.1:12345");
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_user() {
        let manager = AclManager::default_manager();

        // Create new user
        manager
            .set_user("alice", &["on", ">password", "~user:*", "+@read"])
            .unwrap();

        assert!(manager.user_exists("alice"));

        // Authenticate
        let result = manager.authenticate("alice", "password", "127.0.0.1:12345");
        assert!(result.is_ok());

        // Wrong password
        let result = manager.authenticate("alice", "wrong", "127.0.0.1:12345");
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_user() {
        let manager = AclManager::default_manager();
        manager.set_user("alice", &["on", "nopass"]).unwrap();

        // Delete alice
        manager.delete_user("alice").unwrap();
        assert!(!manager.user_exists("alice"));

        // Can't delete default
        let result = manager.delete_user("default");
        assert!(matches!(result, Err(AclError::CannotDeleteDefaultUser)));

        // Can't delete non-existent
        let result = manager.delete_user("nonexistent");
        assert!(matches!(result, Err(AclError::UserNotFound { .. })));
    }

    #[test]
    fn test_delete_users() {
        let manager = AclManager::default_manager();
        manager.set_user("alice", &["on"]).unwrap();
        manager.set_user("bob", &["on"]).unwrap();
        manager.set_user("charlie", &["on"]).unwrap();

        // Delete multiple
        let count = manager
            .delete_users(&["alice", "bob", "nonexistent"])
            .unwrap();
        assert_eq!(count, 2);
        assert!(!manager.user_exists("alice"));
        assert!(!manager.user_exists("bob"));
        assert!(manager.user_exists("charlie"));

        // Can't delete default
        let result = manager.delete_users(&["charlie", "default"]);
        assert!(matches!(result, Err(AclError::CannotDeleteDefaultUser)));
    }

    #[test]
    fn test_list_users() {
        let manager = AclManager::default_manager();
        manager.set_user("alice", &["on"]).unwrap();
        manager.set_user("bob", &["on"]).unwrap();

        let users = manager.list_users();
        assert_eq!(users.len(), 3); // default, alice, bob
        assert!(users.contains(&"default".to_string()));
        assert!(users.contains(&"alice".to_string()));
        assert!(users.contains(&"bob".to_string()));
    }

    #[test]
    fn test_list_users_detailed() {
        let manager = AclManager::default_manager();

        let acl_strings = manager.list_users_detailed();
        assert!(!acl_strings.is_empty());
        assert!(acl_strings[0].contains("user"));
    }

    #[test]
    fn test_get_user() {
        let manager = AclManager::default_manager();
        manager.set_user("alice", &["on", ">password"]).unwrap();

        let user = manager.get_user("alice");
        assert!(user.is_some());
        let user = user.unwrap();
        assert_eq!(user.name, "alice");
        assert!(user.enabled);

        let none = manager.get_user("nonexistent");
        assert!(none.is_none());
    }

    #[test]
    fn test_disabled_user() {
        let manager = AclManager::default_manager();
        manager.set_user("alice", &["off", ">password"]).unwrap();

        // Can't authenticate disabled user
        let result = manager.authenticate("alice", "password", "127.0.0.1:12345");
        assert!(matches!(result, Err(AclError::UserDisabled)));
    }

    #[test]
    fn test_acl_log() {
        let manager = AclManager::default_manager();

        // Failed authentication should be logged
        let _ = manager.authenticate("nonexistent", "pass", "127.0.0.1:12345");
        assert_eq!(manager.log().len(), 1);

        let entries = manager.log().get(None);
        assert_eq!(entries[0].username, "nonexistent");
    }

    #[test]
    fn test_save_load() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        // Create manager with aclfile
        let config = AclConfig {
            aclfile: Some(path.clone()),
            ..Default::default()
        };
        let manager = AclManager::new(config);

        // Add some users
        manager
            .set_user("alice", &["on", ">password", "~user:*", "+@read"])
            .unwrap();
        manager
            .set_user("bob", &["on", "nopass", "~*", "+@all"])
            .unwrap();

        // Save
        manager.save().unwrap();

        // Create new manager and load
        let config2 = AclConfig {
            aclfile: Some(path),
            ..Default::default()
        };
        let manager2 = AclManager::new(config2);
        manager2.load().unwrap();

        // Verify users
        assert!(manager2.user_exists("alice"));
        assert!(manager2.user_exists("bob"));
        assert!(manager2.user_exists("default"));

        // Verify alice can authenticate
        let result = manager2.authenticate("alice", "password", "127.0.0.1:12345");
        assert!(result.is_ok());
    }

    #[test]
    fn test_no_aclfile() {
        let manager = AclManager::default_manager();
        let result = manager.save();
        assert!(matches!(result, Err(AclError::NoAclFile)));

        let result = manager.load();
        assert!(matches!(result, Err(AclError::NoAclFile)));
    }

    #[test]
    fn test_load_preserves_default_user() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        // Write a file without default user
        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "user alice on nopass ~* +@all").unwrap();
        }

        // Load should still have default user
        let config = AclConfig {
            aclfile: Some(path),
            ..Default::default()
        };
        let manager = AclManager::new(config);
        manager.load().unwrap();

        assert!(manager.user_exists("default"));
        assert!(manager.user_exists("alice"));
    }
}
