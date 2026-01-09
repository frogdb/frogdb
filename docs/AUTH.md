# FrogDB Authentication & ACL

This document details FrogDB's authentication and Access Control List (ACL) architecture, including the abstraction design for Redis 6.0 compatibility with a path to Redis 7.0 features.

## Overview

FrogDB implements Redis-compatible authentication and authorization:

- **Authentication**: Verify client identity via passwords (AUTH command)
- **Authorization**: Control what authenticated users can do (ACL system)

The system uses an abstracted checker interface that starts as "allow all" (no enforcement) and can be enabled for full ACL support.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Connection                                                      │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  AuthState                                               │    │
│  │  • user: AuthenticatedUser (immutable permission snapshot)    │
│  │  • authenticated: bool                                   │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼ Permission checks at 3 hook points
┌─────────────────────────────────────────────────────────────────┐
│  Command Execution Flow                                          │
│                                                                  │
│  1. check_command(user, "SET", None)        ← Hook 1 (command)  │
│  2. check_key_access(user, "mykey", Write)  ← Hook 2 (keys)     │
│  3. check_channel_access(user, "ch1")       ← Hook 3 (pub/sub)  │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  AclManager (shared across all shards)                           │
│                                                                  │
│  ┌──────────────────┐    ┌─────────────────────────────────┐   │
│  │  dyn AclChecker  │    │  User Store                      │   │
│  │                  │    │  HashMap<String, Arc<User>>      │   │
│  │  • AllowAll      │    │                                  │   │
│  │    (stub/noop)   │    │  "default" → User { ... }        │   │
│  │                  │    │  "alice"   → User { ... }        │   │
│  │  • FullAcl       │    │                                  │   │
│  │    (enforcing)   │    │                                  │   │
│  └──────────────────┘    └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Abstractions

### AclChecker Trait

The permission checker is abstracted to allow swapping between stub and full implementations:

```rust
/// Result of a permission check
pub enum PermissionResult {
    Allowed,
    Denied(PermissionDenied),
}

/// Type of key access being performed
pub enum KeyAccessType {
    Read,
    Write,
    ReadWrite,
}

/// Abstract permission checker - implementation can change without affecting callers
pub trait AclChecker: Send + Sync {
    /// Check if command (and optional subcommand) is allowed
    fn check_command(
        &self,
        user: &AuthenticatedUser,
        command: &str,
        subcommand: Option<&str>,
    ) -> PermissionResult;

    /// Check if user can access a key with given access type
    fn check_key_access(
        &self,
        user: &AuthenticatedUser,
        key: &[u8],
        access_type: KeyAccessType,
    ) -> PermissionResult;

    /// Check if user can access a pub/sub channel
    fn check_channel_access(
        &self,
        user: &AuthenticatedUser,
        channel: &[u8],
    ) -> PermissionResult;
}
```

### AllowAllChecker (Stub Implementation)

Initial implementation that permits everything - zero overhead in hot path:

```rust
pub struct AllowAllChecker;

impl AclChecker for AllowAllChecker {
    #[inline(always)]
    fn check_command(&self, ..) -> PermissionResult {
        PermissionResult::Allowed
    }

    #[inline(always)]
    fn check_key_access(&self, ..) -> PermissionResult {
        PermissionResult::Allowed
    }

    #[inline(always)]
    fn check_channel_access(&self, ..) -> PermissionResult {
        PermissionResult::Allowed
    }
}
```

### AuthenticatedUser

Lightweight handle representing who is authenticated on a connection:

```rust
/// Immutable snapshot of user permissions for fast checking
pub struct AuthenticatedUser {
    pub username: Arc<str>,
    pub permissions: Arc<UserPermissions>,
}

impl AuthenticatedUser {
    /// Create the default user (used before authentication)
    pub fn default_user() -> Self {
        Self {
            username: Arc::from("default"),
            permissions: Arc::new(UserPermissions::allow_all()),
        }
    }
}
```

---

## User & Permission Structures

### User Definition

```rust
pub struct User {
    /// Username (case-sensitive)
    pub name: String,
    /// Whether the user can authenticate
    pub enabled: bool,
    /// Password hashes (SHA256) - multiple allowed
    pub password_hashes: HashSet<[u8; 32]>,
    /// Allow authentication without password
    pub nopass: bool,
    /// Root permissions (Redis 6.0 style)
    pub root_permissions: PermissionSet,
    /// Additional selectors (Redis 7.0) - empty for v6 compatibility
    pub selectors: Vec<PermissionSet>,
}
```

### UserPermissions (Immutable Snapshot)

Created when a user authenticates, not modified during connection lifetime:

```rust
pub struct UserPermissions {
    pub root: PermissionSet,
    pub selectors: Vec<PermissionSet>,  // Empty for Redis 6.0, populated for 7.0
}
```

**Redis 6.0 → 7.0 Evolution:**
- For v6: `selectors` vector is empty, only `root` permissions checked
- For v7: `selectors` populated, checker evaluates root OR any selector
- Same API, no breaking changes

### PermissionSet

```rust
pub struct PermissionSet {
    pub commands: CommandPermissions,
    pub key_patterns: Vec<KeyPattern>,
    pub channel_patterns: Vec<ChannelPattern>,
}
```

### CommandPermissions

```rust
pub struct CommandPermissions {
    /// Allow all commands by default?
    pub allow_all: bool,
    /// Explicitly allowed commands (lowercase)
    pub allowed_commands: HashSet<String>,
    /// Explicitly denied commands (lowercase)
    pub denied_commands: HashSet<String>,
    /// Allowed command categories
    pub allowed_categories: HashSet<CommandCategory>,
    /// Denied command categories
    pub denied_categories: HashSet<CommandCategory>,
    /// Subcommand rules (Redis 7.0+): "command|subcommand"
    pub subcommand_rules: Vec<SubcommandRule>,
}
```

---

## Key Pattern Support

| Syntax | Redis Version | Access Type | Example |
|--------|---------------|-------------|---------|
| `~pattern` | 6.0+ | ReadWrite | `~user:*` |
| `%R~pattern` | 7.0+ | Read only | `%R~cache:*` |
| `%W~pattern` | 7.0+ | Write only | `%W~logs:*` |

```rust
pub struct KeyPattern {
    pub pattern: GlobPattern,
    pub access_type: KeyAccessType,
}

impl KeyPattern {
    /// Parse from Redis ACL format
    pub fn parse(s: &str) -> Result<Self, AclParseError> {
        if s.starts_with("%R~") {
            Ok(Self { pattern: GlobPattern::new(&s[3..]), access_type: KeyAccessType::Read })
        } else if s.starts_with("%W~") {
            Ok(Self { pattern: GlobPattern::new(&s[3..]), access_type: KeyAccessType::Write })
        } else if s.starts_with('~') {
            Ok(Self { pattern: GlobPattern::new(&s[1..]), access_type: KeyAccessType::ReadWrite })
        } else {
            Err(AclParseError::InvalidKeyPattern)
        }
    }
}
```

---

## Command Categories

```rust
pub enum CommandCategory {
    // Access type
    Read, Write,
    // Data structures
    String, List, Set, SortedSet, Hash, Stream, Bitmap, Hyperloglog, Geo,
    // Behavior
    Fast, Slow, Blocking, Dangerous, Admin,
    // Features
    Keyspace, Pubsub, Transaction, Scripting, Connection,
}
```

Each command declares its categories for ACL matching:

| Category | Description | Example Commands |
|----------|-------------|------------------|
| `@read` | Read-only commands | GET, HGET, LRANGE |
| `@write` | Write commands | SET, HSET, LPUSH |
| `@fast` | O(1) operations | GET, SET, PING |
| `@slow` | O(N) operations | KEYS, SMEMBERS |
| `@dangerous` | Admin commands | DEBUG, CONFIG, SHUTDOWN |
| `@pubsub` | Pub/sub commands | SUBSCRIBE, PUBLISH |
| `@scripting` | Lua scripts | EVAL, EVALSHA |

---

## ACL Rule Syntax

| Syntax | Example | Description |
|--------|---------|-------------|
| `on` | `on` | Enable user |
| `off` | `off` | Disable user |
| `>password` | `>secret123` | Add password (SHA256 hashed) |
| `<password` | `<secret123` | Remove password |
| `nopass` | `nopass` | Allow passwordless auth |
| `resetpass` | `resetpass` | Clear all passwords |
| `~pattern` | `~user:*` | Allow key pattern (read+write) |
| `%R~pattern` | `%R~cache:*` | Allow key pattern (read only, v7) |
| `%W~pattern` | `%W~logs:*` | Allow key pattern (write only, v7) |
| `allkeys` | `allkeys` | Allow all keys |
| `resetkeys` | `resetkeys` | Clear key patterns |
| `&pattern` | `&notifications:*` | Allow pub/sub channel pattern |
| `allchannels` | `allchannels` | Allow all channels |
| `resetchannels` | `resetchannels` | Clear channel patterns |
| `+command` | `+get` | Allow command |
| `-command` | `-debug` | Deny command |
| `+@category` | `+@read` | Allow category |
| `-@category` | `-@dangerous` | Deny category |
| `+cmd\|sub` | `+config\|get` | Allow subcommand (v7) |
| `-cmd\|sub` | `-config\|set` | Deny subcommand (v7) |
| `allcommands` | `allcommands` | Allow all commands |
| `nocommands` | `nocommands` | Deny all commands |
| `(rules)` | `(~temp:* +@read)` | Add selector (v7) |

---

## Commands

### AUTH

Authenticate the connection:

```
AUTH password                 # Legacy: authenticate as 'default' user
AUTH username password        # Redis 6+: authenticate as named user
```

**Responses:**
- `+OK` on success
- `-WRONGPASS invalid username-password pair` on failure
- `-NOAUTH Authentication required` if required but not provided

### ACL SETUSER

Create or modify a user:

```
ACL SETUSER username [rule ...]
```

**Examples:**
```
ACL SETUSER alice on >password123 ~user:* +@read +@write -@dangerous
ACL SETUSER readonly on >readpass ~* +@read -@write
ACL SETUSER admin on >adminpass ~* +@all
```

### ACL DELUSER

Delete users:

```
ACL DELUSER username [username ...]
```

### ACL LIST

List all users with their rules:

```
ACL LIST
```

### ACL GETUSER

Get a specific user's configuration:

```
ACL GETUSER username
```

### ACL WHOAMI

Return current authenticated username:

```
ACL WHOAMI
```

### ACL CAT

List command categories or commands in a category:

```
ACL CAT                  # List all categories
ACL CAT category         # List commands in category
```

### ACL SAVE / ACL LOAD

Persist or reload ACL configuration:

```
ACL SAVE                 # Save to aclfile
ACL LOAD                 # Load from aclfile
```

---

## Hook Points in Command Execution

ACL checks integrate into the command execution flow at three points:

### Hook 1: Command Permission

**Location:** After command lookup, before key routing

```rust
// Check if command is allowed (independent of keys)
let result = acl.check_command(&user, "SET", None);
if let PermissionResult::Denied(reason) = result {
    return Err(CommandError::PermissionDenied(reason));
}
```

### Hook 2: Key Access Permission

**Location:** After key extraction, before shard dispatch

```rust
// Check each key with appropriate access type
let access_type = if command.flags().contains(WRITE) {
    KeyAccessType::Write
} else {
    KeyAccessType::Read
};

for key in command.keys(&args) {
    let result = acl.check_key_access(&user, key, access_type);
    if let PermissionResult::Denied(reason) = result {
        return Err(CommandError::PermissionDenied(reason));
    }
}
```

### Hook 3: Channel Access Permission

**Location:** For pub/sub commands (SUBSCRIBE, PUBLISH, etc.)

```rust
// Check channel access for pub/sub commands
for channel in channels {
    let result = acl.check_channel_access(&user, channel);
    if let PermissionResult::Denied(reason) = result {
        return Err(CommandError::PermissionDenied(reason));
    }
}
```

---

## Connection State

Each connection maintains authentication state:

```rust
pub struct ConnectionState {
    // ... existing fields (tx_queue, watches, subscriptions, etc.)

    /// Authentication state
    pub auth: AuthState,
}

pub struct AuthState {
    /// The authenticated user (or default)
    pub user: AuthenticatedUser,
    /// Whether explicit authentication has occurred
    pub authenticated: bool,
}
```

**Key design:** `AuthenticatedUser` contains an immutable snapshot of permissions. ACL changes don't affect existing connections until re-authentication (consistent with Redis behavior).

---

## Error Messages

Redis-compatible error responses:

| Error | Message |
|-------|---------|
| Not authenticated | `NOAUTH Authentication required.` |
| Invalid credentials | `WRONGPASS invalid username-password pair or user is disabled.` |
| Command denied | `NOPERM this user has no permissions to run the 'DEBUG' command` |
| Key denied | `NOPERM this user has no permissions to access the 'admin:config' key` |
| Channel denied | `NOPERM this user has no permissions to access the 'secret' channel` |

---

## Stub Implementation Notes

For initial implementation (ACL disabled):

1. `AllowAllChecker` is the default `AclChecker` implementation
2. `AuthState` is added to `ConnectionState` with default user
3. All three hook points exist but always return `Allowed`
4. No actual password verification occurs
5. Enable full ACL later via configuration flag

---

## Redis 7.0 Features (Future)

The abstraction supports Redis 7.0 features with minimal changes:

| Feature | Implementation | Status |
|---------|----------------|--------|
| Selectors | `selectors: Vec<PermissionSet>` in UserPermissions | Planned |
| Subcommand control | `SubcommandRule` in CommandPermissions | Planned |
| Read/Write key patterns | `KeyAccessType` enum | Ready |

**Enabling v7 features:**
```toml
[acl]
redis7_features = true
```

When enabled:
- Selector syntax `(rules)` is parsed
- Subcommand syntax `+cmd|sub` is parsed
- `%R~` and `%W~` patterns are fully enforced

---

## Shared-Nothing Considerations

Given FrogDB's thread-per-core architecture:

1. **AclManager is shared** across all shards via `Arc`
2. **User store uses RwLock** for safe concurrent access
3. **Permission snapshots are immutable** - no lock on hot path
4. **User changes don't affect existing connections** until re-auth

```rust
pub struct AclManager {
    users: Arc<RwLock<HashMap<String, Arc<User>>>>,
    checker: Arc<dyn AclChecker>,
}
```

---

## Cluster Mode

### ACL Propagation

In cluster mode, ACLs are **per-node** and not automatically synchronized. This matches
Redis Cluster behavior where each node maintains its own independent ACL.

**Recommended Approach: Orchestrator Distribution**

The orchestrator pushes identical ACL configuration to all nodes:

```
Orchestrator (source of truth)
      │
      │── POST /admin/acl { users: [...] } ──▶ Node 1 ─┐
      │── POST /admin/acl { users: [...] } ──▶ Node 2 ─┼── All nodes receive
      │── POST /admin/acl { users: [...] } ──▶ Node 3 ─┘   identical config
      │
      ▼
   ACL stored centrally
   (e.g., etcd, config file)
```

### Admin API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/acl` | POST | Apply ACL configuration (full replacement) |
| `/admin/acl` | GET | Return current ACL state |

### Consistency Model

ACL updates are **eventually consistent** across the cluster:

1. Orchestrator updates its source of truth
2. Orchestrator pushes to all nodes (sequential or parallel)
3. Brief window where nodes have different ACL states
4. All nodes converge to identical config

**Timing considerations:**
- Push in parallel for minimal divergence window
- Consider rolling updates during maintenance windows
- Monitor for failed pushes (node may have stale ACL)

### Client Behavior During ACL Update

| Scenario | Behavior |
|----------|----------|
| Client connected to updated node | New permissions on re-auth |
| Client connected to stale node | Old permissions until node updated |
| Client reconnects to different node | May see different permissions briefly |

**Important:** ACL changes don't affect existing connections. Clients continue with
their permission snapshot until they re-authenticate or reconnect.

### Alternative Approaches (Not Recommended)

| Approach | Why Not Recommended |
|----------|---------------------|
| ACL SAVE + shared file | Requires shared filesystem, complex coordination |
| ACL commands via gossip | FrogDB uses orchestrated model, no gossip |
| Real-time replication | Complexity, eventual consistency is acceptable |

### Configuration Example

Orchestrator stores ACL config and pushes to all nodes:

```json
{
  "version": 2,
  "timestamp": "2024-01-15T10:30:00Z",
  "users": [
    {
      "name": "default",
      "enabled": true,
      "passwords": ["sha256:e3b0c44298fc1c149afbf4c8996fb924..."],
      "permissions": {
        "commands": ["+@all", "-@dangerous"],
        "keys": ["*"],
        "channels": ["*"]
      }
    },
    {
      "name": "app_readonly",
      "enabled": true,
      "passwords": ["sha256:..."],
      "permissions": {
        "commands": ["+@read", "-@write"],
        "keys": ["app:*"],
        "channels": []
      }
    }
  ]
}
```

**Version and timestamp** allow nodes to detect stale configurations and orchestrator
to track which version each node has applied.

See [CLUSTER.md](CLUSTER.md#acl-in-cluster-mode) for additional cluster ACL details.

---

## Transport Security (TLS)

For encrypted connections, see [OPERATIONS.md TLS configuration](OPERATIONS.md#tls-future).

TLS provides:
- Encrypted client-server communication
- Optional client certificate authentication
- Protection against eavesdropping and MITM attacks

**Note:** TLS is orthogonal to ACL - both can be enabled independently.
