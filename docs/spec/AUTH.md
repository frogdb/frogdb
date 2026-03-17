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
│  │    (when ACL     │    │  "default" → User { ... }        │   │
│  │     disabled)    │    │                                  │   │
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

### AllowAllChecker (Default / ACL Disabled)

Default implementation that permits everything when ACL is not configured - zero overhead in hot path:

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

**Multiple Passwords:** Users can have multiple valid passwords simultaneously. This supports use cases like:
- Password rotation without downtime (add new password, then remove old)
- Different credentials for different services accessing the same user
- Emergency backup passwords

Add passwords with `>password` or `#hash`, remove with `<password` or `!hash`. Any stored password is valid for authentication.

```rust
// Example: User with two valid passwords
// ACL SETUSER app on >password1 >password2 ~* +@all
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
| `>password` | `>secret123` | Add password (stored as SHA256 hash) |
| `<password` | `<secret123` | Remove password |
| `#hash` | `#a1b2c3...` | Add pre-hashed password (64-char hex SHA256) |
| `!hash` | `!a1b2c3...` | Remove specific hashed password |
| `nopass` | `nopass` | Allow passwordless auth |
| `resetpass` | `resetpass` | Clear all passwords |
| `reset` | `reset` | Reset user to default (off, no passwords, no perms) |
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
| `clearselectors` | `clearselectors` | Clear all selectors (v7) |
| `ratelimit:cps=N` | `ratelimit:cps=1000` | Limit to N commands per second |
| `ratelimit:bps=N` | `ratelimit:bps=1048576` | Limit to N bytes per second |
| `resetratelimit` | `resetratelimit` | Clear all rate limits |

---

## Per-User Rate Limiting

FrogDB extends the ACL rule syntax with per-user rate limiting using a token bucket algorithm. Rate limits are configured as ACL rules and shared across all connections authenticated as the same user.

### Rule Syntax

| Rule | Description |
|------|-------------|
| `ratelimit:cps=N` | Limit to N commands per second |
| `ratelimit:bps=N` | Limit to N bytes per second (raw command bytes) |
| `resetratelimit` | Clear all rate limits for the user |

Either or both limits can be set independently on a user. When no rate limit is configured, there is zero overhead (the check is a single `Option<Arc<RateLimitState>>` branch).

### Examples

```
# Rate-limited application user: 500 cmd/s, 512 KB/s
ACL SETUSER app on >apppass ~app:* +@read +@write ratelimit:cps=500 ratelimit:bps=524288

# Add a bytes-per-second limit to an existing user
ACL SETUSER app ratelimit:bps=1048576

# Remove all rate limits
ACL SETUSER app resetratelimit
```

### Behavior

- **Token bucket:** Each limit maintains a bucket with 1-second burst capacity (not configurable). Tokens refill continuously at the configured rate.
- **Shared state:** All connections for the same ACL user share the same token buckets.
- **Exempt commands:** `AUTH`, `HELLO`, `PING`, `QUIT`, and `RESET` are never rate-limited.
- **Admin port bypass:** Connections on the admin port are exempt from rate limits.
- **MULTI/EXEC:** Queued commands do not consume tokens individually. At `EXEC` time, the batch of N commands plus total bytes are checked atomically. If the limit would be exceeded, the entire transaction is rejected.
- **Error format:** `-ERR rate limit exceeded: commands per second` or `-ERR rate limit exceeded: bytes per second`.

### Visibility

Rate limit configuration appears in:
- `ACL LIST` output (as `ratelimit:cps=N` and/or `ratelimit:bps=N` in the rule string)
- `ACL GETUSER` response (rate limit fields)
- `INFO` output (dedicated `# Ratelimit` section with per-user counters)

See [CONNECTION.md](CONNECTION.md#per-acl-user-rate-limiting) for connection-layer enforcement details.

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

**Response format (array of field-value pairs):**

| Field | Type | Description |
|-------|------|-------------|
| `flags` | Array | User flags: `on`/`off`, `nopass`, etc. |
| `passwords` | Array | SHA256 password hashes (hex strings) |
| `commands` | String | Command permissions in rule format |
| `keys` | Array | Key patterns (e.g., `~app:*`) |
| `channels` | Array | Channel patterns (e.g., `&notifications:*`) |
| `selectors` | Array | Selector rules (Redis 7.0+, each as rule string) |

**Example response:**
```
1) "flags"
2) 1) "on"
3) "passwords"
4) 1) "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
5) "commands"
6) "+@read +@write -@dangerous"
7) "keys"
8) 1) "~app:*"
9) "channels"
10) 1) "&*"
11) "selectors"
12) (empty array)
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

### ACL USERS

List all configured usernames:

```
ACL USERS
```

Returns array of usernames. Simpler than `ACL LIST` when only names are needed.

### ACL GENPASS

Generate a secure random password:

```
ACL GENPASS              # Generate 256-bit (64 hex chars)
ACL GENPASS bits         # Generate specified bits (1-1024, rounded to multiple of 4)
```

Use for creating strong passwords to prevent brute-force attacks. Passwords generated this way are cryptographically random.

### ACL LOG

View recent ACL security events:

```
ACL LOG [count]          # Show last N entries (default: 10)
ACL LOG RESET            # Clear the log
```

Logs authentication failures and permission denials. Each entry includes:
- Timestamp
- Username (or attempted username)
- Client address
- Reason (auth failure, command denied, key denied, channel denied)
- Context (command attempted, key accessed, etc.)

Useful for security auditing and debugging permission issues.

---

## ACL File Format

The ACL file uses Redis-compatible format, one user per line:

```
user default on nopass ~* &* +@all
user alice on #e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 ~app:* +@read +@write -@dangerous
user readonly on >plainpass ~* +@read -@write
```

**Format:** `user <username> <rules...>`

**Notes:**
- Passwords should use hashed form (`#<sha256>`) in files for security
- Plaintext passwords (`>password`) work but are discouraged in persistent files
- File is loaded at startup via `--aclfile` option
- Runtime changes via `ACL SAVE` overwrite the file
- Comments start with `#` at the beginning of a line
- Empty lines are ignored

---

## Configuration

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `--requirepass` | "" | Legacy password for default user |
| `--aclfile` | "" | Path to ACL file for SAVE/LOAD |
| `--acllog-max-len` | 128 | Maximum ACL LOG entries retained |

### Config File Settings

```toml
[security]
requirepass = ""           # Legacy password for default user
aclfile = ""               # Path to ACL file

[acl]
log_max_len = 128          # Maximum ACL LOG entries
redis7_features = false    # Enable Redis 7.0 ACL features
```

### Inline User Configuration

Users can be defined directly in config file:

```toml
[[acl.users]]
name = "app"
rules = "on >password ~app:* +@read +@write"

[[acl.users]]
name = "admin"
rules = "on >adminpass ~* +@all"
```

**Priority:** Inline users are loaded first, then `aclfile` is applied. Users defined in both are merged (aclfile rules take precedence).

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

## Implementation Notes

Full ACL is implemented in `crates/acl/` (manager, checker, parser, user store, all ACL commands).

When ACL is not configured:

1. `AllowAllChecker` is the default `AclChecker` implementation (zero overhead in hot path)
2. `AuthState` is added to `ConnectionState` with default user
3. All three hook points exist but always return `Allowed`

When ACL is enabled (via `--requirepass`, `--aclfile`, or inline user config):

1. Full permission enforcement at all three hook points
2. Password verification via SHA256 hashing
3. All ACL commands available (SETUSER, GETUSER, DELUSER, LIST, etc.)

---

## Redis 7.0 Features (Future)

The abstraction supports Redis 7.0 features with minimal changes:

| Feature | Implementation |
|---------|----------------|
| Selectors | `selectors: Vec<PermissionSet>` in UserPermissions |
| Subcommand control | `SubcommandRule` in CommandPermissions |
| Read/Write key patterns | `KeyAccessType` enum |

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

## Compatibility Notes

### Redis/Valkey Compatibility

FrogDB targets full Redis 7.0 ACL compatibility. All standard ACL commands and rules are supported, including:
- All ACL commands (SETUSER, GETUSER, DELUSER, LIST, USERS, WHOAMI, CAT, LOG, GENPASS, SAVE, LOAD)
- Full rule syntax including Redis 7.0 features (selectors, subcommand ACLs, read/write key patterns)
- SHA256 password hashing
- Command categories and permission enforcement

### DragonflyDB Differences

FrogDB follows Redis behavior where it differs from DragonflyDB:

| Behavior | FrogDB (Redis) | DragonflyDB |
|----------|----------------|-------------|
| Permission propagation | Snapshot at auth time | Immediate to active connections |
| Subcommand ACLs | Supported (`+config\|get`) | Not supported |
| Key patterns in ACL files | Supported | Not supported |
| Channel patterns in ACL files | Supported | Not supported |

**Permission Propagation:** When ACL rules change via `ACL SETUSER`, FrogDB (like Redis) does not update active connections. Users continue with their permission snapshot until they re-authenticate. This is intentional for:
- **Consistency:** Commands in progress aren't affected mid-execution
- **Performance:** No lock overhead on the hot path
- **Predictability:** Clients know their permissions won't change unexpectedly

To revoke a user's access immediately, use `CLIENT KILL USER <username>` to terminate their connections.

---

## Transport Security (TLS)

For encrypted connections, see [TLS.md](TLS.md).

TLS provides:
- Encrypted client-server communication
- Optional client certificate authentication
- Protection against eavesdropping and MITM attacks

**Note:** TLS is orthogonal to ACL - both can be enabled independently.
