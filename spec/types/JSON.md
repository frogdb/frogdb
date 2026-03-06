# FrogDB JSON Commands

JSON document storage with path-based access. Stores, retrieves, and manipulates JSON documents using JSONPath-like syntax. Enables partial updates without rewriting entire documents and supports nested structures with type-aware operations.

Based on RedisJSON module semantics.

## Data Structure

```rust
pub struct JsonValue {
    /// Root JSON value
    root: serde_json::Value,
}

// Value enum addition:
pub enum Value {
    // ... existing variants ...
    Json(JsonValue),
}
```

**Supported JSON Types:**
- Object: `{"key": value}`
- Array: `[value, ...]`
- String: `"text"`
- Number: integer or floating-point
- Boolean: `true` / `false`
- Null: `null`

**Path Syntax (JSONPath subset):**

| Pattern | Description |
|---------|-------------|
| `$` | Root element |
| `.key` or `['key']` | Object member |
| `[0]` | Array index |
| `[*]` | All array elements |
| `..key` | Recursive descent |
| `[-1]` | Last array element |

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| JSON.SET | O(M+N) | Set value at path |
| JSON.GET | O(N) | Get value(s) at path(s) |
| JSON.DEL | O(N) | Delete value at path |
| JSON.MGET | O(M*N) | Multi-key get |
| JSON.TYPE | O(1) | Get JSON type at path |
| JSON.NUMINCRBY | O(1) | Increment number |
| JSON.NUMMULTBY | O(1) | Multiply number |
| JSON.STRAPPEND | O(M) | Append to string |
| JSON.STRLEN | O(1) | String length |
| JSON.ARRAPPEND | O(M) | Append to array |
| JSON.ARRINDEX | O(N) | Find in array |
| JSON.ARRINSERT | O(N) | Insert into array |
| JSON.ARRLEN | O(1) | Array length |
| JSON.ARRPOP | O(1) | Pop from array |
| JSON.ARRTRIM | O(N) | Trim array |
| JSON.OBJKEYS | O(N) | Object keys |
| JSON.OBJLEN | O(1) | Object key count |
| JSON.CLEAR | O(N) | Clear container |
| JSON.TOGGLE | O(1) | Toggle boolean |
| JSON.MERGE | O(N+M) | Deep merge objects |
| JSON.DEBUG | O(N) | Debug info |

Where N = path elements, M = value size.

---

## Command Details

### JSON.SET

Set JSON value at path.

```
JSON.SET key path value [NX | XX]
```

| Parameter | Description |
|-----------|-------------|
| path | JSONPath expression ($ for root) |
| value | JSON value to set |
| NX | Only set if path doesn't exist |
| XX | Only set if path exists |

| Aspect | Behavior |
|--------|----------|
| Returns | OK on success, nil if NX/XX condition fails |
| New key | Created with value at root |
| Non-existent path | Error (except for new object keys) |
| Type mismatch | Error (e.g., setting index on object) |

**Examples:**
```
> JSON.SET user $ '{"name":"alice","age":30}'
OK

> JSON.SET user $.email '"alice@example.com"'
OK

> JSON.GET user $
"[{\"name\":\"alice\",\"age\":30,\"email\":\"alice@example.com\"}]"

> JSON.SET user $.age 31
OK

> JSON.SET user $.name NX '"bob"'
(nil)
```

### JSON.GET

Get JSON value(s) at path(s).

```
JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] path [path ...]
```

| Option | Description |
|--------|-------------|
| INDENT | Indentation string for pretty-print |
| NEWLINE | Newline string for pretty-print |
| SPACE | Space after colon in objects |

| Aspect | Behavior |
|--------|----------|
| Returns | JSON array of values matching path(s) |
| Non-existent key | nil |
| Non-existent path | Empty array for that path |
| Multiple paths | Object with path keys |

**Examples:**
```
> JSON.GET user $.name
"[\"alice\"]"

> JSON.GET user $.name $.age
"{\"$.name\":[\"alice\"],\"$.age\":[30]}"

> JSON.GET user INDENT "  " NEWLINE "\n" $
"[\n  {\n    \"name\": \"alice\",\n    \"age\": 30\n  }\n]"
```

### JSON.DEL

Delete value at path.

```
JSON.DEL key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: number of paths deleted |
| No path | Deletes entire key (like DEL) |
| Root path ($) | Deletes entire key |
| Array element | Shifts subsequent elements |

**Examples:**
```
> JSON.SET obj $ '{"a":1,"b":2,"c":3}'
OK

> JSON.DEL obj $.b
(integer) 1

> JSON.GET obj $
"[{\"a\":1,\"c\":3}]"
```

### JSON.MGET

Get values from multiple keys.

```
JSON.MGET key [key ...] path
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of JSON values (nil for missing keys) |
| Path per key | Same path applied to all keys |

**Examples:**
```
> JSON.SET user:1 $ '{"name":"alice"}'
> JSON.SET user:2 $ '{"name":"bob"}'

> JSON.MGET user:1 user:2 user:3 $.name
1) "[\"alice\"]"
2) "[\"bob\"]"
3) (nil)
```

### JSON.TYPE

Get JSON type at path.

```
JSON.TYPE key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | String: object, array, string, integer, number, boolean, null |
| No path | Returns type of root |
| Non-existent | nil |

### JSON.NUMINCRBY

Increment number at path.

```
JSON.NUMINCRBY key path value
```

| Aspect | Behavior |
|--------|----------|
| Returns | New value as string |
| Non-number | Error |
| Overflow | Error (configurable) |

**Examples:**
```
> JSON.SET counter $ '{"views":100}'
OK

> JSON.NUMINCRBY counter $.views 5
"105"

> JSON.NUMINCRBY counter $.views -10
"95"
```

### JSON.NUMMULTBY

Multiply number at path.

```
JSON.NUMMULTBY key path value
```

| Aspect | Behavior |
|--------|----------|
| Returns | New value as string |
| Non-number | Error |

### JSON.STRAPPEND

Append to string at path.

```
JSON.STRAPPEND key [path] value
```

| Aspect | Behavior |
|--------|----------|
| Returns | New string length |
| Non-string | Error |

### JSON.STRLEN

Get string length at path.

```
JSON.STRLEN key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: string length |
| Non-string | Error |

### JSON.ARRAPPEND

Append values to array.

```
JSON.ARRAPPEND key path value [value ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of new lengths for each matching path |
| Non-array | Error |

**Examples:**
```
> JSON.SET arr $ '{"items":[1,2,3]}'
OK

> JSON.ARRAPPEND arr $.items 4 5
1) (integer) 5

> JSON.GET arr $.items
"[[1,2,3,4,5]]"
```

### JSON.ARRINDEX

Find value in array.

```
JSON.ARRINDEX key path value [start [stop]]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Index of first match, or -1 if not found |
| start/stop | Search range (negative = from end) |

### JSON.ARRINSERT

Insert values into array.

```
JSON.ARRINSERT key path index value [value ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of new lengths |
| Negative index | Insert from end |

### JSON.ARRLEN

Get array length.

```
JSON.ARRLEN key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: array length |
| Non-array | Error |

### JSON.ARRPOP

Pop element from array.

```
JSON.ARRPOP key [path [index]]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Popped element as JSON string |
| No index | Pops last element |
| Negative index | Pop from end |

### JSON.ARRTRIM

Trim array to range.

```
JSON.ARRTRIM key path start stop
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of new lengths |
| Out of range | Clamps to valid range |

### JSON.OBJKEYS

Get object keys.

```
JSON.OBJKEYS key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of key names |
| Non-object | Error |

**Examples:**
```
> JSON.SET obj $ '{"a":1,"b":2,"c":3}'
OK

> JSON.OBJKEYS obj $
1) 1) "a"
   2) "b"
   3) "c"
```

### JSON.OBJLEN

Get object key count.

```
JSON.OBJLEN key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: number of keys |
| Non-object | Error |

### JSON.CLEAR

Clear container (array or object).

```
JSON.CLEAR key [path]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: number of values cleared |
| Array | Becomes empty array |
| Object | Becomes empty object |

### JSON.TOGGLE

Toggle boolean value.

```
JSON.TOGGLE key path
```

| Aspect | Behavior |
|--------|----------|
| Returns | New boolean value as string |
| Non-boolean | Error |

### JSON.MERGE

Deep merge into existing document.

```
JSON.MERGE key path value
```

| Aspect | Behavior |
|--------|----------|
| Returns | OK |
| Object + Object | Recursively merge |
| null value | Deletes key |
| Other types | Replaces |

**Examples:**
```
> JSON.SET user $ '{"name":"alice","settings":{"theme":"dark"}}'
OK

> JSON.MERGE user $ '{"email":"a@b.com","settings":{"lang":"en"}}'
OK

> JSON.GET user $
"[{\"name\":\"alice\",\"email\":\"a@b.com\",\"settings\":{\"theme\":\"dark\",\"lang\":\"en\"}}]"
```

---

## Cross-Shard Behavior

### Single-Key Commands

All JSON commands except JSON.MGET route to single shard.

### Multi-Key Commands

| Command | Constraint |
|---------|------------|
| JSON.MGET | All keys must be on same shard |

**Hash tag requirement:**
```
JSON.MGET {user}:1 {user}:2 {user}:3 $.name
```

---

## Persistence

JSON values persist as serialized JSON:
- Stored as UTF-8 JSON text in RocksDB
- Compact format (no pretty-printing)
- Preserves number precision

---

## Implementation Notes

### Path Parsing

```rust
pub enum PathSegment {
    Root,                    // $
    Key(String),            // .key or ['key']
    Index(i64),             // [0] or [-1]
    Wildcard,               // [*]
    RecursiveDescent,       // ..
}

pub struct JsonPath {
    segments: Vec<PathSegment>,
}

impl JsonPath {
    pub fn parse(path: &str) -> Result<Self, PathError>;
    pub fn query(&self, value: &Value) -> Vec<&Value>;
    pub fn query_mut(&self, value: &mut Value) -> Vec<&mut Value>;
}
```

### Memory Considerations

```rust
// JSON documents can be large - implement size limits
const MAX_JSON_DEPTH: usize = 128;
const MAX_JSON_SIZE: usize = 64 * 1024 * 1024; // 64MB default

fn validate_json(value: &Value, depth: usize) -> Result<(), JsonError> {
    if depth > MAX_JSON_DEPTH {
        return Err(JsonError::TooDeep);
    }
    // Recursively validate...
}
```

### Crate Dependencies

```toml
serde_json = "1.0"
jsonpath-rust = "0.3"  # Or implement subset
```

### Use Cases

- **User profiles**: Flexible schema for user data
- **Configuration**: Application settings with nested structure
- **API responses**: Cache and query JSON from external APIs
- **Event data**: Store semi-structured events
- **Product catalogs**: Variable attributes per product
