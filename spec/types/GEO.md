# FrogDB Geospatial Commands

Geospatial indexes for location-based queries. GEO commands are not a distinct data type - they operate on Sorted Sets using geohash encoding. Longitude/latitude pairs are encoded into 52-bit geohashes stored as scores, enabling efficient radius and bounding box searches via score range queries.

## Data Structure

No new `Value` variant required. GEO operations use `SortedSetValue`:

```rust
// GEO uses existing SortedSetValue
// Score = 52-bit geohash encoded as f64
// Member = location identifier (e.g., "restaurant:123")

/// Encode longitude/latitude to 52-bit geohash score
pub fn geohash_encode(longitude: f64, latitude: f64) -> f64 {
    // Interleave bits: longitude in even positions, latitude in odd
    // Returns value in range [0, 2^52) stored as f64
}

/// Decode geohash score back to longitude/latitude
pub fn geohash_decode(hash: f64) -> (f64, f64) {
    // Extract interleaved bits back to coordinates
    // Returns (longitude, latitude)
}

/// Convert geohash to base32 string representation
pub fn geohash_to_string(hash: f64, precision: u8) -> String {
    // Standard geohash alphabet: 0123456789bcdefghjkmnpqrstuvwxyz
}
```

**Geohash Properties:**
- 52 bits = ~0.6mm precision at equator
- Adjacent geohashes share common prefixes (locality)
- Enables range queries: nearby locations have similar scores
- Edge cases at +/-180 longitude and +/-85.05 latitude boundaries

**Coordinate Constraints:**
- Longitude: -180 to 180
- Latitude: -85.05112878 to 85.05112878 (Web Mercator limits)

---

## Commands

| Command | Complexity | Description |
|---------|------------|-------------|
| GEOADD | O(log N) per item | Add locations with coordinates |
| GEODIST | O(1) | Distance between two members |
| GEOHASH | O(1) per member | Get geohash strings |
| GEOPOS | O(1) per member | Get coordinates |
| GEOSEARCH | O(N+log M) | Search by radius or box |
| GEOSEARCHSTORE | O(N+log M) | Search and store results |
| GEORADIUS | O(N+log M) | Search by radius (deprecated) |
| GEORADIUSBYMEMBER | O(N+log M) | Search from member (deprecated) |

Where N = items in search area, M = sorted set size.

---

## Command Details

### GEOADD

Add locations with longitude/latitude coordinates.

```
GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member ...]
```

| Option | Meaning |
|--------|---------|
| NX | Only add new members (don't update existing) |
| XX | Only update existing members (don't add new) |
| CH | Return count of changed members (added + updated) |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: members added (or changed if CH) |
| Invalid coords | Error if outside valid ranges |
| Duplicate members | Later coordinates win |

**Examples:**
```
> GEOADD locations -122.4194 37.7749 "san_francisco"
(integer) 1

> GEOADD locations -73.9857 40.7484 "new_york" -87.6298 41.8781 "chicago"
(integer) 2

> GEOADD locations XX -122.4194 37.8000 "san_francisco"
(integer) 0

> GEOPOS locations san_francisco
1) 1) "-122.41940140724182129"
   2) "37.80000068058983634"
```

**Note:** Coordinates have ~0.6mm precision; retrieved values may differ slightly from input.

### GEODIST

Get distance between two members.

```
GEODIST key member1 member2 [M | KM | FT | MI]
```

| Unit | Description |
|------|-------------|
| M | Meters (default) |
| KM | Kilometers |
| FT | Feet |
| MI | Miles |

| Aspect | Behavior |
|--------|----------|
| Returns | Bulk string: distance, or nil if member missing |
| Formula | Haversine (spherical Earth, radius 6372.8km) |
| Precision | Up to 0.5% error (Earth not perfectly spherical) |

**Examples:**
```
> GEODIST locations san_francisco new_york KM
"4138.5350"

> GEODIST locations san_francisco chicago MI
"1859.5493"
```

### GEOHASH

Get geohash strings for members.

```
GEOHASH key member [member ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of 11-character geohash strings (or nil for missing) |
| Format | Base32 using alphabet: 0123456789bcdefghjkmnpqrstuvwxyz |
| Precision | 11 chars = 0.074m x 0.037m cell |

**Examples:**
```
> GEOHASH locations san_francisco new_york
1) "9q8yyk8yutp"
2) "dr5ru6j6zbc"
```

**Use case:** Geohash strings are compatible with external geohash services and can be used for URL-safe location identifiers.

### GEOPOS

Get coordinates for members.

```
GEOPOS key member [member ...]
```

| Aspect | Behavior |
|--------|----------|
| Returns | Array of [longitude, latitude] pairs (or nil for missing) |
| Precision | 6 decimal places (~0.11m) |

**Examples:**
```
> GEOPOS locations san_francisco chicago missing
1) 1) "-122.41940140724182129"
   2) "37.77490028375886668"
2) 1) "-87.62979656457901001"
   2) "41.87810032091501512"
3) (nil)
```

### GEOSEARCH

Search for members within radius or bounding box.

```
GEOSEARCH key
    FROMMEMBER member | FROMLONLAT longitude latitude
    BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
    [ASC | DESC]
    [COUNT count [ANY]]
    [WITHCOORD] [WITHDIST] [WITHHASH]
```

| Parameter | Description |
|-----------|-------------|
| FROMMEMBER | Search from existing member's location |
| FROMLONLAT | Search from specified coordinates |
| BYRADIUS | Circular search area |
| BYBOX | Rectangular search area (axis-aligned) |
| ASC/DESC | Sort by distance (default: unsorted) |
| COUNT | Limit results (ANY = stop early once count reached) |
| WITHCOORD | Include coordinates in results |
| WITHDIST | Include distance in results |
| WITHHASH | Include raw geohash score in results |

| Aspect | Behavior |
|--------|----------|
| Returns | Array of members (with optional data) |
| Empty result | Empty array |
| Missing center member | Error |

**Examples:**
```
> GEOSEARCH locations FROMMEMBER san_francisco BYRADIUS 500 KM ASC
1) "san_francisco"

> GEOSEARCH locations FROMLONLAT -100 40 BYRADIUS 1000 KM COUNT 3 WITHDIST
1) 1) "chicago"
   2) "940.8473"
2) 1) "new_york"
   2) "2433.1072"

> GEOSEARCH locations FROMLONLAT -100 40 BYBOX 2000 1500 KM WITHCOORD
1) 1) "chicago"
   2) 1) "-87.62979656457901001"
      2) "41.87810032091501512"
```

**Algorithm:**
1. Convert center to geohash
2. Determine geohash ranges covering search area
3. Range query sorted set by score
4. Post-filter by exact distance/bounds
5. Sort and limit results

### GEOSEARCHSTORE

Store GEOSEARCH results in destination key.

```
GEOSEARCHSTORE destination source
    FROMMEMBER member | FROMLONLAT longitude latitude
    BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
    [ASC | DESC] [COUNT count [ANY]]
    [STOREDIST]
```

| Option | Description |
|--------|-------------|
| STOREDIST | Store distances as scores instead of geohashes |

| Aspect | Behavior |
|--------|----------|
| Returns | Integer: number of members stored |
| Destination | Created as sorted set (replaces if exists) |
| STOREDIST | Enables ZRANGE by distance |

### GEORADIUS / GEORADIUSBYMEMBER (Deprecated)

Legacy commands replaced by GEOSEARCH. Supported for compatibility.

```
GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]

GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
```

---

## Cross-Shard Behavior

### Single-Key Commands

All GEO commands except GEOSEARCHSTORE with different source/dest route to single shard.

### Multi-Key Commands

| Command | Constraint |
|---------|------------|
| GEOSEARCHSTORE | Source and destination must be on same shard |

**Hash tag requirement:**
```
GEOSEARCHSTORE {locations}:nearby {locations}:all FROMLONLAT -122.4 37.8 BYRADIUS 10 KM
```

---

## Persistence

GEO operations persist like sorted set modifications:
- GEOADD triggers WAL write (stored as ZADD with geohash score)
- Standard sorted set persistence format
- Geohash scores stored as f64

---

## Implementation Notes

### Geohash Encoding

```rust
/// 52-bit geohash encoding (Redis-compatible)
pub fn geohash_encode(lon: f64, lat: f64) -> u64 {
    let lon_bits = quantize(lon, -180.0, 180.0, 26);   // 26 bits longitude
    let lat_bits = quantize(lat, -85.05112878, 85.05112878, 26); // 26 bits latitude

    interleave_bits(lon_bits, lat_bits) // lon in even bits, lat in odd
}

fn quantize(value: f64, min: f64, max: f64, bits: u32) -> u32 {
    let range = max - min;
    let normalized = (value - min) / range;
    (normalized * (1u64 << bits) as f64) as u32
}

fn interleave_bits(x: u32, y: u32) -> u64 {
    // Morton encoding / Z-order curve
    let mut result = 0u64;
    for i in 0..26 {
        result |= ((x >> i) as u64 & 1) << (2 * i);
        result |= ((y >> i) as u64 & 1) << (2 * i + 1);
    }
    result
}
```

### Search Algorithm

```
+-----------------------------------------------------+
|  GEOSEARCH Algorithm                                |
+-----------------------------------------------------+
|  1. Compute center geohash                          |
|  2. Determine search radius in geohash units        |
|  3. Find covering geohash ranges (8-9 cells)        |
|     +---+---+---+                                   |
|     | 7 | 6 | 5 |   Neighbors of center cell        |
|     +---+---+---+                                   |
|     | 0 | C | 4 |   C = center                      |
|     +---+---+---+                                   |
|     | 1 | 2 | 3 |                                   |
|     +---+---+---+                                   |
|  4. ZRANGEBYSCORE for each covering range           |
|  5. Post-filter: exact distance <= radius           |
|  6. Sort by distance if ASC/DESC                    |
|  7. Apply COUNT limit                               |
+-----------------------------------------------------+
```

### Distance Calculation (Haversine)

```rust
const EARTH_RADIUS_M: f64 = 6372797.560856;

pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}
```

### Crate Dependencies

```toml
# No additional dependencies - math uses std library
```

### Use Cases

- **Store locator**: Find nearest stores to user location
- **Delivery radius**: Check if address within delivery zone
- **Geofencing**: Monitor when devices enter/exit areas
- **Proximity matching**: Find nearby users/vehicles
- **Location-based caching**: Cache data by geographic region
