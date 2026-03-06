//! Geospatial operations.
//!
//! Provides geohash encoding/decoding and distance calculations for geo commands.
//! Uses 52-bit geohash precision (~0.6mm) stored as sorted set scores.

use std::f64::consts::PI;

/// Earth's radius in meters (used for Haversine calculation).
pub const EARTH_RADIUS_M: f64 = 6372797.560856;

/// Minimum/maximum valid latitude for geohash encoding.
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;

/// Minimum/maximum valid longitude for geohash encoding.
pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;

/// Number of bits for geohash precision (26 bits for each coordinate = 52 total).
pub const GEOHASH_BITS: u32 = 26;

/// Distance unit for geo calculations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceUnit {
    /// Meters (default).
    #[default]
    M,
    /// Kilometers.
    Km,
    /// Miles.
    Mi,
    /// Feet.
    Ft,
}

impl DistanceUnit {
    /// Parse a distance unit from bytes.
    pub fn parse(s: &[u8]) -> Option<Self> {
        let upper: Vec<u8> = s.iter().map(|b| b.to_ascii_uppercase()).collect();
        match upper.as_slice() {
            b"M" => Some(DistanceUnit::M),
            b"KM" => Some(DistanceUnit::Km),
            b"MI" => Some(DistanceUnit::Mi),
            b"FT" => Some(DistanceUnit::Ft),
            _ => None,
        }
    }

    /// Convert meters to this unit.
    pub fn from_meters(&self, meters: f64) -> f64 {
        match self {
            DistanceUnit::M => meters,
            DistanceUnit::Km => meters / 1000.0,
            DistanceUnit::Mi => meters / 1609.34,
            DistanceUnit::Ft => meters * 3.28084,
        }
    }

    /// Convert this unit to meters.
    pub fn to_meters(&self, value: f64) -> f64 {
        match self {
            DistanceUnit::M => value,
            DistanceUnit::Km => value * 1000.0,
            DistanceUnit::Mi => value * 1609.34,
            DistanceUnit::Ft => value / 3.28084,
        }
    }
}

/// Coordinates (longitude, latitude).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinates {
    pub lon: f64,
    pub lat: f64,
}

impl Coordinates {
    /// Create new coordinates.
    pub fn new(lon: f64, lat: f64) -> Option<Self> {
        if !(LON_MIN..=LON_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
            return None;
        }
        Some(Self { lon, lat })
    }
}

/// A bounding box for geo searches.
#[derive(Debug, Clone, Copy)]
pub struct BoundingBox {
    pub min_lon: f64,
    pub max_lon: f64,
    pub min_lat: f64,
    pub max_lat: f64,
}

impl BoundingBox {
    /// Create a bounding box from center point and radius (in meters).
    ///
    /// Matches Redis's `geohashBoundingBox`: uses the **pole-side** latitude
    /// (lat + lat_delta for north, lat - lat_delta for south) when computing the
    /// longitude delta, since the pole side has the smallest cosine and thus the
    /// widest angular span for a given linear distance.
    pub fn from_radius(center: Coordinates, radius_m: f64) -> Self {
        let lat_delta = radius_m / EARTH_RADIUS_M * (180.0 / PI);
        let min_lat = (center.lat - lat_delta).max(LAT_MIN);
        let max_lat = (center.lat + lat_delta).min(LAT_MAX);

        // Use pole-side latitude to get the widest longitude extent.
        let pole_lat = if center.lat >= 0.0 {
            center.lat + lat_delta // north hemisphere: top edge is closer to pole
        } else {
            center.lat - lat_delta // south hemisphere: bottom edge is closer to pole
        };
        let cos_pole = (pole_lat * PI / 180.0).cos();

        let (min_lon, max_lon) = if cos_pole < 1e-12 {
            (LON_MIN, LON_MAX)
        } else {
            let lon_delta = lat_delta / cos_pole;
            if lon_delta >= 180.0 {
                (LON_MIN, LON_MAX)
            } else {
                let raw_min = center.lon - lon_delta;
                let raw_max = center.lon + lon_delta;
                // Wrap across the antimeridian instead of clamping.
                // When min > max, contains() will use OR logic.
                let min_lon = if raw_min < LON_MIN {
                    raw_min + 360.0
                } else {
                    raw_min
                };
                let max_lon = if raw_max > LON_MAX {
                    raw_max - 360.0
                } else {
                    raw_max
                };
                (min_lon, max_lon)
            }
        };

        Self {
            min_lon,
            max_lon,
            min_lat,
            max_lat,
        }
    }

    /// Create a bounding box from center point and width/height (in meters).
    ///
    /// Uses the same pole-side latitude convention as `from_radius`.
    pub fn from_box(center: Coordinates, width_m: f64, height_m: f64) -> Self {
        let lat_delta = (height_m / 2.0) / EARTH_RADIUS_M * (180.0 / PI);
        let min_lat = (center.lat - lat_delta).max(LAT_MIN);
        let max_lat = (center.lat + lat_delta).min(LAT_MAX);

        let pole_lat = if center.lat >= 0.0 {
            center.lat + lat_delta
        } else {
            center.lat - lat_delta
        };
        let cos_pole = (pole_lat * PI / 180.0).cos();

        let (min_lon, max_lon) = if cos_pole < 1e-12 {
            (LON_MIN, LON_MAX)
        } else {
            let lon_delta = (width_m / 2.0) / EARTH_RADIUS_M * (180.0 / PI) / cos_pole;
            if lon_delta >= 180.0 {
                (LON_MIN, LON_MAX)
            } else {
                let raw_min = center.lon - lon_delta;
                let raw_max = center.lon + lon_delta;
                let min_lon = if raw_min < LON_MIN {
                    raw_min + 360.0
                } else {
                    raw_min
                };
                let max_lon = if raw_max > LON_MAX {
                    raw_max - 360.0
                } else {
                    raw_max
                };
                (min_lon, max_lon)
            }
        };

        Self {
            min_lon,
            max_lon,
            min_lat,
            max_lat,
        }
    }

    /// Check if coordinates are within this bounding box.
    pub fn contains(&self, coords: Coordinates) -> bool {
        let lon_in = if self.min_lon <= self.max_lon {
            coords.lon >= self.min_lon && coords.lon <= self.max_lon
        } else {
            // Antimeridian crossing: the box wraps around +/-180
            coords.lon >= self.min_lon || coords.lon <= self.max_lon
        };
        lon_in && coords.lat >= self.min_lat && coords.lat <= self.max_lat
    }
}

/// Encode longitude and latitude into a 52-bit geohash.
///
/// Uses interleaved bits: 26 bits for longitude, 26 bits for latitude.
/// The geohash is stored as a u64 (only lower 52 bits used).
pub fn geohash_encode(lon: f64, lat: f64) -> u64 {
    // Clamp to valid range
    let lon = lon.clamp(LON_MIN, LON_MAX);
    let lat = lat.clamp(LAT_MIN, LAT_MAX);

    // Normalize to 0..1 range
    let lon_norm = (lon - LON_MIN) / (LON_MAX - LON_MIN);
    let lat_norm = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN);

    // Scale to 26-bit integers (clamp to avoid overflow at boundaries)
    let max_val = (1u64 << GEOHASH_BITS) - 1;
    let lon_int = ((lon_norm * (1u64 << GEOHASH_BITS) as f64) as u64).min(max_val);
    let lat_int = ((lat_norm * (1u64 << GEOHASH_BITS) as f64) as u64).min(max_val);

    // Interleave bits: longitude in even positions, latitude in odd
    let mut hash = 0u64;
    for i in 0..GEOHASH_BITS {
        // Longitude bit at even position
        hash |= ((lon_int >> (GEOHASH_BITS - 1 - i)) & 1) << (51 - i * 2);
        // Latitude bit at odd position
        hash |= ((lat_int >> (GEOHASH_BITS - 1 - i)) & 1) << (50 - i * 2);
    }

    hash
}

/// Decode a 52-bit geohash to longitude and latitude.
///
/// Returns (longitude, latitude).
pub fn geohash_decode(hash: u64) -> (f64, f64) {
    let mut lon_int = 0u64;
    let mut lat_int = 0u64;

    // De-interleave bits
    for i in 0..GEOHASH_BITS {
        // Longitude from even positions
        lon_int |= ((hash >> (51 - i * 2)) & 1) << (GEOHASH_BITS - 1 - i);
        // Latitude from odd positions
        lat_int |= ((hash >> (50 - i * 2)) & 1) << (GEOHASH_BITS - 1 - i);
    }

    // Convert back to coordinates — use the center of the geohash cell, matching Redis's behavior.
    // Redis computes (min + max) / 2 = (lon_int + 0.5) * step + LON_MIN
    let scale = (1u64 << GEOHASH_BITS) as f64;
    let lon = (lon_int as f64 + 0.5) / scale * (LON_MAX - LON_MIN) + LON_MIN;
    let lat = (lat_int as f64 + 0.5) / scale * (LAT_MAX - LAT_MIN) + LAT_MIN;

    (lon, lat)
}

/// Encode coordinates as an 11-character standard geohash string.
///
/// This re-encodes using the **standard** geohash latitude range [-90, 90]
/// (not the internal Mercator range [-85.05, 85.05]), matching Redis's
/// GEOHASH command behaviour. The last character is always '0' because
/// only 50 of 55 bits carry information at 26-step precision.
pub fn geohash_to_string(lon: f64, lat: f64) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    const STEP: u32 = 26;

    // Standard geohash uses [-90, 90] latitude, [-180, 180] longitude.
    let lon_c = lon.clamp(LON_MIN, LON_MAX);
    let lat_c = lat.clamp(-90.0, 90.0);
    let lon_norm = (lon_c - LON_MIN) / (LON_MAX - LON_MIN);
    let lat_norm = (lat_c - (-90.0)) / (90.0 - (-90.0));

    let max_val = (1u64 << STEP) - 1;
    let lon_int = ((lon_norm * (1u64 << STEP) as f64) as u64).min(max_val);
    let lat_int = ((lat_norm * (1u64 << STEP) as f64) as u64).min(max_val);

    // Interleave: lon in odd bit-positions (51, 49, …, 1), lat in even (50, 48, …, 0).
    // This matches Redis's interleave64(lat, lon) output.
    let mut hash = 0u64;
    for i in 0..STEP {
        hash |= ((lon_int >> (STEP - 1 - i)) & 1) << (51 - i * 2);
        hash |= ((lat_int >> (STEP - 1 - i)) & 1) << (50 - i * 2);
    }

    // Extract 10 × 5-bit groups + last char always '0' (matching Redis).
    let mut result = String::with_capacity(11);
    for i in 0..11u32 {
        let idx = if i == 10 {
            0
        } else {
            ((hash >> (52 - ((i + 1) * 5))) & 0x1F) as usize
        };
        result.push(ALPHABET[idx] as char);
    }

    result
}

/// Calculate the Haversine distance between two points in meters.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_rad = lat1 * PI / 180.0;
    let lat2_rad = lat2 * PI / 180.0;
    let lon1_rad = lon1 * PI / 180.0;
    let lon2_rad = lon2 * PI / 180.0;

    let dlat = lat2_rad - lat1_rad;
    let dlon = lon2_rad - lon1_rad;

    let a =
        (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

/// Get the geohash score as an f64 for storage in a sorted set.
pub fn geohash_to_score(hash: u64) -> f64 {
    hash as f64
}

/// Get the geohash from a sorted set score.
pub fn score_to_geohash(score: f64) -> u64 {
    score as u64
}

/// Get the bounding box geohash range for area searches.
///
/// Returns (min_hash, max_hash) for the given bounding box.
/// This is a simplification - for production use, consider using
/// multiple ranges to cover the area more precisely.
pub fn geohash_range_for_bbox(bbox: &BoundingBox) -> (u64, u64) {
    let _min_hash = geohash_encode(bbox.min_lon, bbox.min_lat);
    let _max_hash = geohash_encode(bbox.max_lon, bbox.max_lat);

    // Due to geohash interleaving, we can't simply use min/max
    // Instead, we need to consider all corners and find the actual range
    let hash_corners = [
        geohash_encode(bbox.min_lon, bbox.min_lat),
        geohash_encode(bbox.min_lon, bbox.max_lat),
        geohash_encode(bbox.max_lon, bbox.min_lat),
        geohash_encode(bbox.max_lon, bbox.max_lat),
    ];

    let min = *hash_corners.iter().min().unwrap();
    let max = *hash_corners.iter().max().unwrap();

    (min, max)
}

/// Check if a point is within a given radius of a center point.
pub fn is_within_radius(center: Coordinates, point: Coordinates, radius_m: f64) -> bool {
    haversine_distance(center.lon, center.lat, point.lon, point.lat) <= radius_m
}

/// Check if a point is within a given box.
///
/// Matches Redis's `geohashGetDistanceIfInRectangle`: east-west distance is
/// measured at the **point's** latitude, not the center's.
pub fn is_within_box(center: Coordinates, point: Coordinates, width_m: f64, height_m: f64) -> bool {
    let lon_dist = haversine_distance(center.lon, point.lat, point.lon, point.lat);
    let lat_dist = haversine_distance(center.lon, center.lat, center.lon, point.lat);

    lon_dist <= width_m / 2.0 && lat_dist <= height_m / 2.0
}

// ============================================================================
// 9-Area Geohash Scanning (matching Redis's membersOfAllNeighbors)
// ============================================================================

/// Mercator projection maximum value (meters), matching Redis.
const MERCATOR_MAX: f64 = 20037726.37;

/// A geohash encoded at an arbitrary step size (1-26).
#[derive(Debug, Clone, Copy)]
pub struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

/// Encode longitude/latitude at an arbitrary step size (1-26).
/// Same normalization as `geohash_encode` but using `step` instead of 26.
fn geohash_encode_at_step(lon: f64, lat: f64, step: u8) -> GeoHashBits {
    let lon = lon.clamp(LON_MIN, LON_MAX);
    let lat = lat.clamp(LAT_MIN, LAT_MAX);

    let lon_norm = (lon - LON_MIN) / (LON_MAX - LON_MIN);
    let lat_norm = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN);

    let max_val = (1u64 << step) - 1;
    let lon_int = ((lon_norm * (1u64 << step) as f64) as u64).min(max_val);
    let lat_int = ((lat_norm * (1u64 << step) as f64) as u64).min(max_val);

    // Interleave: lon in odd positions, lat in even (matching Redis's interleave64(lat, lon))
    let s = step as u32;
    let mut hash = 0u64;
    for i in 0..s {
        hash |= ((lon_int >> (s - 1 - i)) & 1) << (s * 2 - 1 - i * 2);
        hash |= ((lat_int >> (s - 1 - i)) & 1) << (s * 2 - 2 - i * 2);
    }

    GeoHashBits { bits: hash, step }
}

/// Left-shift a geohash to fill 52 bits (matching Redis's `geohashAlign52Bits`).
fn geohash_align_52_bits(hash: &GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step as u32 * 2)
}

/// Compute the sorted-set score range [min, max) for a geohash cell.
/// `min` is inclusive, `max` is exclusive.
pub fn geohash_score_range(hash: &GeoHashBits) -> (f64, f64) {
    let min = geohash_align_52_bits(hash);
    let max = geohash_align_52_bits(&GeoHashBits {
        bits: hash.bits + 1,
        step: hash.step,
    });
    (min as f64, max as f64)
}

/// Estimate geohash step size for a given search radius and latitude.
/// Matches Redis's `geohashEstimateStepsByRadius`.
fn geohash_estimate_steps_by_radius(radius_m: f64, lat: f64) -> u8 {
    if radius_m == 0.0 {
        return 26;
    }
    let mut range = radius_m;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2; // Ensure range is included in worst case
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// Move a geohash in the longitude direction (odd/x bits).
/// Matches Redis's `geohash_move_x`.
fn geohash_move_x(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let total_bits = hash.step as u32 * 2;
    let x_mask = 0xAAAA_AAAA_AAAA_AAAAu64 >> (64 - total_bits);
    let y_mask = 0x5555_5555_5555_5555u64 >> (64 - total_bits);

    let mut x = hash.bits & x_mask;
    let y = hash.bits & y_mask;
    let zz = y_mask;

    if d > 0 {
        x = x.wrapping_add(zz.wrapping_add(1));
    } else {
        x = (x | zz).wrapping_sub(zz.wrapping_add(1));
    }
    x &= x_mask;
    hash.bits = x | y;
}

/// Move a geohash in the latitude direction (even/y bits).
/// Matches Redis's `geohash_move_y`.
fn geohash_move_y(hash: &mut GeoHashBits, d: i8) {
    if d == 0 {
        return;
    }
    let total_bits = hash.step as u32 * 2;
    let x_mask = 0xAAAA_AAAA_AAAA_AAAAu64 >> (64 - total_bits);
    let y_mask = 0x5555_5555_5555_5555u64 >> (64 - total_bits);

    let x = hash.bits & x_mask;
    let mut y = hash.bits & y_mask;
    let zz = x_mask;

    if d > 0 {
        y = y.wrapping_add(zz.wrapping_add(1));
    } else {
        y = (y | zz).wrapping_sub(zz.wrapping_add(1));
    }
    y &= y_mask;
    hash.bits = x | y;
}

/// Compute the 8 neighbor cells of a geohash.
/// Returns in Redis order: [N, S, E, W, NE, NW, SE, SW].
fn geohash_neighbors(hash: &GeoHashBits) -> [GeoHashBits; 8] {
    let mut neighbors = [*hash; 8];

    // N: y+1
    geohash_move_y(&mut neighbors[0], 1);
    // S: y-1
    geohash_move_y(&mut neighbors[1], -1);
    // E: x+1
    geohash_move_x(&mut neighbors[2], 1);
    // W: x-1
    geohash_move_x(&mut neighbors[3], -1);
    // NE: x+1, y+1
    geohash_move_x(&mut neighbors[4], 1);
    geohash_move_y(&mut neighbors[4], 1);
    // NW: x-1, y+1
    geohash_move_x(&mut neighbors[5], -1);
    geohash_move_y(&mut neighbors[5], 1);
    // SE: x+1, y-1
    geohash_move_x(&mut neighbors[6], 1);
    geohash_move_y(&mut neighbors[6], -1);
    // SW: x-1, y-1
    geohash_move_x(&mut neighbors[7], -1);
    geohash_move_y(&mut neighbors[7], -1);

    neighbors
}

/// Decode a geohash cell into its coordinate ranges.
/// Returns (min_lon, max_lon, min_lat, max_lat).
fn geohash_decode_area(hash: &GeoHashBits) -> (f64, f64, f64, f64) {
    let s = hash.step as u32;

    let mut lon_int = 0u64;
    let mut lat_int = 0u64;
    for i in 0..s {
        lon_int |= ((hash.bits >> (s * 2 - 1 - i * 2)) & 1) << (s - 1 - i);
        lat_int |= ((hash.bits >> (s * 2 - 2 - i * 2)) & 1) << (s - 1 - i);
    }

    let scale = (1u64 << s) as f64;
    let lon_range = LON_MAX - LON_MIN;
    let lat_range = LAT_MAX - LAT_MIN;

    let min_lon = lon_int as f64 / scale * lon_range + LON_MIN;
    let max_lon = (lon_int + 1) as f64 / scale * lon_range + LON_MIN;
    let min_lat = lat_int as f64 / scale * lat_range + LAT_MIN;
    let max_lat = (lat_int + 1) as f64 / scale * lat_range + LAT_MIN;

    (min_lon, max_lon, min_lat, max_lat)
}

/// Compute the raw (unclamped) bounding box for the decrease_step check.
/// Matches Redis's `geohashBoundingBox`. Returns (min_lon, min_lat, max_lon, max_lat).
/// Longitude values may be inverted (min > max) when spanning the antimeridian.
fn geohash_raw_bbox(
    center: Coordinates,
    half_width_m: f64,
    half_height_m: f64,
) -> (f64, f64, f64, f64) {
    let lat_delta = (half_height_m / EARTH_RADIUS_M) * (180.0 / PI);
    let min_lat = center.lat - lat_delta;
    let max_lat = center.lat + lat_delta;

    let pole_lat = if center.lat >= 0.0 {
        center.lat + lat_delta
    } else {
        center.lat - lat_delta
    };
    let cos_pole = (pole_lat * PI / 180.0).cos();
    let lon_delta = (half_width_m / EARTH_RADIUS_M / cos_pole) * (180.0 / PI);

    let min_lon = center.lon - lon_delta;
    let max_lon = center.lon + lon_delta;

    (min_lon, min_lat, max_lon, max_lat)
}

/// Check if the 3x3 geohash grid needs larger cells (decrease step).
/// Compares decoded neighbor edges against the raw bounding box.
/// Matches Redis's decrease_step logic in `geohashCalculateAreasByShapeWGS84`.
fn needs_decrease_step(
    raw_bbox: (f64, f64, f64, f64), // (min_lon, min_lat, max_lon, max_lat)
    neighbors: &[GeoHashBits; 8],
) -> bool {
    let (bbox_min_lon, bbox_min_lat, bbox_max_lon, bbox_max_lat) = raw_bbox;

    let (_, _, _, n_max_lat) = geohash_decode_area(&neighbors[0]); // N
    let (_, _, s_min_lat, _) = geohash_decode_area(&neighbors[1]); // S
    let (_, e_max_lon, _, _) = geohash_decode_area(&neighbors[2]); // E
    let (w_min_lon, _, _, _) = geohash_decode_area(&neighbors[3]); // W

    n_max_lat < bbox_max_lat
        || s_min_lat > bbox_min_lat
        || e_max_lon < bbox_max_lon
        || w_min_lon > bbox_min_lon
}

/// Calculate the 9 geohash areas (center + 8 neighbors) for a geo search.
/// Returns areas in Redis's iteration order: center, N, S, E, W, NE, NW, SE, SW.
///
/// `search_radius_m` is used for step estimation (radius for circular, diagonal for box).
/// `bbox_half_width_m` and `bbox_half_height_m` are the half-dimensions used for the
/// bounding box check (both equal radius for circular; width/2 and height/2 for box).
pub fn geohash_calculate_areas(
    center: Coordinates,
    search_radius_m: f64,
    bbox_half_width_m: f64,
    bbox_half_height_m: f64,
) -> Vec<GeoHashBits> {
    let mut step = geohash_estimate_steps_by_radius(search_radius_m, center.lat);
    let mut hash = geohash_encode_at_step(center.lon, center.lat, step);
    let mut neighbors = geohash_neighbors(&hash);

    // Check if the 3x3 grid is too small to cover the search bounding box.
    // If so, decrease step (make cells larger) and recalculate.
    let raw_bbox = geohash_raw_bbox(center, bbox_half_width_m, bbox_half_height_m);
    if step > 1 && needs_decrease_step(raw_bbox, &neighbors) {
        step -= 1;
        hash = geohash_encode_at_step(center.lon, center.lat, step);
        neighbors = geohash_neighbors(&hash);
    }

    let mut areas = Vec::with_capacity(9);
    areas.push(hash);
    areas.extend_from_slice(&neighbors);
    areas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geohash_encode_decode_roundtrip() {
        let test_cases = vec![
            (-122.4194, 37.7749), // San Francisco
            (139.6917, 35.6895),  // Tokyo
            (-0.1276, 51.5074),   // London
            (0.0, 0.0),           // Origin
            (-179.0, -80.0),      // Near min corner
            (179.0, 80.0),        // Near max corner
        ];

        for (lon, lat) in test_cases {
            let hash = geohash_encode(lon, lat);
            let (decoded_lon, decoded_lat) = geohash_decode(hash);

            // Should be accurate to within ~1 meter for typical coordinates
            let dist = haversine_distance(lon, lat, decoded_lon, decoded_lat);
            assert!(
                dist < 1.0,
                "Roundtrip error too large: {} meters for ({}, {})",
                dist,
                lon,
                lat
            );
        }
    }

    #[test]
    fn test_geohash_to_string() {
        // Wikipedia example: (-5.6, 42.6) → "ezs42e44yx0"
        let hash_str = geohash_to_string(-5.6, 42.6);
        assert_eq!(hash_str, "ezs42e44yx0");

        // San Francisco
        let hash_str = geohash_to_string(-122.4194, 37.7749);
        assert_eq!(hash_str.len(), 11);
        assert!(hash_str.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_haversine_distance() {
        // San Francisco to Los Angeles: ~559 km
        let sf_to_la = haversine_distance(-122.4194, 37.7749, -118.2437, 34.0522);
        assert!(
            (sf_to_la - 559_000.0).abs() < 10_000.0,
            "SF to LA: {} m",
            sf_to_la
        );

        // Same point should be 0
        let same = haversine_distance(-122.4194, 37.7749, -122.4194, 37.7749);
        assert!(same < 0.001, "Same point distance: {}", same);
    }

    #[test]
    fn test_distance_unit_conversion() {
        let meters = 1000.0;

        assert!((DistanceUnit::M.from_meters(meters) - 1000.0).abs() < 0.001);
        assert!((DistanceUnit::Km.from_meters(meters) - 1.0).abs() < 0.001);
        assert!((DistanceUnit::Mi.from_meters(meters) - 0.621371).abs() < 0.001);
        assert!((DistanceUnit::Ft.from_meters(meters) - 3280.84).abs() < 1.0);
    }

    #[test]
    fn test_bounding_box_contains() {
        let center = Coordinates::new(-122.4194, 37.7749).unwrap();
        let bbox = BoundingBox::from_radius(center, 10_000.0); // 10 km radius

        // Center should be in box
        assert!(bbox.contains(center));

        // Point 5 km away should be in box
        let nearby = Coordinates::new(-122.4, 37.78).unwrap();
        assert!(bbox.contains(nearby));

        // Point 100 km away should not be in box
        let far = Coordinates::new(-121.0, 37.0).unwrap();
        assert!(!bbox.contains(far));
    }

    #[test]
    fn test_is_within_radius() {
        let sf = Coordinates::new(-122.4194, 37.7749).unwrap();
        let oakland = Coordinates::new(-122.2712, 37.8044).unwrap();

        // SF to Oakland is about 13 km
        assert!(is_within_radius(sf, oakland, 15_000.0));
        assert!(!is_within_radius(sf, oakland, 10_000.0));
    }

    #[test]
    fn test_coordinates_validation() {
        assert!(Coordinates::new(-122.4194, 37.7749).is_some());
        assert!(Coordinates::new(-181.0, 0.0).is_none());
        assert!(Coordinates::new(181.0, 0.0).is_none());
        assert!(Coordinates::new(0.0, -90.0).is_none());
        assert!(Coordinates::new(0.0, 90.0).is_none());
    }
}
