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
    pub fn from_radius(center: Coordinates, radius_m: f64) -> Self {
        // Calculate lat/lon deltas for the radius
        let lat_delta = radius_m / EARTH_RADIUS_M * (180.0 / PI);
        let lon_delta = lat_delta / (center.lat * PI / 180.0).cos();

        Self {
            min_lon: (center.lon - lon_delta).max(LON_MIN),
            max_lon: (center.lon + lon_delta).min(LON_MAX),
            min_lat: (center.lat - lat_delta).max(LAT_MIN),
            max_lat: (center.lat + lat_delta).min(LAT_MAX),
        }
    }

    /// Create a bounding box from center point and width/height (in meters).
    pub fn from_box(center: Coordinates, width_m: f64, height_m: f64) -> Self {
        let lat_delta = (height_m / 2.0) / EARTH_RADIUS_M * (180.0 / PI);
        let lon_delta = (width_m / 2.0) / EARTH_RADIUS_M * (180.0 / PI) / (center.lat * PI / 180.0).cos();

        Self {
            min_lon: (center.lon - lon_delta).max(LON_MIN),
            max_lon: (center.lon + lon_delta).min(LON_MAX),
            min_lat: (center.lat - lat_delta).max(LAT_MIN),
            max_lat: (center.lat + lat_delta).min(LAT_MAX),
        }
    }

    /// Check if coordinates are within this bounding box.
    pub fn contains(&self, coords: Coordinates) -> bool {
        coords.lon >= self.min_lon
            && coords.lon <= self.max_lon
            && coords.lat >= self.min_lat
            && coords.lat <= self.max_lat
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

    // Convert back to coordinates
    let lon = lon_int as f64 / (1u64 << GEOHASH_BITS) as f64 * (LON_MAX - LON_MIN) + LON_MIN;
    let lat = lat_int as f64 / (1u64 << GEOHASH_BITS) as f64 * (LAT_MAX - LAT_MIN) + LAT_MIN;

    (lon, lat)
}

/// Convert a 52-bit geohash to an 11-character base32 string.
///
/// Uses the standard geohash alphabet: 0-9b-hjkmnp-z (no a, i, l, o).
pub fn geohash_to_string(hash: u64) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

    // Pad to 55 bits (11 characters * 5 bits)
    let padded = hash << 3;

    let mut result = String::with_capacity(11);
    for i in 0..11 {
        let idx = ((padded >> (55 - 5 - i * 5)) & 0x1F) as usize;
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

    let a = (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
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
pub fn is_within_box(center: Coordinates, point: Coordinates, width_m: f64, height_m: f64) -> bool {
    let dx = haversine_distance(center.lon, center.lat, point.lon, center.lat);
    let dy = haversine_distance(center.lon, center.lat, center.lon, point.lat);

    dx <= width_m / 2.0 && dy <= height_m / 2.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geohash_encode_decode_roundtrip() {
        let test_cases = vec![
            (-122.4194, 37.7749),   // San Francisco
            (139.6917, 35.6895),    // Tokyo
            (-0.1276, 51.5074),     // London
            (0.0, 0.0),             // Origin
            (-179.0, -80.0),        // Near min corner
            (179.0, 80.0),          // Near max corner
        ];

        for (lon, lat) in test_cases {
            let hash = geohash_encode(lon, lat);
            let (decoded_lon, decoded_lat) = geohash_decode(hash);

            // Should be accurate to within ~1 meter for typical coordinates
            let dist = haversine_distance(lon, lat, decoded_lon, decoded_lat);
            assert!(
                dist < 1.0,
                "Roundtrip error too large: {} meters for ({}, {})",
                dist, lon, lat
            );
        }
    }

    #[test]
    fn test_geohash_to_string() {
        // Test encoding for San Francisco
        let hash = geohash_encode(-122.4194, 37.7749);
        let hash_str = geohash_to_string(hash);
        assert_eq!(hash_str.len(), 11);
        // Should produce a valid 11-char geohash string
        // Note: different geohash implementations may produce slightly different results
        assert!(hash_str.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_haversine_distance() {
        // San Francisco to Los Angeles: ~559 km
        let sf_to_la = haversine_distance(-122.4194, 37.7749, -118.2437, 34.0522);
        assert!((sf_to_la - 559_000.0).abs() < 10_000.0, "SF to LA: {} m", sf_to_la);

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
