#![no_main]
use frogdb_types::geo::{
    BoundingBox, Coordinates, DistanceUnit, geohash_calculate_areas, geohash_decode,
    geohash_encode, geohash_range_for_bbox, geohash_to_string, haversine_distance, is_within_box,
    is_within_radius,
};
use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Op {
    EncodeDecodeRoundtrip { lon: f64, lat: f64 },
    HashToString { lon: f64, lat: f64 },
    Distance { lon1: f64, lat1: f64, lon2: f64, lat2: f64 },
    BboxFromRadius { lon: f64, lat: f64, radius: f64 },
    CalculateAreas { lon: f64, lat: f64, radius: f64 },
    WithinRadius { center_lon: f64, center_lat: f64, point_lon: f64, point_lat: f64, radius: f64 },
    WithinBox { center_lon: f64, center_lat: f64, point_lon: f64, point_lat: f64, width: f64, height: f64 },
    UnitConversion { value: f64, unit: u8 },
    BboxRange { min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64 },
}

#[derive(Arbitrary, Debug)]
struct GeoInput {
    ops: Vec<Op>,
}

/// Clamp to a reasonable range to avoid extremely slow trig computations.
fn clamp_coord(lon: f64, lat: f64) -> (f64, f64) {
    let lon = if lon.is_finite() { lon.clamp(-360.0, 360.0) } else { 0.0 };
    let lat = if lat.is_finite() { lat.clamp(-180.0, 180.0) } else { 0.0 };
    (lon, lat)
}

fn clamp_positive(v: f64) -> f64 {
    if v.is_finite() && v >= 0.0 { v.min(1e9) } else { 0.0 }
}

fn unit_from_byte(b: u8) -> DistanceUnit {
    match b % 4 {
        0 => DistanceUnit::M,
        1 => DistanceUnit::Km,
        2 => DistanceUnit::Mi,
        _ => DistanceUnit::Ft,
    }
}

fuzz_target!(|input: GeoInput| {
    if input.ops.len() > 128 {
        return;
    }

    for op in &input.ops {
        match *op {
            Op::EncodeDecodeRoundtrip { lon, lat } => {
                let (lon, lat) = clamp_coord(lon, lat);
                let hash = geohash_encode(lon, lat);
                let (dec_lon, dec_lat) = geohash_decode(hash);

                // Decoded values must be within valid geo range.
                assert!(dec_lon >= -180.0 && dec_lon <= 180.0, "decoded lon out of range: {dec_lon}");
                assert!(dec_lat >= -90.0 && dec_lat <= 90.0, "decoded lat out of range: {dec_lat}");

                // If input was within valid coordinate range, roundtrip error should be < 1m.
                if (-180.0..=180.0).contains(&lon) && (-85.05112878..=85.05112878).contains(&lat) {
                    let dist = haversine_distance(lon, lat, dec_lon, dec_lat);
                    assert!(
                        dist < 1.0 || !dist.is_finite(),
                        "roundtrip error too large: {dist}m for ({lon}, {lat})"
                    );
                }
            }

            Op::HashToString { lon, lat } => {
                let (lon, lat) = clamp_coord(lon, lat);
                let s = geohash_to_string(lon, lat);
                assert_eq!(s.len(), 11, "geohash string wrong length: {}", s.len());
                assert!(
                    s.chars().all(|c| c.is_ascii_alphanumeric()),
                    "geohash string contains invalid chars: {s}"
                );
            }

            Op::Distance { lon1, lat1, lon2, lat2 } => {
                let (lon1, lat1) = clamp_coord(lon1, lat1);
                let (lon2, lat2) = clamp_coord(lon2, lat2);
                let d = haversine_distance(lon1, lat1, lon2, lat2);
                // Distance must be non-negative (may be NaN for extreme inputs).
                assert!(d >= 0.0 || d.is_nan(), "negative distance: {d}");
            }

            Op::BboxFromRadius { lon, lat, radius } => {
                let radius = clamp_positive(radius);
                if let Some(center) = Coordinates::new(lon, lat) {
                    let bbox = BoundingBox::from_radius(center, radius);
                    // Center must be inside its own bounding box.
                    assert!(
                        bbox.contains(center),
                        "center ({lon}, {lat}) not in its bbox (r={radius})"
                    );
                }
            }

            Op::CalculateAreas { lon, lat, radius } => {
                let radius = clamp_positive(radius);
                if let Some(center) = Coordinates::new(lon, lat) {
                    let areas = geohash_calculate_areas(center, radius, radius, radius);
                    assert_eq!(areas.len(), 9, "expected 9 areas, got {}", areas.len());
                    for area in &areas {
                        assert!(area.step >= 1 && area.step <= 26, "invalid step: {}", area.step);
                    }
                }
            }

            Op::WithinRadius { center_lon, center_lat, point_lon, point_lat, radius } => {
                let radius = clamp_positive(radius);
                if let (Some(center), Some(point)) = (
                    Coordinates::new(center_lon, center_lat),
                    Coordinates::new(point_lon, point_lat),
                ) {
                    // Just exercise the function — no crash = pass.
                    let _ = is_within_radius(center, point, radius);
                }
            }

            Op::WithinBox { center_lon, center_lat, point_lon, point_lat, width, height } => {
                let width = clamp_positive(width);
                let height = clamp_positive(height);
                if let (Some(center), Some(point)) = (
                    Coordinates::new(center_lon, center_lat),
                    Coordinates::new(point_lon, point_lat),
                ) {
                    let _ = is_within_box(center, point, width, height);
                }
            }

            Op::UnitConversion { value, unit } => {
                let u = unit_from_byte(unit);
                let value = if value.is_finite() { value.clamp(-1e15, 1e15) } else { 0.0 };
                let meters = u.to_meters(value);
                let back = u.from_meters(meters);
                // Roundtrip should be close for finite values.
                if meters.is_finite() && back.is_finite() && value.abs() > 1e-10 {
                    let rel_err = ((back - value) / value).abs();
                    assert!(
                        rel_err < 1e-9,
                        "unit roundtrip error: {value} -> {meters}m -> {back} (err={rel_err})"
                    );
                }
            }

            Op::BboxRange { min_lon, min_lat, max_lon, max_lat } => {
                let (min_lon, min_lat) = clamp_coord(min_lon, min_lat);
                let (max_lon, max_lat) = clamp_coord(max_lon, max_lat);
                let bbox = BoundingBox {
                    min_lon,
                    max_lon,
                    min_lat,
                    max_lat,
                };
                let (lo, hi) = geohash_range_for_bbox(&bbox);
                assert!(lo <= hi, "bbox range inverted: {lo} > {hi}");
            }
        }
    }
});
