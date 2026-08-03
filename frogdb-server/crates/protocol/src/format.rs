//! Canonical float rendering.
//!
//! FrogDB renders an `f64` as a client-visible string in exactly one place: this
//! module. It lives in `frogdb-protocol` because that is the only crate every
//! other crate on the rendering path already depends on — `frogdb-types` (the
//! value store), `frogdb-core` (the shard) and `frogdb-commands` (the reply
//! builders) all sit above it, so a single definition here is reachable from all
//! of them without a new edge in the dependency graph.
//!
//! **Why one definition matters.** The rendering is not only what a client sees:
//! `INCRBYFLOAT`/`HINCRBYFLOAT` *store* the rendered string, so the same bytes
//! are what persists to the WAL and what replicates to every replica. Two
//! renderers on that path meant `INCRBYFLOAT k 0.1` replied `0.1` and stored
//! `0.10000000000000001` — the reply and a subsequent `GET` disagreed, and the
//! ugly string was the one that crossed the link
//! (`.scratch/testing-improvements-round2/issues/` issue 55, theme issue 26).

/// Format a float the way Redis renders one.
///
/// Mirrors Redis's `d2string`/`%.17g`:
///
/// - non-finite values render as `inf` / `-inf` / `nan`;
/// - zero (either sign) renders as `0`, matching Redis's explicit `-0` → `0`
///   normalization in `ld2string`;
/// - an integer-valued float below `1e17` renders without a decimal point,
///   because `%.17g` only switches to exponent form at an exponent of 17;
/// - everything else uses `ryu`'s shortest round-trip representation, with the
///   exponent normalized to C's `e+NN` spelling.
///
/// # Why shortest round-trip rather than `%.17f`
///
/// Redis computes `INCRBYFLOAT` in `long double` and renders it with `%.17Lf`
/// plus a trailing-zero trim. That trick relies on the 80-bit accumulator: at
/// 64-bit precision the same algorithm renders `0.1` as `0.10000000000000001`,
/// because 17 *decimal places* of a `double` expose the representation error
/// that a `long double` still has room to hide. FrogDB computes in `f64`, so it
/// reproduces Redis's *output* — the shortest string that round-trips — instead
/// of Redis's *algorithm*.
pub fn format_float(f: f64) -> String {
    if f == f64::INFINITY {
        return "inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if f.is_nan() {
        return "nan".to_string();
    }
    // `-0.0 == 0.0`, so this also normalizes negative zero to "0".
    if f == 0.0 {
        return "0".to_string();
    }

    // Redis uses %.17g, which uses decimal notation for exponents < 17
    // (i.e. values < 1e17). For integer-valued floats below that, return a
    // plain integer string matching Redis's behavior.
    if f.fract() == 0.0 && f.abs() < 1e17 {
        return format!("{:.0}", f);
    }

    // ryu produces minimal-length representations that round-trip correctly.
    let mut buf = ryu::Buffer::new();
    let s = buf.format(f);

    // Redis uses C's %.17g format which includes "e+308" (with explicit '+' sign
    // for positive exponents). ryu produces "e308" (no '+'), so we normalize here.
    if let Some(e_pos) = s.find('e') {
        let after_e = &s[e_pos + 1..];
        if !after_e.starts_with('-') && !after_e.starts_with('+') {
            return format!("{}e+{}", &s[..e_pos], after_e);
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    // 3.14 is one of the values issue 55 names; it is a test datum, not an
    // approximation of pi that should be replaced by the constant.
    #![allow(clippy::approx_constant)]

    use super::format_float;

    #[test]
    fn non_finite_values_render_as_redis_spells_them() {
        assert_eq!(format_float(f64::INFINITY), "inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-inf");
        assert_eq!(format_float(f64::NAN), "nan");
    }

    #[test]
    fn both_zeroes_render_as_a_bare_zero() {
        assert_eq!(format_float(0.0), "0");
        assert_eq!(format_float(-0.0), "0");
    }

    #[test]
    fn integer_valued_floats_lose_the_decimal_point() {
        assert_eq!(format_float(3.0), "3");
        assert_eq!(format_float(-3.0), "-3");
        assert_eq!(format_float(17179869184.0), "17179869184");
        // Just under the %.17g exponent-form cutoff.
        assert_eq!(format_float(1e16), "10000000000000000");
    }

    #[test]
    fn inexact_values_render_as_the_shortest_string_that_round_trips() {
        // The whole point of issue 55: `{:.17}` renders these with the
        // representation error spelled out.
        assert_eq!(format_float(0.1), "0.1");
        assert_eq!(format_float(3.14), "3.14");
        assert_eq!(format_float(0.1 + 0.2), "0.30000000000000004");
    }

    #[test]
    fn extreme_magnitudes_use_c_style_exponents() {
        assert_eq!(format_float(1e17), "1e+17");
        assert_eq!(format_float(1e300), "1e+300");
        assert_eq!(format_float(1e-7), "1e-7");
        // `{:.17}` rendered this as the empty string: seventeen decimal places
        // of 1e-320 are all zero, and the trim ate the "0." as well.
        assert_eq!(format_float(1e-320), "1e-320");
    }

    #[test]
    fn every_rendering_parses_back_to_the_value_it_came_from() {
        let table = [
            0.1,
            3.14,
            1e-7,
            -0.0,
            1e17,
            1e-320,
            0.1 + 0.2,
            -2.5,
            f64::MIN_POSITIVE,
            f64::MAX,
        ];
        for f in table {
            let rendered = format_float(f);
            let parsed: f64 = rendered.parse().unwrap_or_else(|e| {
                panic!("format_float({f:?}) produced {rendered:?}, which does not parse: {e}")
            });
            assert_eq!(
                parsed,
                // -0.0 is deliberately normalized to 0.0.
                if f == 0.0 { 0.0 } else { f },
                "format_float({f:?}) = {rendered:?} did not round-trip"
            );
        }
    }
}
