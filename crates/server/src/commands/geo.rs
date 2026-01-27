//! Geo commands.
//!
//! Commands for geospatial operations using sorted sets:
//! - GEOADD - Add geospatial items
//! - GEODIST - Get distance between two members
//! - GEOHASH - Get geohash string for members
//! - GEOPOS - Get coordinates for members
//! - GEOSEARCH - Search members within an area
//! - GEOSEARCHSTORE - Store search results
//! - GEORADIUS, GEORADIUSBYMEMBER - Legacy search commands

use bytes::Bytes;
use frogdb_core::{
    Arity, BoundingBox, Command, CommandContext, CommandError, CommandFlags, Coordinates,
    DistanceUnit, SortedSetValue, Value, geohash_decode, geohash_encode, geohash_to_score,
    geohash_to_string, haversine_distance, is_within_box, score_to_geohash,
};
use frogdb_protocol::Response;

use super::utils::{
    format_float, get_or_create_zset as get_or_create_geo, parse_f64, parse_usize, NxXxOptions,
};

/// Search result with member and optional distance/hash/coordinates.
#[derive(Debug)]
struct GeoSearchResult {
    member: Bytes,
    dist: Option<f64>,
    hash: Option<u64>,
    coords: Option<(f64, f64)>,
}

// ============================================================================
// GEOADD - Add geospatial items
// ============================================================================

pub struct GeoaddCommand;

impl Command for GeoaddCommand {
    fn name(&self) -> &'static str {
        "GEOADD"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // GEOADD key [NX|XX] [CH] longitude latitude member [...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse options using shared utility
        let mut nx_xx = NxXxOptions::default();
        let mut ch = false;
        let mut i = 1;

        while i < args.len() {
            let arg = args[i].as_ref();
            if let Some(new_opts) = nx_xx.try_parse(arg)? {
                nx_xx = new_opts;
                i += 1;
            } else if arg.to_ascii_uppercase() == b"CH" {
                ch = true;
                i += 1;
            } else {
                break;
            }
        }
        let nx = nx_xx.nx;
        let xx = nx_xx.xx;

        // Parse coordinate-member triplets
        let remaining = &args[i..];
        if remaining.len() < 3 || remaining.len() % 3 != 0 {
            return Err(CommandError::WrongArity { command: "GEOADD" });
        }

        let zset = get_or_create_geo(ctx, key)?;
        let mut added = 0;
        let mut changed = 0;

        for chunk in remaining.chunks(3) {
            let lon = parse_f64(&chunk[0])?;
            let lat = parse_f64(&chunk[1])?;
            let member = chunk[2].clone();

            // Validate coordinates
            if let Some(coords) = Coordinates::new(lon, lat) {
                let score = geohash_to_score(geohash_encode(coords.lon, coords.lat));
                let exists = zset.get_score(&member).is_some();

                if (nx && exists) || (xx && !exists) {
                    continue;
                }

                let old_score = zset.get_score(&member);
                zset.add(member, score);

                if old_score.is_none() {
                    added += 1;
                } else if old_score != Some(score) {
                    changed += 1;
                }
            } else {
                return Err(CommandError::InvalidArgument {
                    message: format!("invalid longitude,latitude pair {},{}", lon, lat),
                });
            }
        }

        let result = if ch { added + changed } else { added };
        Ok(Response::Integer(result))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// GEODIST - Get distance between two members
// ============================================================================

pub struct GeodistCommand;

impl Command for GeodistCommand {
    fn name(&self) -> &'static str {
        "GEODIST"
    }

    fn arity(&self) -> Arity {
        Arity::Range { min: 3, max: 4 } // GEODIST key member1 member2 [unit]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let member1 = &args[1];
        let member2 = &args[2];

        let unit = if args.len() > 3 {
            DistanceUnit::parse(&args[3]).ok_or(CommandError::SyntaxError)?
        } else {
            DistanceUnit::M
        };

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                let score1 = match zset.get_score(member1) {
                    Some(s) => s,
                    None => return Ok(Response::null()),
                };

                let score2 = match zset.get_score(member2) {
                    Some(s) => s,
                    None => return Ok(Response::null()),
                };

                let (lon1, lat1) = geohash_decode(score_to_geohash(score1));
                let (lon2, lat2) = geohash_decode(score_to_geohash(score2));

                let dist_m = haversine_distance(lon1, lat1, lon2, lat2);
                let dist = unit.from_meters(dist_m);

                Ok(Response::bulk(Bytes::from(format_float(dist))))
            }
            None => Ok(Response::null()),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// GEOHASH - Get geohash strings for members
// ============================================================================

pub struct GeohashCommand;

impl Command for GeohashCommand {
    fn name(&self) -> &'static str {
        "GEOHASH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // GEOHASH key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let members = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                let results: Vec<Response> = members
                    .iter()
                    .map(|member| {
                        match zset.get_score(member) {
                            Some(score) => {
                                let hash = score_to_geohash(score);
                                let hash_str = geohash_to_string(hash);
                                Response::bulk(Bytes::from(hash_str))
                            }
                            None => Response::null(),
                        }
                    })
                    .collect();

                Ok(Response::Array(results))
            }
            None => Ok(Response::Array(
                members.iter().map(|_| Response::null()).collect(),
            )),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// GEOPOS - Get coordinates for members
// ============================================================================

pub struct GeoposCommand;

impl Command for GeoposCommand {
    fn name(&self) -> &'static str {
        "GEOPOS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(2) // GEOPOS key member [member ...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let members = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                let results: Vec<Response> = members
                    .iter()
                    .map(|member| {
                        match zset.get_score(member) {
                            Some(score) => {
                                let (lon, lat) = geohash_decode(score_to_geohash(score));
                                Response::Array(vec![
                                    Response::bulk(Bytes::from(format_float(lon))),
                                    Response::bulk(Bytes::from(format_float(lat))),
                                ])
                            }
                            None => Response::null(),
                        }
                    })
                    .collect();

                Ok(Response::Array(results))
            }
            None => Ok(Response::Array(
                members.iter().map(|_| Response::null()).collect(),
            )),
        }
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// GEOSEARCH - Search members within an area
// ============================================================================

pub struct GeosearchCommand;

impl Command for GeosearchCommand {
    fn name(&self) -> &'static str {
        "GEOSEARCH"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // GEOSEARCH key FROMMEMBER|FROMLONLAT ... BYRADIUS|BYBOX ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse search options
        let opts = parse_geosearch_options(&args[1..], ctx, key)?;
        let results = execute_geosearch(ctx, key, &opts)?;

        // Format results
        format_geosearch_results(&results, &opts)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// GEOSEARCHSTORE - Store search results
// ============================================================================

pub struct GeosearchstoreCommand;

impl Command for GeosearchstoreCommand {
    fn name(&self) -> &'static str {
        "GEOSEARCHSTORE"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // GEOSEARCHSTORE dest src FROMMEMBER|FROMLONLAT ... BYRADIUS|BYBOX ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let destkey = &args[0];
        let srckey = &args[1];

        // Parse search options
        let opts = parse_geosearch_options(&args[2..], ctx, srckey)?;
        let results = execute_geosearch(ctx, srckey, &opts)?;

        if results.is_empty() {
            ctx.store.delete(destkey);
            return Ok(Response::Integer(0));
        }

        // Store results in destination
        let mut dest_zset = SortedSetValue::new();
        for result in &results {
            let score = if opts.store_dist {
                result.dist.unwrap_or(0.0)
            } else {
                geohash_to_score(result.hash.unwrap_or(0))
            };
            dest_zset.add(result.member.clone(), score);
        }

        let count = results.len();
        ctx.store.set(destkey.clone(), Value::SortedSet(dest_zset));

        Ok(Response::Integer(count as i64))
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.len() >= 2 {
            vec![&args[0], &args[1]]
        } else {
            vec![]
        }
    }

    fn requires_same_slot(&self) -> bool {
        true
    }
}

// ============================================================================
// GEORADIUS - Legacy search by radius (deprecated)
// ============================================================================

pub struct GeoradiusCommand;

impl Command for GeoradiusCommand {
    fn name(&self) -> &'static str {
        "GEORADIUS"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(5) // GEORADIUS key lon lat radius unit [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let lon = parse_f64(&args[1])?;
        let lat = parse_f64(&args[2])?;
        let radius = parse_f64(&args[3])?;
        let unit = DistanceUnit::parse(&args[4]).ok_or(CommandError::SyntaxError)?;

        // Parse options
        let extra_opts = &args[5..];
        let radius_opts = parse_georadius_options(extra_opts)?;

        let coords = Coordinates::new(lon, lat).ok_or_else(|| CommandError::InvalidArgument {
            message: format!("invalid longitude,latitude pair {},{}", lon, lat),
        })?;

        let opts = GeoSearchOptions {
            center: coords,
            radius_m: Some(unit.to_meters(radius)),
            width_m: None,
            height_m: None,
            unit,
            with_coord: radius_opts.with_coord,
            with_dist: radius_opts.with_dist,
            with_hash: radius_opts.with_hash,
            count: radius_opts.count,
            any: false,
            asc: radius_opts.asc,
            desc: radius_opts.desc,
            store_dist: false,
        };

        let results = execute_geosearch(ctx, key, &opts)?;
        format_geosearch_results(&results, &opts)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// GEORADIUSBYMEMBER - Legacy search by member (deprecated)
// ============================================================================

pub struct GeoradiusbymemberCommand;

impl Command for GeoradiusbymemberCommand {
    fn name(&self) -> &'static str {
        "GEORADIUSBYMEMBER"
    }

    fn arity(&self) -> Arity {
        Arity::AtLeast(4) // GEORADIUSBYMEMBER key member radius unit [options...]
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn execute(
        &self,
        ctx: &mut CommandContext,
        args: &[Bytes],
    ) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];
        let radius = parse_f64(&args[2])?;
        let unit = DistanceUnit::parse(&args[3]).ok_or(CommandError::SyntaxError)?;

        // Get member's coordinates
        let coords = match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                match zset.get_score(member) {
                    Some(score) => {
                        let (lon, lat) = geohash_decode(score_to_geohash(score));
                        Coordinates::new(lon, lat).ok_or_else(|| CommandError::InvalidArgument {
                            message: "member has invalid coordinates".to_string(),
                        })?
                    }
                    None => {
                        return Err(CommandError::InvalidArgument {
                            message: format!(
                                "could not decode requested zset member: {}",
                                String::from_utf8_lossy(member)
                            ),
                        });
                    }
                }
            }
            None => return Ok(Response::Array(vec![])),
        };

        // Parse options
        let extra_opts = &args[4..];
        let radius_opts = parse_georadius_options(extra_opts)?;

        let opts = GeoSearchOptions {
            center: coords,
            radius_m: Some(unit.to_meters(radius)),
            width_m: None,
            height_m: None,
            unit,
            with_coord: radius_opts.with_coord,
            with_dist: radius_opts.with_dist,
            with_hash: radius_opts.with_hash,
            count: radius_opts.count,
            any: false,
            asc: radius_opts.asc,
            desc: radius_opts.desc,
            store_dist: false,
        };

        let results = execute_geosearch(ctx, key, &opts)?;
        format_geosearch_results(&results, &opts)
    }

    fn keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            vec![]
        } else {
            vec![&args[0]]
        }
    }
}

// ============================================================================
// Helper structs and functions
// ============================================================================

/// Options for GEORADIUS command.
#[allow(dead_code)]
struct GeoRadiusOptions {
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    asc: bool,
    desc: bool,
    store: Option<Bytes>,
    store_dist: bool,
}

/// Options for GEOSEARCH command.
struct GeoSearchOptions {
    center: Coordinates,
    radius_m: Option<f64>,
    width_m: Option<f64>,
    height_m: Option<f64>,
    unit: DistanceUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<usize>,
    any: bool,
    asc: bool,
    desc: bool,
    store_dist: bool,
}

/// Parse GEOSEARCH options from args.
fn parse_geosearch_options(
    args: &[Bytes],
    ctx: &CommandContext,
    key: &Bytes,
) -> Result<GeoSearchOptions, CommandError> {
    let mut center: Option<Coordinates> = None;
    let mut radius_m: Option<f64> = None;
    let mut width_m: Option<f64> = None;
    let mut height_m: Option<f64> = None;
    let mut unit = DistanceUnit::M;
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count: Option<usize> = None;
    let mut any = false;
    let mut asc = false;
    let mut desc = false;
    let mut store_dist = false;

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"FROMMEMBER" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let member = &args[i + 1];

                center = match ctx.store.get(key) {
                    Some(value) => {
                        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                        let score = zset.get_score(member).ok_or_else(|| CommandError::InvalidArgument {
                            message: format!(
                                "could not decode requested zset member: {}",
                                String::from_utf8_lossy(member)
                            ),
                        })?;

                        let (lon, lat) = geohash_decode(score_to_geohash(score));
                        Some(Coordinates::new(lon, lat).ok_or_else(|| CommandError::InvalidArgument {
                            message: "member has invalid coordinates".to_string(),
                        })?)
                    }
                    None => {
                        return Err(CommandError::InvalidArgument {
                            message: format!(
                                "could not decode requested zset member: {}",
                                String::from_utf8_lossy(member)
                            ),
                        });
                    }
                };
                i += 2;
            }
            b"FROMLONLAT" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let lon = parse_f64(&args[i + 1])?;
                let lat = parse_f64(&args[i + 2])?;
                center = Some(Coordinates::new(lon, lat).ok_or_else(|| CommandError::InvalidArgument {
                    message: format!("invalid longitude,latitude pair {},{}", lon, lat),
                })?);
                i += 3;
            }
            b"BYRADIUS" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let radius = parse_f64(&args[i + 1])?;
                unit = DistanceUnit::parse(&args[i + 2]).ok_or(CommandError::SyntaxError)?;
                radius_m = Some(unit.to_meters(radius));
                i += 3;
            }
            b"BYBOX" => {
                if i + 3 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                let w = parse_f64(&args[i + 1])?;
                let h = parse_f64(&args[i + 2])?;
                unit = DistanceUnit::parse(&args[i + 3]).ok_or(CommandError::SyntaxError)?;
                width_m = Some(unit.to_meters(w));
                height_m = Some(unit.to_meters(h));
                i += 4;
            }
            b"WITHCOORD" => {
                with_coord = true;
                i += 1;
            }
            b"WITHDIST" => {
                with_dist = true;
                i += 1;
            }
            b"WITHHASH" => {
                with_hash = true;
                i += 1;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = Some(parse_usize(&args[i + 1])?);
                i += 2;

                // Check for ANY
                if i < args.len() && args[i].to_ascii_uppercase() == b"ANY".as_slice() {
                    any = true;
                    i += 1;
                }
            }
            b"ASC" => {
                asc = true;
                i += 1;
            }
            b"DESC" => {
                desc = true;
                i += 1;
            }
            b"STOREDIST" => {
                store_dist = true;
                i += 1;
            }
            _ => {
                return Err(CommandError::SyntaxError);
            }
        }
    }

    let center = center.ok_or(CommandError::SyntaxError)?;
    if radius_m.is_none() && width_m.is_none() {
        return Err(CommandError::SyntaxError);
    }

    Ok(GeoSearchOptions {
        center,
        radius_m,
        width_m,
        height_m,
        unit,
        with_coord,
        with_dist,
        with_hash,
        count,
        any,
        asc,
        desc,
        store_dist,
    })
}

/// Parse legacy GEORADIUS options.
fn parse_georadius_options(args: &[Bytes]) -> Result<GeoRadiusOptions, CommandError> {
    let mut with_coord = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut count: Option<usize> = None;
    let mut asc = false;
    let mut desc = false;
    let mut store: Option<Bytes> = None;
    let mut store_dist = false;

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"WITHCOORD" => {
                with_coord = true;
                i += 1;
            }
            b"WITHDIST" => {
                with_dist = true;
                i += 1;
            }
            b"WITHHASH" => {
                with_hash = true;
                i += 1;
            }
            b"COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                count = Some(parse_usize(&args[i + 1])?);
                i += 2;
            }
            b"ASC" => {
                asc = true;
                i += 1;
            }
            b"DESC" => {
                desc = true;
                i += 1;
            }
            b"STORE" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                store = Some(args[i + 1].clone());
                i += 2;
            }
            b"STOREDIST" => {
                store_dist = true;
                i += 1;
            }
            _ => {
                return Err(CommandError::SyntaxError);
            }
        }
    }

    Ok(GeoRadiusOptions {
        with_coord,
        with_dist,
        with_hash,
        count,
        asc,
        desc,
        store,
        store_dist,
    })
}

/// Execute a geo search and return results.
fn execute_geosearch(
    ctx: &CommandContext,
    key: &Bytes,
    opts: &GeoSearchOptions,
) -> Result<Vec<GeoSearchResult>, CommandError> {
    match ctx.store.get(key) {
        Some(value) => {
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            // Create bounding box for initial filtering
            let bbox = if let Some(radius_m) = opts.radius_m {
                BoundingBox::from_radius(opts.center, radius_m)
            } else {
                BoundingBox::from_box(opts.center, opts.width_m.unwrap(), opts.height_m.unwrap())
            };

            let mut results = Vec::new();

            // Scan all members (could be optimized with geohash range queries)
            for (member, score) in zset.to_vec() {
                let hash = score_to_geohash(score);
                let (lon, lat) = geohash_decode(hash);

                // Quick bounding box check
                if let Some(coords) = Coordinates::new(lon, lat) {
                    if !bbox.contains(coords) {
                        continue;
                    }

                    // Precise distance/box check
                    let dist_m = haversine_distance(opts.center.lon, opts.center.lat, lon, lat);

                    let in_area = if let Some(radius_m) = opts.radius_m {
                        dist_m <= radius_m
                    } else {
                        is_within_box(opts.center, coords, opts.width_m.unwrap(), opts.height_m.unwrap())
                    };

                    if in_area {
                        let dist = if opts.with_dist || opts.asc || opts.desc || opts.store_dist {
                            Some(opts.unit.from_meters(dist_m))
                        } else {
                            None
                        };

                        results.push(GeoSearchResult {
                            member,
                            dist,
                            hash: if opts.with_hash || opts.store_dist { Some(hash) } else { None },
                            coords: if opts.with_coord { Some((lon, lat)) } else { None },
                        });

                        // Early exit if ANY and we have enough results
                        if opts.any && opts.count.map(|c| results.len() >= c).unwrap_or(false) {
                            break;
                        }
                    }
                }
            }

            // Sort results
            if opts.asc {
                results.sort_by(|a, b| {
                    a.dist
                        .unwrap_or(0.0)
                        .partial_cmp(&b.dist.unwrap_or(0.0))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else if opts.desc {
                results.sort_by(|a, b| {
                    b.dist
                        .unwrap_or(0.0)
                        .partial_cmp(&a.dist.unwrap_or(0.0))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }

            // Apply count limit
            if let Some(count) = opts.count {
                results.truncate(count);
            }

            Ok(results)
        }
        None => Ok(vec![]),
    }
}

/// Format search results as a Response.
fn format_geosearch_results(
    results: &[GeoSearchResult],
    opts: &GeoSearchOptions,
) -> Result<Response, CommandError> {
    let need_array = opts.with_coord || opts.with_dist || opts.with_hash;

    let formatted: Vec<Response> = results
        .iter()
        .map(|r| {
            if need_array {
                let mut items = vec![Response::bulk(r.member.clone())];

                if let Some(dist) = r.dist {
                    if opts.with_dist {
                        items.push(Response::bulk(Bytes::from(format_float(dist))));
                    }
                }

                if let Some(hash) = r.hash {
                    if opts.with_hash {
                        items.push(Response::Integer(hash as i64));
                    }
                }

                if let Some((lon, lat)) = r.coords {
                    if opts.with_coord {
                        items.push(Response::Array(vec![
                            Response::bulk(Bytes::from(format_float(lon))),
                            Response::bulk(Bytes::from(format_float(lat))),
                        ]));
                    }
                }

                Response::Array(items)
            } else {
                Response::bulk(r.member.clone())
            }
        })
        .collect();

    Ok(Response::Array(formatted))
}
