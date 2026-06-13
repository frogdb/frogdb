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
    AccessSpec, Arity, BoundingBox, Command, CommandContext, CommandError, CommandFlags,
    CommandSpec, Coordinates, DistanceUnit, EventSpec, KeyAccessFlag, KeySpec, ScoreBound,
    SortedSetValue, Value, WaiterWake, WalStrategy, geohash_calculate_areas, geohash_decode,
    geohash_encode, geohash_score_range, geohash_to_score, geohash_to_string, haversine_distance,
    is_within_box, score_to_geohash,
};
use frogdb_protocol::Response;

use super::utils::{NxXxOptions, format_float, parse_f64, parse_usize};

/// Format a distance value with 4 decimal places, matching Redis's `addReplyDoubleDistance`
/// which uses `%.4f` format.
fn format_distance(dist: f64) -> String {
    format!("{:.4}", dist)
}

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
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEOADD",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistFirstKey,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse options using shared utility
        let mut nx_xx = NxXxOptions::default();
        let mut ch = false;
        let mut i = 1;

        while i < args.len() {
            let arg = args[i].as_ref();
            let upper = arg.to_ascii_uppercase();
            match upper.as_slice() {
                b"NX" | b"XX" => {
                    match nx_xx.try_parse(arg) {
                        Ok(Some(new_opts)) => nx_xx = new_opts,
                        Ok(None) => break,
                        // XX+NX conflict - Redis returns syntax error for GEOADD
                        Err(_) => return Err(CommandError::SyntaxError),
                    }
                    i += 1;
                }
                b"CH" => {
                    ch = true;
                    i += 1;
                }
                _ => break,
            }
        }
        let nx = nx_xx.nx;
        let xx = nx_xx.xx;

        // Parse coordinate-member triplets: must be at least one triplet, divisible by 3
        let remaining = &args[i..];
        if remaining.len() < 3 || !remaining.len().is_multiple_of(3) {
            return Err(CommandError::SyntaxError);
        }

        let zset = ctx.get_or_create::<SortedSetValue>(key)?;
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
}

// ============================================================================
// GEODIST - Get distance between two members
// ============================================================================

pub struct GeodistCommand;

impl Command for GeodistCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEODIST",
            arity: Arity::Range { min: 3, max: 4 },
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
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

                Ok(Response::bulk(Bytes::from(format_distance(dist))))
            }
            None => Ok(Response::null()),
        }
    }
}

// ============================================================================
// GEOHASH - Get geohash strings for members
// ============================================================================

pub struct GeohashCommand;

impl Command for GeohashCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEOHASH",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let members = &args[1..];

        // No members → empty array
        if members.is_empty() {
            return Ok(Response::Array(vec![]));
        }

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                let results: Vec<Response> = members
                    .iter()
                    .map(|member| match zset.get_score(member) {
                        Some(score) => {
                            let (lon, lat) = geohash_decode(score_to_geohash(score));
                            let hash_str = geohash_to_string(lon, lat);
                            Response::bulk(Bytes::from(hash_str))
                        }
                        None => Response::null(),
                    })
                    .collect();

                Ok(Response::Array(results))
            }
            None => Ok(Response::Array(
                members.iter().map(|_| Response::null()).collect(),
            )),
        }
    }
}

// ============================================================================
// GEOPOS - Get coordinates for members
// ============================================================================

pub struct GeoposCommand;

impl Command for GeoposCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEOPOS",
            arity: Arity::AtLeast(1),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let members = &args[1..];

        match ctx.store.get(key) {
            Some(value) => {
                let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                let results: Vec<Response> = members
                    .iter()
                    .map(|member| match zset.get_score(member) {
                        Some(score) => {
                            let (lon, lat) = geohash_decode(score_to_geohash(score));
                            Response::Array(vec![
                                Response::bulk(Bytes::from(format_float(lon))),
                                Response::bulk(Bytes::from(format_float(lat))),
                            ])
                        }
                        None => Response::null(),
                    })
                    .collect();

                Ok(Response::Array(results))
            }
            None => Ok(Response::Array(
                members.iter().map(|_| Response::null()).collect(),
            )),
        }
    }
}

// ============================================================================
// GEOSEARCH - Search members within an area
// ============================================================================

pub struct GeosearchCommand;

impl Command for GeosearchCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEOSEARCH",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];

        // Parse search options (GEOSEARCH does not accept STOREDIST)
        let opts = parse_geosearch_options(&args[1..], ctx, key, false)?;
        let results = execute_geosearch(ctx, key, &opts)?;

        // Format results
        format_geosearch_results(&results, &opts)
    }
}

// ============================================================================
// GEOSEARCHSTORE - Store search results
// ============================================================================

pub struct GeosearchstoreCommand;

impl Command for GeosearchstoreCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEOSEARCHSTORE",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE,
            keys: KeySpec::FirstTwo,
            access: AccessSpec::Uniform,
            wal: WalStrategy::PersistDestination(0),
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: true,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let destkey = &args[0];
        let srckey = &args[1];

        // If source key doesn't exist, delete dest and return 0.
        // We must check this before parsing options because FROMMEMBER
        // would error trying to look up a member in a non-existent key.
        if ctx.store.get(srckey).is_none() {
            // Still need to validate the options syntax, but use a special
            // mode that skips FROMMEMBER member lookup on missing keys.
            let _ = parse_geosearch_options(&args[2..], ctx, srckey, true)?;
            ctx.store.delete(destkey);
            return Ok(Response::Integer(0));
        }

        let opts = parse_geosearch_options(&args[2..], ctx, srckey, true)?;
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
}

// ============================================================================
// GEORADIUS - Legacy search by radius (deprecated)
// ============================================================================

pub struct GeoradiusCommand;

impl Command for GeoradiusCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEORADIUS",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::WRITE.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::Dynamic,
            wal: WalStrategy::Dynamic,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let lon = parse_f64(&args[1])?;
        let lat = parse_f64(&args[2])?;
        let radius = parse_f64(&args[3])?;
        let unit = DistanceUnit::parse(&args[4]).ok_or(CommandError::SyntaxError)?;

        // Parse options
        let extra_opts = &args[5..];
        let radius_opts = parse_georadius_options(extra_opts)?;

        if (radius_opts.store.is_some() || radius_opts.store_dist)
            && (radius_opts.with_coord || radius_opts.with_dist || radius_opts.with_hash)
        {
            return Err(CommandError::InvalidArgument {
                    message: "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options".to_string(),
                });
        }

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
            any: radius_opts.any,
            asc: radius_opts.asc,
            desc: radius_opts.desc,
            store_dist: radius_opts.store_dist,
        };

        let results = execute_geosearch(ctx, key, &opts)?;

        // Handle STORE option: store results in destination key and return count
        if let Some(dest) = radius_opts.store {
            if results.is_empty() {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
            let mut dest_zset = SortedSetValue::new();
            for result in &results {
                let score = if opts.store_dist {
                    result.dist.unwrap_or(0.0)
                } else {
                    geohash_to_score(result.hash.unwrap_or(0))
                };
                dest_zset.add(result.member.clone(), score);
            }
            let count = dest_zset.len();
            ctx.store.set(dest, Value::SortedSet(dest_zset));
            return Ok(Response::Integer(count as i64));
        }

        format_geosearch_results(&results, &opts)
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }
        let mut keys = vec![args[0].as_ref()];
        // Options begin after `key lon lat radius unit` (args[5..]).
        if args.len() > 5
            && let Some(dest) = georadius_store_dest(&args[5..])
        {
            keys.push(dest);
        }
        keys
    }

    fn dynamic_keys_with_flags<'a>(
        &self,
        args: &'a [Bytes],
    ) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        // Source key (args[0]) is read; any STORE destination is written.
        // The write flag drives WalStrategy::Dynamic to persist only the dest.
        self.dynamic_keys(args)
            .into_iter()
            .enumerate()
            .map(|(i, k)| {
                let flag = if i == 0 {
                    KeyAccessFlag::R
                } else {
                    KeyAccessFlag::OW
                };
                (k, vec![flag])
            })
            .collect()
    }
}

// ============================================================================
// GEORADIUSBYMEMBER - Legacy search by member (deprecated)
// ============================================================================

pub struct GeoradiusbymemberCommand;

impl Command for GeoradiusbymemberCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEORADIUSBYMEMBER",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::WRITE.union(CommandFlags::MOVABLEKEYS),
            keys: KeySpec::Dynamic,
            access: AccessSpec::Dynamic,
            wal: WalStrategy::Dynamic,
            wakes: WaiterWake::None,
            event: EventSpec::Suppressed,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        let key = &args[0];
        let member = &args[1];
        let radius = parse_f64(&args[2])?;
        let unit = DistanceUnit::parse(&args[3]).ok_or(CommandError::SyntaxError)?;

        // Parse options first so we know if STORE is set
        let extra_opts = &args[4..];
        let radius_opts = parse_georadius_options(extra_opts)?;

        if (radius_opts.store.is_some() || radius_opts.store_dist)
            && (radius_opts.with_coord || radius_opts.with_dist || radius_opts.with_hash)
        {
            return Err(CommandError::InvalidArgument {
                    message: "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options".to_string(),
                });
        }

        // Get member's coordinates; if key doesn't exist, return early
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
            None => {
                // Key doesn't exist — with STORE, delete dest and return 0
                if let Some(dest) = radius_opts.store {
                    ctx.store.delete(&dest);
                    return Ok(Response::Integer(0));
                }
                return Ok(Response::Array(vec![]));
            }
        };

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
            any: radius_opts.any,
            asc: radius_opts.asc,
            desc: radius_opts.desc,
            store_dist: radius_opts.store_dist,
        };

        let results = execute_geosearch(ctx, key, &opts)?;

        // Handle STORE option: store results and return count
        if let Some(dest) = radius_opts.store {
            if results.is_empty() {
                ctx.store.delete(&dest);
                return Ok(Response::Integer(0));
            }
            let mut dest_zset = SortedSetValue::new();
            for result in &results {
                let score = if opts.store_dist {
                    result.dist.unwrap_or(0.0)
                } else {
                    geohash_to_score(result.hash.unwrap_or(0))
                };
                dest_zset.add(result.member.clone(), score);
            }
            let count = dest_zset.len();
            ctx.store.set(dest, Value::SortedSet(dest_zset));
            return Ok(Response::Integer(count as i64));
        }

        format_geosearch_results(&results, &opts)
    }

    fn dynamic_keys<'a>(&self, args: &'a [Bytes]) -> Vec<&'a [u8]> {
        if args.is_empty() {
            return vec![];
        }
        let mut keys = vec![args[0].as_ref()];
        // Options begin after `key member radius unit` (args[4..]).
        if args.len() > 4
            && let Some(dest) = georadius_store_dest(&args[4..])
        {
            keys.push(dest);
        }
        keys
    }

    fn dynamic_keys_with_flags<'a>(
        &self,
        args: &'a [Bytes],
    ) -> Vec<(&'a [u8], Vec<KeyAccessFlag>)> {
        // Source key (args[0]) is read; any STORE destination is written.
        // The write flag drives WalStrategy::Dynamic to persist only the dest.
        self.dynamic_keys(args)
            .into_iter()
            .enumerate()
            .map(|(i, k)| {
                let flag = if i == 0 {
                    KeyAccessFlag::R
                } else {
                    KeyAccessFlag::OW
                };
                (k, vec![flag])
            })
            .collect()
    }
}

// ============================================================================
// GEORADIUS_RO - Read-only variant of GEORADIUS
// ============================================================================

pub struct GeoradiusRoCommand;

impl Command for GeoradiusRoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEORADIUS_RO",
            arity: Arity::AtLeast(5),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Same logic as GEORADIUS
        GeoradiusCommand.execute(ctx, args)
    }
}

// ============================================================================
// GEORADIUSBYMEMBER_RO - Read-only variant of GEORADIUSBYMEMBER
// ============================================================================

pub struct GeoradiusbymemberRoCommand;

impl Command for GeoradiusbymemberRoCommand {
    fn spec(&self) -> &'static CommandSpec {
        static SPEC: CommandSpec = CommandSpec {
            name: "GEORADIUSBYMEMBER_RO",
            arity: Arity::AtLeast(4),
            flags: CommandFlags::READONLY,
            keys: KeySpec::First,
            access: AccessSpec::Uniform,
            wal: WalStrategy::NoOp,
            wakes: WaiterWake::None,
            event: EventSpec::NotApplicable,
            requires_same_slot: false,
        };
        &SPEC
    }

    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Same logic as GEORADIUSBYMEMBER
        GeoradiusbymemberCommand.execute(ctx, args)
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
    any: bool,
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
///
/// `allow_storedist` controls whether the STOREDIST option is accepted (only
/// valid for GEOSEARCHSTORE, not for GEOSEARCH).
fn parse_geosearch_options(
    args: &[Bytes],
    ctx: &mut CommandContext,
    key: &Bytes,
    allow_storedist: bool,
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

    // Track which source/shape options were specified for conflict detection
    let mut has_from_member = false;
    let mut has_from_lonlat = false;
    let mut has_by_radius = false;
    let mut has_by_box = false;

    let mut i = 0;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"FROMMEMBER" => {
                if has_from_member || has_from_lonlat {
                    return Err(CommandError::SyntaxError);
                }
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                has_from_member = true;
                let member = &args[i + 1];

                // When the key doesn't exist, we skip member lookup — the caller
                // (GEOSEARCHSTORE) will handle returning 0.
                center = match ctx.store.get(key) {
                    Some(value) => {
                        let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

                        let score = zset.get_score(member).ok_or_else(|| {
                            CommandError::InvalidArgument {
                                message: format!(
                                    "could not decode requested zset member: {}",
                                    String::from_utf8_lossy(member)
                                ),
                            }
                        })?;

                        let (lon, lat) = geohash_decode(score_to_geohash(score));
                        Some(Coordinates::new(lon, lat).ok_or_else(|| {
                            CommandError::InvalidArgument {
                                message: "member has invalid coordinates".to_string(),
                            }
                        })?)
                    }
                    None => {
                        // Key doesn't exist — use a dummy center; the caller
                        // will short-circuit before using it.
                        Some(Coordinates::new(0.0, 0.0).unwrap())
                    }
                };
                i += 2;
            }
            b"FROMLONLAT" => {
                if has_from_member || has_from_lonlat {
                    return Err(CommandError::SyntaxError);
                }
                if i + 2 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                has_from_lonlat = true;
                let lon = parse_f64(&args[i + 1])?;
                let lat = parse_f64(&args[i + 2])?;
                center = Some(Coordinates::new(lon, lat).ok_or_else(|| {
                    CommandError::InvalidArgument {
                        message: format!("invalid longitude,latitude pair {},{}", lon, lat),
                    }
                })?);
                i += 3;
            }
            b"BYRADIUS" => {
                if has_by_radius || has_by_box {
                    return Err(CommandError::SyntaxError);
                }
                if i + 2 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                has_by_radius = true;
                let radius = parse_f64(&args[i + 1])?;
                unit = DistanceUnit::parse(&args[i + 2]).ok_or(CommandError::SyntaxError)?;
                radius_m = Some(unit.to_meters(radius));
                i += 3;
            }
            b"BYBOX" => {
                if has_by_radius || has_by_box {
                    return Err(CommandError::SyntaxError);
                }
                if i + 3 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                has_by_box = true;
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
            b"ANY" => {
                return Err(CommandError::InvalidArgument {
                    message: "the ANY option requires the COUNT option".to_string(),
                });
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
                if !allow_storedist {
                    return Err(CommandError::SyntaxError);
                }
                store_dist = true;
                i += 1;
            }
            _ => {
                return Err(CommandError::SyntaxError);
            }
        }
    }

    // Validate required options with Redis-compatible error messages
    if !has_from_member && !has_from_lonlat {
        return Err(CommandError::InvalidArgument {
            message: "exactly one of FROMMEMBER or FROMLONLAT can be provided".to_string(),
        });
    }
    if !has_by_radius && !has_by_box {
        return Err(CommandError::InvalidArgument {
            message: "exactly one of BYRADIUS and BYBOX can be provided".to_string(),
        });
    }

    let center = center.ok_or(CommandError::SyntaxError)?;

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
    let mut any = false;
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
                // Check for ANY after COUNT
                if i < args.len() && args[i].to_ascii_uppercase().as_slice() == b"ANY" {
                    any = true;
                    i += 1;
                }
            }
            b"ANY" => {
                // ANY without COUNT is an error
                return Err(CommandError::InvalidArgument {
                    message: "the ANY option requires the COUNT option".to_string(),
                });
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
                if i + 1 >= args.len() {
                    return Err(CommandError::SyntaxError);
                }
                store = Some(args[i + 1].clone());
                store_dist = true;
                i += 2;
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
        any,
        asc,
        desc,
        store,
        store_dist,
    })
}

/// Extract the STORE/STOREDIST destination key from the option portion of a
/// GEORADIUS-style command (the args after the fixed leading positional args).
///
/// Used by `keys()` for routing, ACL key checks, cluster slot validation, and
/// client-tracking invalidation. Returns the last destination if STORE/STOREDIST
/// appears more than once (Redis applies the last occurrence). This is a
/// position-tolerant scan that mirrors `parse_georadius_options`: only STORE and
/// STOREDIST consume a following key argument, so a destination key that happens
/// to be the literal string "STORE" is handled correctly by the bounds guard.
fn georadius_store_dest(opts: &[Bytes]) -> Option<&[u8]> {
    let mut dest: Option<&[u8]> = None;
    let mut i = 0;
    while i < opts.len() {
        match opts[i].to_ascii_uppercase().as_slice() {
            b"STORE" | b"STOREDIST" if i + 1 < opts.len() => {
                dest = Some(opts[i + 1].as_ref());
                i += 2;
            }
            _ => i += 1,
        }
    }
    dest
}

/// Execute a geo search and return results.
///
/// Uses Redis's 9-area geohash scanning strategy: estimate a coarse geohash
/// step size based on the search radius, encode the center at that step, then
/// iterate the center cell + 8 neighbor cells in order (center, N, S, E, W,
/// NE, NW, SE, SW). Within each cell, members appear in ascending score order.
/// This produces the same default iteration order as Redis.
fn execute_geosearch(
    ctx: &mut CommandContext,
    key: &Bytes,
    opts: &GeoSearchOptions,
) -> Result<Vec<GeoSearchResult>, CommandError> {
    match ctx.store.get(key) {
        Some(value) => {
            let zset = value.as_sorted_set().ok_or(CommandError::WrongType)?;

            // Create bounding box for filtering
            let bbox = if let Some(radius_m) = opts.radius_m {
                BoundingBox::from_radius(opts.center, radius_m)
            } else {
                BoundingBox::from_box(opts.center, opts.width_m.unwrap(), opts.height_m.unwrap())
            };

            // Compute search radius for step estimation and bbox half-dimensions
            let (search_radius_m, bbox_hw, bbox_hh) = if let Some(r) = opts.radius_m {
                (r, r, r)
            } else {
                let w = opts.width_m.unwrap();
                let h = opts.height_m.unwrap();
                (
                    ((w / 2.0).powi(2) + (h / 2.0).powi(2)).sqrt(),
                    w / 2.0,
                    h / 2.0,
                )
            };

            let areas = geohash_calculate_areas(opts.center, search_radius_m, bbox_hw, bbox_hh);

            let mut results = Vec::new();
            let mut last_processed: Option<(u64, u8)> = None;
            let mut early_exit = false;

            for area in &areas {
                // Skip duplicate areas (matching Redis's membersOfAllNeighbors)
                if let Some((prev_bits, prev_step)) = last_processed
                    && area.bits == prev_bits
                    && area.step == prev_step
                {
                    continue;
                }
                last_processed = Some((area.bits, area.step));

                let (min_score, max_score) = geohash_score_range(area);
                let members = zset.range_by_score(
                    &ScoreBound::Inclusive(min_score),
                    &ScoreBound::Exclusive(max_score),
                    0,
                    None,
                );

                for (member, score) in members {
                    let hash = score_to_geohash(score);
                    let (lon, lat) = geohash_decode(hash);

                    if let Some(coords) = Coordinates::new(lon, lat) {
                        if !bbox.contains(coords) {
                            continue;
                        }

                        let dist_m = haversine_distance(opts.center.lon, opts.center.lat, lon, lat);

                        let in_area = if let Some(radius_m) = opts.radius_m {
                            dist_m <= radius_m
                        } else {
                            is_within_box(
                                opts.center,
                                coords,
                                opts.width_m.unwrap(),
                                opts.height_m.unwrap(),
                            )
                        };

                        if in_area {
                            let dist = Some(opts.unit.from_meters(dist_m));

                            results.push(GeoSearchResult {
                                member,
                                dist,
                                hash: Some(hash),
                                coords: if opts.with_coord {
                                    Some((lon, lat))
                                } else {
                                    None
                                },
                            });

                            // Early exit if ANY and we have enough results
                            if opts.any && opts.count.map(|c| results.len() >= c).unwrap_or(false) {
                                early_exit = true;
                                break;
                            }
                        }
                    }
                }

                if early_exit {
                    break;
                }
            }

            // Sort results: explicit ASC/DESC, or implicit ASC when COUNT is set without ANY
            let sort_asc = opts.asc || (!opts.desc && opts.count.is_some() && !opts.any);
            if sort_asc {
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

                if let Some(dist) = r.dist
                    && opts.with_dist
                {
                    items.push(Response::bulk(Bytes::from(format_distance(dist))));
                }

                if let Some(hash) = r.hash
                    && opts.with_hash
                {
                    items.push(Response::Integer(hash as i64));
                }

                if let Some((lon, lat)) = r.coords
                    && opts.with_coord
                {
                    items.push(Response::Array(vec![
                        Response::bulk(Bytes::from(format_float(lon))),
                        Response::bulk(Bytes::from(format_float(lat))),
                    ]));
                }

                Response::Array(items)
            } else {
                Response::bulk(r.member.clone())
            }
        })
        .collect();

    Ok(Response::Array(formatted))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|s| Bytes::from(s.to_string())).collect()
    }

    #[test]
    fn georadius_keys_without_store_is_source_only() {
        let cmd = GeoradiusCommand;
        let a = args(&["src", "15", "37", "200", "km", "ASC"]);
        let keys = cmd.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref()]);
    }

    #[test]
    fn georadius_keys_with_store_includes_destination() {
        let cmd = GeoradiusCommand;
        let a = args(&["src", "15", "37", "200", "km", "STORE", "dest"]);
        let keys = cmd.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref(), b"dest".as_ref()]);
    }

    #[test]
    fn georadius_keys_with_storedist_includes_destination() {
        let cmd = GeoradiusCommand;
        let a = args(&[
            "src",
            "15",
            "37",
            "200",
            "km",
            "COUNT",
            "5",
            "ASC",
            "STOREDIST",
            "dest",
        ]);
        let keys = cmd.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref(), b"dest".as_ref()]);
    }

    #[test]
    fn georadius_keys_uses_last_store_destination() {
        // Redis applies the last STORE/STOREDIST occurrence.
        let cmd = GeoradiusCommand;
        let a = args(&[
            "src",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "first",
            "STOREDIST",
            "second",
        ]);
        let keys = cmd.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref(), b"second".as_ref()]);
    }

    #[test]
    fn georadiusbymember_keys_with_store_includes_destination() {
        let cmd = GeoradiusbymemberCommand;
        let a = args(&["src", "Palermo", "200", "km", "STORE", "dest"]);
        let keys = cmd.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref(), b"dest".as_ref()]);
    }

    #[test]
    fn georadiusbymember_keys_without_store_is_source_only() {
        let cmd = GeoradiusbymemberCommand;
        let a = args(&["src", "Palermo", "200", "km"]);
        let keys = cmd.keys(&a);
        assert_eq!(keys, vec![b"src".as_ref()]);
    }
}
