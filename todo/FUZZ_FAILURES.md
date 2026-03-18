# Fuzz Failures

Bugs found during initial smoke testing of new fuzz targets (5s each).

## 1. `ts_decompress` — OOM (9.2 GB allocation from 4-byte input)

- **Signal**: `out-of-memory (malloc(9194255360))`
- **Location**: `frogdb_types::timeseries::compression::decode_samples`
- **Cause**: The Gorilla decoder reads a sample count from the header and allocates a `Vec` sized by that count without validating it against the remaining input length. A crafted 4-byte input can claim billions of samples.
- **Fix**: Validate that the declared count is plausible given `data.len()` before allocating.

## 2. `filter_expr_parse` — Stack overflow

- **Signal**: `deadly signal` (SIGBUS/SIGSEGV on stack guard page)
- **Location**: `frogdb_types::vectorset::FilterExpr::parse`
- **Cause**: The recursive descent parser for `AND`/`OR`/`NOT` filter expressions has no depth limit. Deeply nested input like `((((((...))))))` causes unbounded recursion.
- **Fix**: Add a recursion depth counter and return an error when it exceeds a reasonable limit (e.g. 64).

## 3. `search_expr_parse` — Stack overflow

- **Signal**: `deadly signal`
- **Location**: `frogdb_search::expression::parse_expression`
- **Cause**: The Pratt parser for arithmetic/comparison/logical expressions recurses without a depth bound. Pathological input with deep nesting exhausts the stack.
- **Fix**: Track recursion depth in the `Parser` struct and bail out at a threshold.

## 4. `deserialize` — OOM (136 GB allocation)

- **Signal**: `out-of-memory (malloc(136091512720))`
- **Location**: `frogdb_persistence::serialization::deserialize` (type-specific payload paths)
- **Cause**: The improved fuzz target constructs valid headers that route into type-specific deserializers. These read collection lengths (e.g. sorted set member count, list element count) from the payload and allocate accordingly without cross-checking against remaining bytes.
- **Fix**: Each type-specific deserializer should validate that the declared element count is consistent with `payload.len()` before allocating.
