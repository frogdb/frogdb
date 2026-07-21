# Documentation Rules

- Do not adopt an arrogant tone. Stay grounded.
- Use backticks for:
  - Redis command names/prefixes (eg. `BGSAVE` or `ES.*`)
  - bash commands or specific binary names (`frogctl`)
- FrogDB existing as a single binary is not a selling point for a database. You can mention it when
  relevant, but don't lead with it.
- RESP2/RESP3 are implementation details. When advertising redis compatibility, say "wire-compatible
  with Redis" or similar. If mentioning compatible clients, just say that all standard redis clients are compatible.
- Don't emphasize that it's built in Rust as a marketing point. It's something we can mention when
  relevant, but don't say it like Rust makes the database better.
- Be succinct, but explain details when they matter.

## Include why a behavior matters

Examples:
- "Forkless BGSAVE. BGSAVE triggers a snapshot without a fork() of the server process."
    - Why is this desirable? What are the implications of this? 
- "CLUSTER BUMPEPOCH is not supported."
    - Why is this desirable? What are the implications of this? 

## How **not** to write

Examples from prose previously-written by Claude:

* BAD: "Compatibility and correctness are established by evidence, not assertion."
  * This sentence is a nothing burger. Instead state what is actually meant.
  * GOOD: "Redis compatibility and correctness are achieved through an extensive regression and
    concurrency validation test suite"

## Banned sentence structure

* NO: "it's not x — it's y" or "it's not x, it's y"

## Banned phrases/terminology

- Don't say "speaks" when referring to a protocol. 
  - BAD: "FrogDB speaks RESP2 and RESP3"
  - GOOD: "FrogDB implements RESP2 and RESP3 protocols"
