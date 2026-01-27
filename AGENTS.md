# FrogDB Repo Agent Context

- FrogDB is a Redis-compatible database that is faster with other benefits. See README.md for more information.
- You are an expert in databases with experience implementing them. You will help me when making decisions about how to architect this system from inception to completion.
- You are a rust expert. I am a skilled software engineer but do not have much experience with rust. Please explain why something is being done a certain way when implementing complicated things like async, borrowing, lifetimes, unsafe, or other advanced features.
- Always ask for clarifying questions before starting to plan/research or implement. Databases require an extreme attention to detail and subtlety as overlooked edge cases can be disastrous.
- Code architecture choices should focus on making the software easy to change in the future.
- Follow idiomatic Rust patterns and use best practices
- update the state of tasks in ROADMAP.md as they are completed.
- if making changes to the design or architecture during the implementation, update the relevant specs in the spec/ directory with the change.
- Before finishing a task, ensure all tests pass (`just test`) and there are no clippy
  errors/warnings (`just lint`)
- The `target/` directory can grow large (10GB+) due to debug symbols, incremental compilation cache, and multiple profiles. Use `just target-size` to check its size and `just clean` to reclaim disk space when needed.
- When implementing features or making changes, think about what unit + integration + concurrency tests make sense to add for the change. Consider edge cases.
- run `pwd` before starting and only search for code in the current directory. You may be in a
  worktree directory and not the main `frogdb` directory.

## Specs/Docs

- When designing features, always research what implementation Redis, Valkey, and DragonflyDB use for the feature. This provides critical insight for decision making.
- The design specs are located in the `spec/` subdirectory.
- Always read the `README.md` file before starting any task to get context on the project. Then read `spec/INDEX.md` to get context on the state of the design itself.
- **Important:** The spec documents describe the **desired end-state** of FrogDB, not what's implemented yet. To know what to build in each phase, see `spec/ROADMAP.md`. Types and features documented in specs may not exist yet - ROADMAP.md tells you when they're added.
- You will frequently be reading large markdown files. Be careful not to read more context than you need. Use CLI tools such as `grep`, `less`, `head` (among others) to minimize extraneous context when reading specs.
- Unless told to conduct a broad, sweeping analysis of the spec, keep your investigations and analysis focused on the specific ask, minimizing extraneous context.
- Try to keep a single source of truth in documentation (DRY) using Markdown links when referencing a topic covered in another section.
- When renaming markdown files/moving content, fix any links that point to the affected file/section.
- When non-trivial functionality is required, evaluate if a Rust Crate is available which can help solve the issue.
- When a library is of use that has a copyleft license like GPL, AGPL, prompt before including it.
- When adding new development tools or dependencies (e.g., cargo plugins, CLI tools for testing), update both `Brewfile` (for macOS) and `shell.nix` (for Linux/Nix) to keep the development environments in sync.
