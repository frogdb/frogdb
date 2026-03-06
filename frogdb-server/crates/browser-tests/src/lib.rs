//! Browser integration tests for FrogDB debug UI.
//!
//! This crate contains browser-based integration tests using Selenium WebDriver
//! via the thirtyfour crate. Tests are gated behind the `browser-tests` feature
//! and require chromedriver to be running.
//!
//! # Running tests
//!
//! 1. Start chromedriver: `chromedriver --port=9515`
//! 2. Run tests: `cargo test -p frogdb-browser-tests --features browser-tests`
//!
//! Tests will be skipped if the `browser-tests` feature is not enabled.
