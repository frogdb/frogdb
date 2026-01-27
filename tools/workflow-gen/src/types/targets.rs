//! Build target definitions for cross-compilation.

use serde::Serialize;

/// A build target configuration for GitHub Actions matrix builds.
#[derive(Debug, Clone, Serialize)]
pub struct BuildTarget {
    /// Rust target triple (e.g., "x86_64-unknown-linux-gnu")
    pub target: String,
    /// GitHub Actions runner OS (e.g., "ubuntu-latest")
    pub os: String,
    /// Architecture label (e.g., "amd64", "arm64")
    pub arch: String,
    /// Binary extension (e.g., "" for Unix, ".exe" for Windows)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ext: Option<String>,
}

impl BuildTarget {
    pub fn new(target: &str, os: &str, arch: &str) -> Self {
        Self {
            target: target.to_string(),
            os: os.to_string(),
            arch: arch.to_string(),
            ext: None,
        }
    }

    pub fn with_ext(mut self, ext: &str) -> Self {
        self.ext = Some(ext.to_string());
        self
    }
}

/// Predefined Rust targets for common platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RustTarget {
    X86_64Linux,
    Aarch64Linux,
    X86_64MacOs,
    Aarch64MacOs,
    X86_64Windows,
}

impl RustTarget {
    /// Returns the Rust target triple.
    pub fn triple(&self) -> &'static str {
        match self {
            Self::X86_64Linux => "x86_64-unknown-linux-gnu",
            Self::Aarch64Linux => "aarch64-unknown-linux-gnu",
            Self::X86_64MacOs => "x86_64-apple-darwin",
            Self::Aarch64MacOs => "aarch64-apple-darwin",
            Self::X86_64Windows => "x86_64-pc-windows-msvc",
        }
    }

    /// Returns the GitHub Actions runner OS.
    pub fn runner_os(&self) -> &'static str {
        match self {
            Self::X86_64Linux | Self::Aarch64Linux => "ubuntu-latest",
            Self::X86_64MacOs | Self::Aarch64MacOs => "macos-latest",
            Self::X86_64Windows => "windows-latest",
        }
    }

    /// Returns the architecture label.
    pub fn arch(&self) -> &'static str {
        match self {
            Self::X86_64Linux | Self::X86_64MacOs | Self::X86_64Windows => "amd64",
            Self::Aarch64Linux | Self::Aarch64MacOs => "arm64",
        }
    }

    /// Returns the binary extension.
    pub fn ext(&self) -> &'static str {
        match self {
            Self::X86_64Windows => ".exe",
            _ => "",
        }
    }

    /// Converts to a BuildTarget for matrix configuration.
    pub fn to_build_target(&self) -> BuildTarget {
        BuildTarget {
            target: self.triple().to_string(),
            os: self.runner_os().to_string(),
            arch: self.arch().to_string(),
            ext: if self.ext().is_empty() {
                Some(String::new())
            } else {
                Some(self.ext().to_string())
            },
        }
    }

    /// Returns true if this target requires cross-compilation with zigbuild.
    pub fn needs_zigbuild(&self) -> bool {
        matches!(self, Self::X86_64Linux | Self::Aarch64Linux)
    }

    /// Returns true if this target is a Linux target.
    pub fn is_linux(&self) -> bool {
        matches!(self, Self::X86_64Linux | Self::Aarch64Linux)
    }

    /// Returns true if this target is a macOS target.
    pub fn is_macos(&self) -> bool {
        matches!(self, Self::X86_64MacOs | Self::Aarch64MacOs)
    }
}

/// Linux build targets (for build.yml).
pub fn linux_targets() -> Vec<RustTarget> {
    vec![RustTarget::X86_64Linux, RustTarget::Aarch64Linux]
}

/// Release build targets (for release.yml).
pub fn release_targets() -> Vec<RustTarget> {
    vec![
        RustTarget::X86_64Linux,
        RustTarget::Aarch64Linux,
        RustTarget::X86_64MacOs,
        RustTarget::Aarch64MacOs,
    ]
}

/// Converts a list of RustTargets to a matrix include array.
pub fn targets_to_matrix_include(targets: &[RustTarget]) -> Vec<BuildTarget> {
    targets.iter().map(|t| t.to_build_target()).collect()
}
