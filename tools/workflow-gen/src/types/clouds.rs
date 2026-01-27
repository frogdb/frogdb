//! Cloud provider types for deployment workflows.

use serde::Serialize;

/// Cloud provider options for deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CloudProvider {
    Aws,
    Gcp,
    Azure,
}

impl CloudProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Gcp => "gcp",
            Self::Azure => "azure",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Aws, Self::Gcp, Self::Azure]
    }

    pub fn all_str() -> Vec<&'static str> {
        vec!["aws", "gcp", "azure"]
    }
}

/// Deployment environment options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Dev,
    Staging,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Staging => "staging",
            Self::Production => "production",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Dev, Self::Staging, Self::Production]
    }

    pub fn all_str() -> Vec<&'static str> {
        vec!["dev", "staging", "production"]
    }
}

/// Terraform action options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TerraformAction {
    Plan,
    Apply,
    Destroy,
}

impl TerraformAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Apply => "apply",
            Self::Destroy => "destroy",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![Self::Plan, Self::Apply, Self::Destroy]
    }

    pub fn all_str() -> Vec<&'static str> {
        vec!["plan", "apply", "destroy"]
    }
}
