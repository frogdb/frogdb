# Security Policy

## Supported Versions

FrogDB is pre-1.0. Only the latest release receives security fixes.

## Reporting a Vulnerability

Please report security vulnerabilities through GitHub's private vulnerability reporting:

https://github.com/frogdb/frogdb/security/advisories/new

**Do not open public issues for security reports.**

We will acknowledge receipt within 72 hours and aim to provide an initial assessment
within 7 days.

## Artifact Verification

Release artifacts are signed with [Sigstore cosign](https://docs.sigstore.dev/) (keyless
OIDC). Each `.tar.gz` and `.deb` has a corresponding `.bundle` file containing the
signature and certificate.

To verify:

```bash
cosign verify-blob \
  --bundle frogdb-v0.1.0-linux-amd64.tar.gz.bundle \
  --certificate-identity-regexp "https://github.com/frogdb/frogdb" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  frogdb-v0.1.0-linux-amd64.tar.gz
```

SHA-256 checksums are provided in `sha256sums.txt` (also cosign-signed).

APT repository packages are signed with GPG. The public key is available at:

```bash
curl -fsSL https://frogdb.github.io/frogdb/apt/signing-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/frogdb.gpg
```
