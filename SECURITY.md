# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.9.x   | ✅        |
| < 0.9   | ❌        |

## Reporting a Vulnerability

If you discover a security vulnerability in BEAM, please report it
responsibly:

1. **Preferred:** Use GitHub's private vulnerability reporting feature
   (Security tab → "Report a vulnerability")
2. **Alternative:** Email david.r.newman@proton.me

Please do not open a public GitHub issue for security vulnerabilities.

## Response Timeline

- **Acknowledgment:** Within 72 hours
- **Assessment:** Within 7 days
- **Fix for critical issues:** Within 30 days
- **Disclosure:** After fix is released, or 90 days from report
  (whichever is sooner)

## Security Measures

- Dependency advisories monitored via `cargo-audit` and `cargo-deny`
- All releases pass `cargo audit` before publishing
- CI enforces `clippy -D warnings` and zero compiler warnings
