# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |
| 0.x     | :white_check_mark: (latest minor only) |
| < 0.x   | :x:                |

## Reporting a Vulnerability

**Please do not open public issues for security vulnerabilities.**

Report privately via GitHub Security Advisories:
https://github.com/pilab-cloud/pitrace/security/advisories/new

Or email: **gyula@pilab.hu**

Include:

- Description of the vulnerability
- Steps to reproduce / minimal example
- Potential impact
- Suggested fix (optional)

## Response

- Acknowledgement within 7 days.
- Assessment and severity within 14 days.
- Fix released as a patch version; GitHub Security Advisory published.

## Disclosure

Coordinated disclosure: details published only after a fix is available.
CVEs requested for vulnerabilities in pitrace itself.

## Best practices for users

- Always use the latest release.
- Use Go modules (`go get`).
- Keep Go toolchain updated (supported versions only).
