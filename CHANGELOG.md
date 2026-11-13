# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Breaking:** trace id header is now configurable (`Config.TraceIDHeader`, default `X-Trace-Id`, was `X-PiMail-Trace-Id`); default dir is `/var/lib/pitrace`.
- Sniffer, collector and CLI moved to the hackergate repository; this module is a library only.

### Added
- Remote storage, redaction, `net/http` middleware, `curl.HeaderName`.

### Fixed
- `curl.ProtoToCurl` resolves compact well-known header names and quotes shell-safely.

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security

## [0.1.0] - YYYY-MM-DD

### Added
- Initial release.
