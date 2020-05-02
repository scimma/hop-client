# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.5] - 2020-05-02
### Changed
- Change package name from `scimma-client` to `hop-client` with
  entrypoint change from `scimma` to `hop`.

### Added
- Allow subscribing of GCN circulars via Kafka via `scimma subscribe`.
- Add `Stream` API to publish/subscribe to Kafka topics.
- Define `VOEvent` and `GCNCircular` models to store, parse and
  serialize messages of their respective types.

## [0.0.4] - 2020-02-24
### Added
- Allow `scimma publish` to take in client configuration options via
  config file (-F /path/to/file) or config options (-X key=val).

## [0.0.2] - 2020-02-04
### Added
- Expose `scimma` CLI.
- Allow publishing of GCN circulars via Kafka via `scimma publish`.
- Expose API to read and parse an RFC 822 formatted GCN circular from
  a text file.

## [0.0.1] - 2020-01-24
### Added
- Initial Release.

[Unreleased]: https://github.com/scimma/hop-client/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/scimma/hop-client/releases/tag/v0.0.5
[0.0.4]: https://github.com/scimma/hop-client/releases/tag/v0.0.4
[0.0.2]: https://github.com/scimma/hop-client/releases/tag/v0.0.2
[0.0.1]: https://github.com/scimma/hop-client/releases/tag/v0.0.1
