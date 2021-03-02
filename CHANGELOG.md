# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2021-03-02
### Changed
- Generally increase user readability of error messages.
- Fix issue in `hop subscribe` when printing JSON-formatted output.
- Better handling of unstructured messages, avoiding excessive nesting
  when serializing messages.
- Treat input from stdin as JSON for `hop publish`.
- Create credential files with safe permissions by default.
- Require that credential files have proper permissions, refusing to
  open configuration files which have over-broad permissions.
- Modify how consumer group names are generated. When using proper
  authentication, prefix the group name with the credential username,
  instead of the local username.

### Added
- Add documentation for optional parameters passed down to lower-level
  Kafka libraries when using `Stream`.

## [0.2] - 2020-09-16
### Changed
- Change `hop auth` to `hop configure`.
- Return message content rather than a Blob model when unpacking
  unstructured messages.

### Added
- Allow custom message formats via external plugins.
- Add `hop configure web` to get credentials through web UI.
- Add `hop configure setup -c <credentials>` to populate configuration
  with credentials.
- Allow use of open Stream instances without context manager interface.
- Expose manual committing of messages to allow fault tolerance within
  applications.

## [0.1] - 2020-07-24
### Changed
- Modify message serialization format passed through the wire to enable
  rapid message classification.
- Simplify API both the CLI and python API, and provide consistency
  between both methods for publishing and subscribing to messages.
- `Stream` now takes in various message models directly for publishing,
   and return the appropriate message model when grabbing messages,
   with a "catch-all" unstructured type.
- Standardize API for message models throughout.
- Authentication is now enabled by default for both the CLI and python
  API. Disabling authentication now needs to be done explicitly.
- Update options in CLI and python API to reflect `adc-streaming` 1.0
  release.

### Added
- Add `hop version` to print out versions of hop-client dependencies.
- Add `hop auth` for various authentication utilities, including
  setting up configuration for configuring various authentication
  options and showing where the default configuration location is.
- Expose `Auth` class for providing custom configuration if not
  reading in auth configuration from a default location.

### Removed
- Remove timeout option in subscribing to messages in CLI and
  python API, instead will listen to messages until an end-of-stream
  (EOS) is received. To listen to messages indefinitely, use persist
  option.
- Remove options to pass in Producer/Consumer configuration options
  directly. Instead, specific options can be configured and a proper
  method for passing in authentication is exposed.

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

[Unreleased]: https://github.com/scimma/hop-client/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/scimma/hop-client/releases/tag/v0.3.0
[0.2]: https://github.com/scimma/hop-client/releases/tag/v0.2
[0.1]: https://github.com/scimma/hop-client/releases/tag/v0.1
[0.0.5]: https://github.com/scimma/hop-client/releases/tag/v0.0.5
[0.0.4]: https://github.com/scimma/hop-client/releases/tag/v0.0.4
[0.0.2]: https://github.com/scimma/hop-client/releases/tag/v0.0.2
[0.0.1]: https://github.com/scimma/hop-client/releases/tag/v0.0.1
