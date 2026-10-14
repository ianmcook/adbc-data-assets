# Data assets in ADBC

A design proposal for a **data asset** abstraction in [ADBC](https://arrow.apache.org/adbc/), written in response to [apache/arrow-adbc#4877](https://github.com/apache/arrow-adbc/discussions/4877), "Proposal: ADBC file drivers with pluggable storage."

**Status:** draft, October 13, 2026. Discussion of the design is happening in that thread.

Read the proposal: [proposal.md](proposal.md)

## In short

Curt Hagenlocher's proposal has three parts: file drivers that understand one format, a C ABI for storage that the host supplies, and a registry of storage providers in the driver manager. This proposal keeps the storage ABI and the provider registry and changes the first part. Format logic becomes an **asset adapter**, a smaller loadable component with a scan-oriented ABI, rather than an ADBC driver. The driver manager presents any adapter through the regular ADBC API itself, so every language binding can read an asset through an `asset://` URI with no new API.

Around that change, the proposal defines one abstraction with four layers:

- **Asset descriptors.** A small TOML file, parallel to a connection profile, that says where data lives and what shape it has. It names a format and a location, not a driver. Examples cover an Iceberg catalog, a local directory tree of CSV files, and an S3 prefix of Hive-partitioned Parquet.
- **Components and registries.** Storage providers, format adapters, and table-format adapters, each with its own manifest and registry in the driver manager.
- **A scan model.** Objects are planned into fragments that yield Arrow. Pushdown uses a small, closed predicate form with literals bound as Arrow data rather than Substrait. Fragments are describable, so an engine with its own Parquet reader can bypass the adapter for I/O.
- **Ways in.** The driver manager's built-in asset facade for any ADBC application, mounting assets into a SQL engine through a connection profile, direct fragment access for query engines, and descriptors that tools can resolve on their own.

Writes are designed in from the start as a writer plus a two-phase commit and are phased in. No change to the core ADBC header is required for the first phase.

## Files

- [proposal.md](proposal.md): the design.
- [notes.md](notes.md): a summary of the discussion thread, the prototype files that informed the design, the ADBC specifications used, and prior art.

## License

See [LICENSE](LICENSE).
