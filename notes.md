# Sources and notes

Checked October 13, 2026.

## The discussion

[apache/arrow-adbc discussion #4877](https://github.com/apache/arrow-adbc/discussions/4877), "Proposal: ADBC file drivers with pluggable storage," opened by CurtHagenlocher on October 10, 2026 (UTC).

| Date (UTC) | Author | Point |
|---|---|---|
| Oct 10 | CurtHagenlocher | Original proposal: file drivers, a C file-system ABI, a provider registry keyed by URI scheme, pushdown via projection, Substrait filters, limit, and statistics. Non-goals: a query engine in the driver, replacing native Parquet readers, writes in v1. Four questions: scope, the statement-as-scan model, Substrait, and file-system API complexity. |
| Oct 10 | CurtHagenlocher | Could be a language-neutral extension point for Spark (apache/spark#54603). |
| Oct 11 | CurtHagenlocher | Prototypes at `CurtHagenlocher/arrow-adbc` branch `file-api`. |
| Oct 12 | lidavidm | Not opposed, but unsure this is an "ADBC driver." |
| Oct 13 | lidavidm | Could work as a driver or as its own spec. Suggests passing the whole driver manager to a driver so it can use the registry. Points at Spark's native data source PR (apache/spark#59313), OpenDAL, and Arrow Filesystems. Later suggests spinning the driver-manager question into its own discussion. |
| Oct 13 | CurtHagenlocher | Dynamic loading and dependency isolation are the attraction of attaching to ADBC. |
| Oct 13 | amoeba | Asks for a code example. |
| Oct 13 | CurtHagenlocher | Links the ADBC branch and the `datafusion-table-providers` branch `file-drivers`; demo joins an Excel named range with CSVs in a ZIP on S3 through DataFusion, with projection and filter pushdown and 12 partitions. |
| Oct 13 | paleolimbot | SedonaDB has a DataFusion-specific extension mechanism for spatial formats; prefers an expression object over a serialized declarative form; parallel scans matter; would adopt ADBC if performance matches. |
| Oct 14 | ianmcook | Agrees a "data asset" abstraction with interfaces and registries is valuable. Objections: reusing "driver" confuses users; Substrait likely impractical and risks "interoperability theater"; writes need design first; consider rolling together with an Iceberg driver or another object-storage table abstraction; who funds maintainer capacity across language implementations. |
| Oct 14 | ianmcook | Links this proposal at ianmcook/adbc-data-assets and summarizes it: a data asset abstraction instead of reusing "driver," asset descriptors in TOML, and two new kinds of shared libraries, adapters and providers, each with a manifest and installable with dbc. |
| Oct 14 | CurtHagenlocher | Asks whether reading `budget.xlsx` once from Python would require writing a TOML file or registering a custom provider. |
| Oct 14 | ianmcook | Prefers a small predicate language over Substrait. Replies that all parameters could be defined inside an `asset://` URI, analogous to connection profiles coexisting with `drivername://` URIs, and says the proposal will be updated. |

## Curt's prototype

Files in the `file-api` branch that informed the design:

- `c/include/arrow-adbc/adbc_filesystem.h`: `AdbcFileSystem` (open, stat, list, get_extension, clone, release), `AdbcFile` (info, read_ranges, submit_reads, cancel, close), `AdbcFileSystemProvider` (set_option, open with scheme, authority, parent, parent_name, release), nested locations as `zip:<inner>!/<path>`.
- `c/include/arrow-adbc/adbc.h` changes: `ADBC_STATUS_FILESYSTEM_REQUIRED`, `ADBC_INFO_DRIVER_ACCEPTS_FILESYSTEM`, `ADBC_INFO_DRIVER_REQUIRES_FILESYSTEM`, `ADBC_INFO_DRIVER_FILE_EXTENSIONS`, `ADBC_INFO_DRIVER_MIME_TYPES`, and new `AdbcDriver` members `DriverGetInfo`, `DatabaseSetFileSystem`, `ConnectionSetFileSystem`.
- `rust/driver_manager/src/filesystem.rs`: provider manifests with `[ADBC.filesystem] schemes = [...]` and a `[Provider]` table; registry keyed by lowercase scheme; manifests found on a provider path environment variable.
- `c/driver/csv/substrait_filter.{h,cc}`: a hand-written Substrait filter reader, about 640 lines, which is a data point for the cost of the Substrait route even in its minimal form.
- `rust/driver/excel/`: Excel driver in Rust.
- `rust/filesystem/object_store/`, `rust/filesystem/zip/`: S3 and ZIP providers.

## ADBC specifications used

- [Connection profiles (format)](https://arrow.apache.org/adbc/current/format/connection_profiles.html): `profile_version = 1`, optional `driver`, required `[Options]`, `{{ env_var(NAME) }}` substitution, `profile://` URIs and the `profile` option, search locations including `ADBC_PROFILE_PATH` and `~/.config/adbc/profiles/`, option precedence, custom profile providers via `AdbcConnectionProfileProvider`.
- [Driver manifests](https://arrow.apache.org/adbc/current/format/driver_manifests.html): `manifest_version`, `name`, `version`, `publisher`, `license`, `url`, `[ADBC] version`, `[ADBC.features]`, `[Driver] entrypoint`, `[Driver.shared]` keyed by platform tuple.
- `adbc.h` on `main`: ingest options and modes (`create`, `append`, `replace`, `create_append`), statistics keys and schema, object depth constants, `ExecutePartitions` and `ReadPartition`, `SetSubstraitPlan`.

## Related ADBC issues

- [#13 format: generic table select API (dual of bulk ingest)](https://github.com/apache/arrow-adbc/issues/13): June 2026 thread on a table-select API, projection and filter pushdown, Substrait concerns (Protobuf weight), tokoko's position that readers should accept a SQL subset or a Substrait subset, and the point that a local-engine approach cannot distribute reads or writes.
- [#4564 format: generalize current catalog/schema for arbitrarily nested namespaces](https://github.com/apache/arrow-adbc/issues/4564): Iceberg-style nested namespaces do not fit the fixed catalog and schema hierarchy; see also #46 and #320.
- No discussion or issue in apache/arrow-adbc proposes an Iceberg driver directly. The nearest artifact is [tokoko/adbc-iceberg](https://github.com/tokoko/adbc-iceberg), a Rust ADBC driver over iceberg-rust with projection, predicate pushdown, one partition per data file, REST catalog support, and installation through dbc from a GitHub-release registry.

## Prior art consulted from memory

Arrow Datasets; DataFusion `TableProvider::scan` and `supports_filters_pushdown` (Exact, Inexact, Unsupported); Spark DataSource V2 (`Table`, `ScanBuilder`, `SupportsPushDownFilters`, `InputPartition`, `PartitionReaderFactory`, `BatchWrite`, `DataWriter`, `WriterCommitMessage`); Iceberg `FileScanTask`; Delta Kernel's `Engine` interface (file system client, JSON handler, Parquet handler, expression handler) and its own predicate classes; Hadoop `fs.<scheme>.impl`; OpenDAL; fsspec; Intake; Frictionless Data Package.

## Editorial notes

- "Asset" rather than "dataset" avoids a collision with Arrow's Dataset API and matches the word Ian used in the thread.
- The restricted SQL form in the facade is flagged as a judgment call in the proposal, since it is the one place the design lets SQL near the asset path.
- Companies are not named in the capacity section; the proposal frames the funding question structurally and leaves commitments to the discussion.

## Publication

Copied to the repository at `/Users/ian/adbc-data-assets`, published as [ianmcook/adbc-data-assets](https://github.com/ianmcook/adbc-data-assets). The workspace copy is the working draft; sync changes to the repo when publishing.
