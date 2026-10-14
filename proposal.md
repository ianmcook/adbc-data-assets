# Data assets in ADBC: a design proposal

*Draft, October 13, 2026. Written in response to [apache/arrow-adbc discussion #4877](https://github.com/apache/arrow-adbc/discussions/4877), "Proposal: ADBC file drivers with pluggable storage."*

## Summary

Curt's proposal has three parts: file drivers that understand one format, a C ABI for storage that the host supplies, and a registry of storage providers in the driver manager. The storage ABI and the provider registry are sound and should be kept. The part worth changing is the first one. Format logic should not be packaged as an ADBC driver. It should be packaged as an **asset adapter**, a smaller loadable component with a scan-oriented ABI, and the driver manager should present any adapter through the regular ADBC API itself.

Around that change, this document proposes a single abstraction, the **data asset**, with four layers:

1. **Asset descriptors.** A small TOML file, parallel to a connection profile, that says where data lives and what shape it has. It names a format and a location, not a driver. It is resolved by name through an `asset://` URI, or written inline in an `asset:` URI when no file is wanted, and it has value on its own, outside of ADBC code paths.
2. **Components and registries.** Three kinds of dynamically loaded components, each with its own manifest and registry in the driver manager: storage providers (bytes), format adapters (one file format), and table-format adapters (Iceberg, Delta, Lance, and the built-in `files` layout). None of them is a driver.
3. **A scan model.** An asset exposes named objects. A scan of an object is planned into fragments that yield Arrow. Pushdown uses a small, closed predicate form with literals bound as Arrow data, not Substrait. Fragments are describable, so an engine that has its own Parquet reader can bypass the adapter for I/O.
4. **Ways in.** Any ADBC application reaches an asset through the driver manager's built-in **asset facade**, with no new API in any language binding. A connection profile can mount assets into a SQL engine's driver. Engines can consume fragments directly. Tools can resolve a descriptor without scanning anything.

The same model fits an Iceberg catalog, a local directory tree of CSV files, an S3 prefix of Hive-partitioned Parquet, an Excel workbook, and a ZIP of CSVs on S3. Writes are designed in from the start and phased in. No change to `adbc.h` is required for the first phase.

```
 ┌──────────────────────────────────────────────────────────────────┐
 │ Consumers: ADBC apps in any language, query engines, CLI tools   │
 └────────┬────────────────────────┬─────────────────────┬──────────┘
          │ ADBC API (asset://)    │ fragments           │ descriptor
 ┌────────▼────────────────────────▼─────────────────────▼──────────┐
 │ Driver manager                                                   │
 │   asset facade · files composite · host services                 │
 │   registries: drivers · adapters · providers · profiles · assets │
 └────────┬─────────────────────────────────────┬───────────────────┘
          │ adapter ABI                          │ storage ABI
 ┌────────▼───────────────────────┐   ┌─────────▼────────────────────┐
 │ Format adapters                │   │ Storage providers            │
 │   arrow-ipc (built in), csv,   │   │   file (built in), s3, gs,   │
 │   parquet, excel, hdf5, ...    │   │   abfss, http, zip over any  │
 │ Table-format adapters          │   └──────────────────────────────┘
 │   files (built in), iceberg,   │
 │   delta, lance, ...            │
 └────────────────────────────────┘
```

## Where this starts from

Curt's requirements are worth restating because the design below is checked against them. He wants a standard, dynamically loadable way to write format adapters that produce Arrow, written once in any language and loaded by any host. He wants storage to be pluggable and shared so that no format implementation ever contains an S3 client. He wants engines to consume these efficiently through projection and filter pushdown, statistics, and parallel partitioned reads. He wants the changes to ADBC to be minimal and backward compatible, with no query engine inside the component, and he wants to serve the long tail of formats rather than replace native Parquet readers. He also noted that the design could serve as a language-neutral extension point for Spark.

Ian's response agreed that a data asset abstraction with interfaces and registries would be valuable and raised five concerns: reusing "driver" will confuse users who understand ADBC drivers as libraries for talking to systems that speak SQL; Substrait for filters is likely to be impractical without large extensions and risks becoming interoperability theater; writes need a design before the work goes far; the proposal should be considered together with other scope expansions such as an Iceberg driver or another abstraction for tables in object storage; and the maintainer capacity to carry this across ADBC's language implementations needs a funding answer.

David asked whether a file driver is really an ADBC driver, suggested passing the driver manager back into local drivers so they can use its registries, and pointed at OpenDAL and Arrow Filesystems as prior art. Dewey described SedonaDB's similar extension mechanism, preferred an expression object over a serialized declarative form, and said parallel scans matter to them.

## The abstraction

A **data asset** is a named collection of tabular objects together with the information needed to read them. The collection may have one member. Each object has a schema, may have statistics, and can be planned into **fragments**, which are independently readable units that yield Arrow record batches. Fragments carry partition values, statistics, and a residual predicate, and they can be read in parallel or shipped to another process.

This is the model that Arrow Datasets, DataFusion's listing tables, Spark's DataSource V2, Iceberg's scan planning, and Delta Kernel have all converged on. It fits every case in the discussion and every case Ian asked about:

| Asset | Objects | Fragments |
|---|---|---|
| S3 prefix of Hive-partitioned Parquet | One table | Files, or row groups within files, with partition values from the paths |
| Local directory tree of CSV files | One table per file, or one table over all files | Files, or byte ranges within a file |
| Excel workbook | Sheets and named ranges | Sheets, or row chunks |
| ZIP of CSVs on S3 | The CSV files inside the archive | Files |
| HDF5 or NetCDF file | Datasets or variables | Chunks |
| Iceberg catalog | Namespaces and tables | Data files with delete files, residuals, and partition values, as in Iceberg's `FileScanTask` |
| Iceberg, Delta, or Lance table | One table | As above |

Two kinds of adapters produce this model. A **format adapter** understands one file format and reads one file at a time through the storage ABI. A **table-format adapter** understands a layout or a table format and plans fragments itself, reading metadata through the storage ABI or a catalog API. A table-format adapter may emit fragments whose format names a format adapter, so that the host or the engine can read the data files. The Iceberg adapter then becomes a planner that emits Parquet fragments, which is exactly how engines already consume Iceberg.

The driver manager ships one table-format adapter, called `files`, which implements the layout rules for directories, globs, and Hive-style and directory-style partitioning and delegates per-file work to format adapters. It also ships one format adapter, Arrow IPC, and one storage provider, the local file system. These three built-ins exercise every interface and make the system testable end to end with no third-party code.

### Vocabulary

| Term | Meaning | In Curt's proposal |
|---|---|---|
| Data asset | A named collection of tabular objects and how to read them | A file-driver database |
| Asset descriptor | A TOML file that names a format, a location, and options | A URI plus options |
| Object | A table-shaped thing within an asset, named by a path | An object name such as `Sheet1` |
| Fragment | An independently readable unit of an object | A partition from `ExecutePartitions` |
| Format adapter | A loadable component for one file format | A file driver |
| Table-format adapter | A loadable component that plans fragments for a layout or table format | Not present |
| Storage provider | A loadable component that reads bytes for a URI scheme | A storage provider |
| Asset facade | The driver manager's built-in handling of `asset://`, presenting an asset through the ADBC API | The file driver's ADBC surface |
| Host services | A callback table the host hands to an adapter | The file-system object |

"Driver" keeps its current meaning: a library for interacting with a system that executes queries.

## Asset descriptors

An asset descriptor is a TOML file with the same conventions as a connection profile: a version field, a few top-level keys, option tables, `{{ env_var(NAME) }}` substitution in values, and a name-based search path. It differs from a profile in what it answers. A profile answers "how do I connect to this system" and names a driver. A descriptor answers "what data, where, in what shape" and names no driver. Keeping the two separate is what prevents the terminology confusion Ian raised, while letting them compose.

```toml
asset_version = 1
name = "Optional display name"
description = "Optional description"
format = "parquet"              # an adapter name, or "auto" to pick by extension
location = "s3://bucket/prefix/" # a URI; nested locations use Curt's zip:<inner>!/<path> form

[Layout]    # only for file formats; interpreted by the files composite
[Format]    # options passed to the adapter
[Storage.s3]  # options passed to the provider for each scheme in the location
[Schema]    # optional type pins, for partition columns and inference overrides
```

Descriptors live in `~/.config/adbc/assets/` (with platform equivalents matching profiles), are found through `ADBC_ASSET_PATH`, and are referenced as `asset://<name>` or by absolute path. A descriptor can also be written inline in a URI, with no file at all, as described below. The driver manager accepts an `asset://` URI in the `uri` option the same way it accepts `profile://` today. As with profiles, a custom asset provider function can load descriptors from a catalog service or a vault instead of the file system.

### An S3 bucket of Parquet files with Hive-style partitioning

```toml
asset_version = 1
format = "parquet"
location = "s3://acme-lake/events/"

[Layout]
mode = "table"          # all matching files form one table
glob = "**/*.parquet"
partitioning = "hive"   # year=2024/month=07/ segments become columns

[Schema]
year = "int32"          # partition values are strings unless pinned
month = "int32"

[Storage.s3]
region = "us-east-1"
access_key_id = "{{ env_var(AWS_ACCESS_KEY_ID) }}"
secret_access_key = "{{ env_var(AWS_SECRET_ACCESS_KEY) }}"
```

### A local directory with nested subdirectories of data files

```toml
asset_version = 1
format = "csv"
location = "file:///Users/ian/data/exports/"

[Layout]
mode = "collection"     # each file is its own object, named by its relative path
glob = "**/*.csv"

[Format]
header = true
delimiter = ","
```

Listing this asset yields objects such as `2024/jan` and `2024/feb`. The alternative layout, `mode = "table"` with `partitioning = "directory"` and `partition_columns = ["year", "month"]`, treats the same tree as one table with two partition columns, as Arrow's directory partitioning does.

### An Iceberg catalog

```toml
asset_version = 1
format = "iceberg"
location = "https://polaris.example.com/api/catalog"

[Format]
catalog = "rest"
warehouse = "acme"
credential = "{{ env_var(POLARIS_CREDENTIAL) }}"

[Storage.s3]
region = "us-west-2"
```

Objects are namespaces and tables, with nested paths such as `sales/events`. Scans accept table-format options such as a snapshot ID or a timestamp. A single Iceberg table is the same descriptor with a table identifier in `[Format]`, or a `location` that points at a metadata file.

### Curt's demo as two descriptors

```toml
# budget_prices.toml
asset_version = 1
format = "excel"
location = "file:///C:/data/budget.xlsx"
```

```toml
# archived_orders.toml
asset_version = 1
format = "csv"
location = "zip:s3://bucket/archive.zip!/orders/"
[Layout]
mode = "table"
glob = "*.csv"
```

The Excel asset's objects are its sheets and named ranges, including `Prices`. The orders asset is one table over twelve CSVs, read through a ZIP provider layered on an S3 provider.

### Descriptors without a file

Curt asked whether reading `budget.xlsx` once from Python would mean writing a TOML file first or registering a custom asset provider. It would mean neither. Every descriptor key can be given inline in an `asset:` URI, so a descriptor file is for reuse and sharing rather than a prerequisite. This mirrors how the driver manager treats connection parameters today. A `profile://name` reference loads saved options from a file, while a URI such as `postgresql://user@host:5432/db` carries the parameters inline and names the driver by its scheme, and the `driver:scheme://...` form, such as `flightsql:grpc://host:port`, puts the driver name in front of the real URI.

The inline form is:

```
asset:<format>:<location>[?<key>=<value>&...]
```

The two forms are told apart by what follows `asset:`. Two slashes mean a named descriptor or a path to one. Anything else is the inline form. Its first segment names a format adapter, or `auto` to pick one by file extension. The rest, up to the first `?`, is the location. It may be a URI or a local path, and it may be a nested location such as `zip:s3://bucket/archive.zip!/orders/`; a location that needs a query string of its own must percent-encode it. The query parameters are the descriptor's keys flattened with dots: `layout.mode`, `layout.partitioning`, `format.header`, `storage.s3.region`, `schema.year`. Keys beginning with `format.` are adapter options, and the adapter name itself comes from the URI. Values are percent-encoded, and `{{ env_var(NAME) }}` works in them as it does in a file.

Curt's workbook, read once from Python, with the named range `Prices` selected as the object to scan:

```python
import adbc_driver_manager as adbc
import pyarrow as pa

with adbc.AdbcDatabase(uri="asset:excel:file:///C:/data/budget.xlsx") as db, adbc.AdbcConnection(db) as con:
    with adbc.AdbcStatement(con) as stmt:
        stmt.set_options(**{"adbc.asset.object": "Prices"})
        stream, _ = stmt.execute_query()
        prices = pa.RecordBatchReader._import_from_c(stream.address).read_all()
```

A tool that only sends query strings can do the same with the facade's optional shorthand, `SELECT * FROM "Prices"`; see the facade section below.

The Hive-partitioned Parquet asset from above, inline:

```
asset:parquet:s3://acme-lake/events/?layout.partitioning=hive&schema.year=int32&schema.month=int32&storage.s3.region=us-east-1
```

The driver manager turns an inline URI into the same resolved descriptor that a file would produce, and the same precedence applies as for profiles: descriptor values first, then options set through `AdbcDatabaseSetOption`, which can also set individual descriptor keys under an `adbc.asset.` prefix to override one value without rewriting the URI. The inline form is accepted anywhere a named asset is, including the `uri` option, the `[Assets]` table of a connection profile, and the driver manager's resolve function, which can also write an inline URI out as a descriptor file once a one-off becomes something worth keeping.

The format is deliberately not the first segment on its own. A URI such as `excel:file:///C:/data/budget.xlsx` would fall under the driver manager's existing `driver:scheme:` rule and make adapter names compete with driver names, which is the confusion this proposal is trying to avoid. The `asset:` prefix says unambiguously that the facade handles the URI.

## Components, manifests, and registries

The driver manager already has one registry, for drivers, fed by TOML manifests on a search path. This design adds two more, with the same mechanics: a manifest per component, platform-specific shared library paths, `ADBC_ADAPTER_PATH` and `ADBC_PROVIDER_PATH`, and user directories `~/.config/adbc/adapters/` and `~/.config/adbc/providers/`. A package manager such as dbc can install all three kinds, since they are manifests plus shared libraries.

An adapter manifest carries what Curt put into `GetInfo`:

```toml
manifest_version = 1
name = "Excel"
version = "0.1.0"
publisher = "..."
license = "Apache-2.0"

[ADBC.asset]
kind = "format"               # or "table-format"
formats = ["excel"]
extensions = ["xlsx", "xlsm"]
mime_types = ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]
features = ["read", "statistics", "predicate-pushdown"]   # add "write" when supported

[Adapter]
entrypoint = "AdbcAssetAdapterExcelInit"
[Adapter.shared]
osx_arm64 = "libadbc_asset_excel.dylib"
linux_amd64 = "libadbc_asset_excel.so"
```

### Provider manifests

A provider manifest is the one Curt prototyped, and it follows the driver manifest conventions. The `[ADBC.filesystem]` table declares the URI schemes the library serves and whether the provider is layered, meaning it sits on top of another file system the way an archive does. The `[Provider]` table names the shared library, as a single path or a table keyed by platform, with relative paths resolved against the manifest, and an optional entrypoint that defaults to `AdbcFileSystemProviderInit`. An `[options]` table holds scalar configuration that the driver manager passes to the provider through `set_option` before the first open. Options a host supplies at load time are applied after the manifest's, so a manifest holds defaults and a host or a descriptor overrides them. In the prototype, a scheme that is not yet registered is looked up in the directories named by `ADBC_FILESYSTEM_PROVIDER_PATH`; this proposal adds a user directory, as drivers and profiles have.

The S3 provider from the prototype, configured against a local S3 emulator:

```toml
manifest_version = 1
name = "ADBC S3 provider"

[ADBC.filesystem]
schemes = ["s3"]
layered = false

[Provider]
entrypoint = "AdbcFileSystemProviderInit"   # the default
[Provider.shared]
osx_arm64 = "libadbc_filesystem_object_store.dylib"
linux_amd64 = "libadbc_filesystem_object_store.so"
windows_amd64 = "adbc_filesystem_object_store.dll"

[options]
# The prototype's S3 provider accepts object_store's S3 keys.
aws_region = "us-east-1"
aws_endpoint = "http://127.0.0.1:5555"
aws_allow_http = true
```

The ZIP provider, which reads an archive stored in any other file system:

```toml
manifest_version = 1
name = "ADBC ZIP provider"

[ADBC.filesystem]
schemes = ["zip"]
layered = true

[Provider]
shared = "libadbc_filesystem_zip.dylib"
```

A location such as `zip:s3://bucket/archive.zip!/orders/` resolves in two steps. The driver manager sees the `zip` scheme, finds that its provider is layered, resolves the inner location `s3://bucket/archive.zip` through the S3 provider, and hands the resulting file system and the name `archive.zip` to the ZIP provider as its parent. Neither provider knows about the other. In this proposal, the `[Storage.s3]` and `[Storage.zip]` tables of an asset descriptor are the per-asset overrides for each provider in that chain, applied on top of the manifest's `[options]`. That is where credentials belong when the provider does not pick them up from its own environment, and the descriptor's `{{ env_var(NAME) }}` substitution keeps them out of the file.

### The storage ABI

Curt's `adbc_filesystem.h` should be adopted largely as it stands: a file system with open, stat, optional list, extension lookup, clone, and release; a file with info, batched range reads in blocking and asynchronous forms, cancel, and close; a provider with options and an open that supports nested locations. Two additions are proposed. First, a write extension, reached through `get_extension`, that opens a file for streaming writes with commit-or-abort semantics, so that providers can support multipart uploads without changing the read path. Second, a statement that an engine may supply its own file system through host services, so that adapters inherit the engine's credentials, caching, and I/O scheduler. Both were anticipated in Curt's text.

### The adapter ABI

An adapter is a versioned C function table in the ADBC style. The sketch below omits `struct_size`, `version`, and `private_data` fields and shows object paths as single strings for brevity; the real ABI passes paths as arrays of segments.

```c
struct AdbcAssetAdapter {
  AdbcStatusCode (*set_option)(self, const char* key, const char* value, AdbcError*);
  AdbcStatusCode (*open)(self, struct AdbcHost* host, const char* location,
                         struct AdbcFileSystem* fs,   /* NULL for catalog-backed adapters */
                         struct AdbcAsset* out, AdbcError*);
  void (*release)(self);
};

struct AdbcAsset {
  AdbcStatusCode (*list_objects)(self, const char* parent, int depth,
                                 ArrowArrayStream* out, AdbcError*);
  AdbcStatusCode (*get_schema)(self, const char* object, ArrowSchema* out, AdbcError*);
  AdbcStatusCode (*get_statistics)(self, const char* object,
                                   ArrowArrayStream* out, AdbcError*);   /* ADBC statistics schema */
  AdbcStatusCode (*plan_scan)(self, const char* object, const struct AdbcScanRequest* request,
                              struct AdbcScanPlan* out, AdbcError*);
  AdbcStatusCode (*read_fragment)(self, const uint8_t* token, size_t token_length,
                                  ArrowArrayStream* out, AdbcError*);
  AdbcStatusCode (*open_writer)(self, const char* object, const ArrowSchema* schema,
                                const char* mode, struct AdbcAssetWriter* out, AdbcError*);  /* optional */
  AdbcStatusCode (*commit)(self, const char* object, const ArrowArray* receipts, AdbcError*); /* optional */
  void (*release)(self);
};

struct AdbcScanRequest {
  const char* const* columns;  size_t num_columns;   /* projection; nested paths allowed */
  const char* predicate;                             /* compact predicate text, see below */
  const ArrowSchema* literal_schema;  const ArrowArray* literals;  /* one row of bound literals */
  int64_t limit;                                     /* -1 for none */
  const char* const* option_keys;  const char* const* option_values;  size_t num_options;
};

struct AdbcScanPlan {
  AdbcStatusCode (*get_schema)(self, ArrowSchema* out, AdbcError*);      /* output schema, with partition columns */
  AdbcStatusCode (*get_pushdown)(self, ArrowArrayStream* out, AdbcError*); /* per conjunct: exact, inexact, none */
  AdbcStatusCode (*get_fragments)(self, ArrowArrayStream* out, AdbcError*);
  void (*release)(self);
};
```

`read_fragment` sits on the asset rather than the plan so that another process can open the same asset from its descriptor and read a token it was handed. Tokens are opaque, self-contained, and valid across processes for the same adapter version and storage configuration. That is what makes the design usable from Spark executors or any other distributed reader, which tokoko noted a local-engine approach cannot offer.

### Host services

David's question about passing the driver manager back to drivers has a concrete answer here. The host hands every adapter an `AdbcHost` callback table rather than the whole driver manager. It offers: open a file system for a location, using the provider registry or the engine's own storage layer; get a format adapter by name; read a fragment emitted by another adapter; parse a predicate into a tree so adapters need not write their own parser; and report warnings. The `files` composite and the Iceberg adapter both use these to delegate data-file reading to the Parquet adapter. A host that is an engine rather than the driver manager implements the same table, which is the pattern Delta Kernel uses with its engine interface.

### What moves out of `adbc.h`

Curt's branch adds `DriverGetInfo`, `DatabaseSetFileSystem`, and `ConnectionSetFileSystem` to the `AdbcDriver` table, four `ADBC_INFO_DRIVER_*` codes, and a `FILESYSTEM_REQUIRED` status. None of these is needed in this design. Identification moves to adapter manifests, the file system moves to the adapter `open` call, and the ADBC surface is provided by the facade. The first phase changes the driver manager and adds two sibling headers, `adbc_filesystem.h` and `adbc_asset.h`, in the same way that profiles and manifests were added without touching the core specification.

## The scan model

### Projection, predicates, and limit

Projection is a list of column names or nested paths. Limit is an integer, mainly for previews. Predicates are the contentious part.

The proposal is a small, closed predicate language rather than Substrait. A predicate is a boolean combination of atoms using `AND`, `OR`, and `NOT`. An atom compares a column to a bound literal with `=`, `<>`, `<`, `<=`, `>`, or `>=`, tests membership with `IN`, or tests `IS NULL` and `IS NOT NULL`. Literals never appear inline. They are written as `$1`, `$2`, and so on, and they arrive as a one-row Arrow batch whose schema gives each literal an exact Arrow type. The text form is tiny to parse and tiny to emit, and the driver manager ships a reference parser in C++ and Rust that adapters can call through host services.

```
region <> $1 AND qty >= $2 AND (order_date >= $3 OR order_date IS NULL)
```

The top-level `AND` splits the predicate into conjuncts. For each conjunct, the plan reports whether the adapter applied it exactly, applied it inexactly so the engine must re-check, or did not apply it. This keeps Curt's three-way report and matches what DataFusion asks of a table provider.

Spark's DataSource V2 filters, Iceberg's expression API, Delta Kernel's predicates, and Arrow Datasets each define a small predicate language of their own at the plugin boundary. None of them uses Substrait for this, and the design follows them. Substrait extended expressions remain available as an optional richer tier that an adapter can advertise in its manifest, so an engine that already speaks Substrait can send it when both sides agree. The mandatory core stays small enough to specify completely.

### Fragments

`get_fragments` returns an Arrow stream with a fixed schema:

| Column | Type | Meaning |
|---|---|---|
| `token` | binary, not null | Opaque; pass to `read_fragment` |
| `format` | utf8 | A format adapter name when the fragment is a readable file or range, else null |
| `location` | utf8 | URI of the underlying file, when there is one |
| `byte_offset`, `byte_length` | int64 | A range within the file, when the fragment is a range |
| `row_count` | int64 | When known |
| `partition_values` | struct | Columns derived from the layout or table partitioning |
| `residual` | utf8 | Predicate text the reader must still apply, or null |
| `statistics` | list of struct | Same shape as `AdbcConnectionGetStatistics` |
| `extras` | map of utf8 to binary | Format-specific detail, such as Iceberg delete files and field-ID mappings |

An engine that does not understand a fragment's format calls `read_fragment` and gets Arrow. An engine that does understand it, say a Parquet fragment with a byte range and a residual, can read the file natively and skip the adapter. This answers Curt's non-goal about not replacing excellent native readers in a stronger form: the adapter never competes with the engine's reader, and the storage ABI's performance matters less for the hot formats because the engine does its own I/O there.

### Statistics and parallelism

Object-level statistics reuse the ADBC statistics schema. Per-fragment statistics ride in the fragment stream, which gives engines the per-partition statistics Curt listed as a likely addition. Parallel reads fall out of fragments, and Dewey's requirement for parallelized scans is met without a separate mechanism.

## Writes

Ian asked that writes be considered before the work goes far. The model handles three kinds of targets. Immutable-file formats such as Parquet, CSV, and Arrow IPC are written by producing new files. Table formats such as Iceberg, Delta, and Lance are written by producing data files and then committing metadata. Mutable containers such as an Excel workbook or an HDF5 file are rewritten by their adapter, which is allowed to say it cannot append.

The ABI is a writer plus a two-phase commit:

```c
struct AdbcAssetWriter {
  AdbcStatusCode (*write)(self, const ArrowArray* batch, AdbcError*);
  AdbcStatusCode (*finish)(self, ArrowArray* receipt_out, AdbcError*);  /* opaque receipt */
  AdbcStatusCode (*abort)(self, AdbcError*);
  void (*release)(self);
};
```

A single-process write opens one writer, writes batches, finishes, and passes the receipt to `commit`. A distributed write opens one writer per worker, collects the receipts on a coordinator, and commits once. This is the shape of Spark's `DataWriter` and `WriterCommitMessage` and of Iceberg's own commit protocol. Modes reuse ADBC's ingest vocabulary: `create`, `append`, `replace`, and `create_append`. For the `files` composite, `append` adds files under the layout's partitioning, routing rows by partition values; `replace` removes only files the asset's glob would have matched; neither is atomic on object stores, which the specification should say plainly. For table formats, the adapter performs the commit and gets whatever atomicity the format offers. Data files are written through the storage write extension, and an Iceberg adapter writes its Parquet data files by asking host services for the Parquet adapter's writer.

Through the facade, `AdbcStatementBind` with `adbc.ingest.target_table` and `adbc.ingest.mode` performs the single-process form, so bulk ingest into an asset looks exactly like bulk ingest into a database.

The first phase implements reads only, but the ABI reserves the writer and commit entry points, the manifest `features` list includes `write`, and the storage write extension is specified alongside the read path so that providers are not designed twice.

## Ways in

### Any ADBC application, through the asset facade

The driver manager treats `asset://` in the `uri` option the way it treats `profile://` today, and no `driver` option is required.

A scan is a statement with options for the object, the columns, the predicate, and the limit. Predicate literals arrive through `Bind`. A single-object asset, such as a `table` layout, needs no object option.

```python
import adbc_driver_manager as adbc
import pyarrow as pa

with adbc.AdbcDatabase(uri="asset://acme_events") as db, adbc.AdbcConnection(db) as con:
    with adbc.AdbcStatement(con) as stmt:
        stmt.set_options(**{"adbc.asset.columns": "region,qty", "adbc.asset.limit": "10"})
        stream, _ = stmt.execute_query()
        print(pa.RecordBatchReader._import_from_c(stream.address).read_all())
```

For tools that only know how to send a query string, the facade also accepts a restricted SQL shorthand that it parses into the same scan request: `SELECT region, qty FROM events LIMIT 10`. The shorthand is parsed once in the driver manager and never reaches an adapter. In the Python DB-API wrapper, `cursor.execute` takes a query string, which is why the SQL-free form above uses the lower-level statement objects; that is a binding ergonomics question rather than a design one, and bindings can add a convenience for scans.

The facade maps the asset onto the ADBC API:

| ADBC call | Facade behavior |
|---|---|
| `AdbcDatabase` with `uri = asset://...` | Resolve the descriptor, open storage, load the adapter |
| `AdbcConnection` | A session with a metadata cache |
| `GetObjects`, `GetTableSchema`, `GetTableTypes` | The asset's objects, schemas, and object types such as sheet or named range |
| `GetStatistics` | Object-level statistics |
| `GetInfo` | Adapter name and version as the vendor; the driver manager as the driver |
| Statement options `adbc.asset.object`, `.columns`, `.predicate`, `.limit` plus `Bind` | A scan; this is the facade's interface |
| `SetSqlQuery` | Optional shorthand: `SELECT <columns or *> FROM <object> [WHERE <predicate>] [LIMIT n]`, parsed into the same scan request, with inline literals lifted into bound parameters |
| `ExecuteQuery` | All fragments as one stream |
| `ExecutePartitions`, `ReadPartition` | Fragment tokens |
| `Bind` with `adbc.ingest.*` | A write through the adapter |

Anything beyond a scan, such as a join or an aggregate, returns `NOT_IMPLEMENTED` with a message that says to mount the asset into an engine. Whether to ship the SQL shorthand at all is a judgment call. It keeps tools like databow and every DB-API consumer working unchanged, which is most of the standalone value, but it is the one place SQL comes near the asset path. The design works without it, and dropping it would draw a harder line between adapters and SQL systems at the cost of the standalone story.

This is where Curt's mapping of ADBC concepts survives. Users see a database, a connection, objects, and partitioned statements, exactly as he described. Format authors implement none of it.

### A SQL engine, through a connection profile

A profile gains an `[Assets]` table that mounts assets by name into the connection:

```toml
profile_version = 1
driver = "duckdb"

[Options]
path = ":memory:"

[Assets]
events = "asset://acme_events"
prices = "asset://budget_prices"
```

When the driver advertises asset mounting, the driver manager passes each resolved descriptor through a connection option, `adbc.asset.mount.events`, and the driver turns it into a view using its own native readers; DuckDB would issue a `read_parquet` over the S3 prefix with Hive partitioning and configure its own S3 secret from the descriptor's storage options. When the driver does not advertise it, an opt-in fallback scans the asset through the facade and bulk-ingests it into a temporary table of the same name. The fallback costs a materialization, but it gives every SQL driver that supports ingest access to every asset, and it needs no change to any driver. Advertising native mounting is the one addition this design eventually wants in `adbc.h`, as a feature code, and it can be prototyped as an option.

### A query engine, through fragments

DataFusion, DuckDB, Velox, and Spark integrate at the adapter and fragment level. A DataFusion table provider resolves a descriptor, calls `plan_scan` with its projection, filters, and limit, maps the pushdown report onto `supports_filters_pushdown`, and turns fragments into partitions, reading Parquet fragments natively and other fragments through `read_fragment`. Spark's DataSource V2 maps one to one: a table is an object, a scan builder is a scan request, input partitions are fragment tokens serialized to executors, and a batch write is a writer per task with receipts committed by the driver. Curt's DataFusion prototype carries over with the ADBC statement replaced by the scan plan.

### Tools and people, through the descriptor alone

A descriptor is useful before any adapter runs. It is a vendor-neutral, git-committable pointer to a dataset with secrets kept in environment variables, which today has no standard form; people paste S3 paths, Hive flags, and credentials into each tool separately. The driver manager exposes a resolve function that returns the descriptor as structured data with substitutions applied on request, so a tool can validate a descriptor, list an asset's objects, preview a table, or translate the descriptor into an engine's native call without going through ADBC statements. A package manager can install the adapters and providers a descriptor needs, and a CLI can preview or export an asset by name. Orchestration and lineage tools gain a stable name for a dataset that is shared across engines.

## How this answers the discussion

**Reusing "driver."** Format logic becomes an adapter with a scan-oriented ABI and its own registry. Users meet one new noun, the asset, through a URI scheme they already know the shape of. Nothing in the asset path is installed, listed, or documented as a driver.

**Substrait.** The mandatory predicate form is small, closed, and fully specified, with literals typed by Arrow. Substrait is an optional tier that two parties can agree to use. The project commits to specifying a dozen operators, not to extending Substrait.

**Writes.** The writer and two-phase commit are in the ABI from the start, modes reuse the ingest vocabulary, and the storage write extension is specified with the read path. Implementation is phased.

**One abstraction or several.** One abstraction, with two adapter kinds that share an ABI and compose. An Iceberg adapter emits Parquet fragments that a Parquet adapter or an engine reads. The risk of the unification is that table-format detail bloats the shared fragment schema; the opaque token and the `extras` map contain it, and each table-format adapter documents its own fragment profile.

**Capacity and the support matrix.** The surface the ASF project owns is deliberately small: the descriptor format, the two ABIs, the fragment schema and predicate grammar, the driver manager behaviors, three tiny built-ins, and conformance suites for adapters and providers. There are two core implementations, the C++ driver manager and the Rust driver manager, which is the matrix the project already maintains; Python, Go, R, Ruby, Java, and JavaScript inherit the facade through the C++ driver manager with no new binding API. Every real adapter and provider, including Excel, HDF5, Parquet, Iceberg, S3, and ZIP, lives outside the ASF repository and is installed from third-party registries, as tokoko's Iceberg driver already is. A conformance suite makes a sparse matrix explicit rather than silent. One governance rule would keep the matrix honest: a phase enters the specification only when both driver managers implement it and at least two adapters from different maintainers pass conformance. The funding question remains a question, but it is a question about two driver managers and a test suite rather than about every language binding.

**Curt's four questions.** This is in scope for ADBC as a driver-manager-level specification, which is where profiles and manifests already live, and the two ABIs are usable by engines that never touch the rest of ADBC. "A statement is a scan of one object plus pushdown" is the right model for the ABI, expressed as a scan plan rather than a statement; the facade maps it onto statements, and `SetSqlQuery` means the optional restricted shorthand there and nothing to an adapter. Substrait is optional, as above. The file-system ABI can stay close to the prototype because engines that care most about I/O performance supply their own file system or read fragments natively.

## Phasing

**Phase 1, read files.** Descriptors and `asset://`. The storage ABI and provider registry as prototyped. The adapter ABI for reads. The `files` composite with table and collection layouts and Hive and directory partitioning. Adapter manifests and the registry. The facade with structured scans, partitioned execution, and the optional SQL shorthand. Built-ins: the local provider, the Arrow IPC adapter. Conformance tests for providers and adapters. Externally, Curt's Excel and CSV code and S3 and ZIP providers become the first third-party components.

**Phase 2, table formats and writes.** The table-format adapter kind and the `format` and `extras` fragment columns. The writer, commit, and storage write extension. Facade ingest. Profile `[Assets]` mounts with the ingest fallback and the native mount option. An Iceberg adapter outside the repository.

**Phase 3, engine integration.** Host services supplied by engines, fragment interception in DataFusion and DuckDB, a Spark DataSource V2 bridge, and the optional Substrait tier.

## What carries over from the prototype

The `adbc_filesystem.h` header, the provider registry and manifests, the nested-location syntax, and most of the Excel and CSV format logic carry over. The DataFusion table provider carries over with the scan plan in place of the statement. The `adbc.h` additions are dropped: the `AdbcDriver` entry points, the `ADBC_INFO_DRIVER_*` codes, and the new status code move into the adapter ABI and the adapter manifest.

## Open questions

- The exact predicate grammar, in particular whether `OR` within a conjunct and string prefix matching are in the mandatory core.
- The encoding of object paths in SQL identifiers and the mapping of nested paths onto `GetObjects`, which has a fixed catalog and schema hierarchy (see apache/arrow-adbc#4564).
- How much of Iceberg's `FileScanTask` belongs in the shared fragment columns versus `extras`.
- Whether the facade should be a build-time option of the driver manager.
- Whether the facade ships the restricted SQL shorthand at all, or only the structured scan options.
- Schema pinning syntax in `[Schema]`, and schema unification rules across files in a `table` layout.
- The trust model for descriptors found on a search path, which point at local paths and name environment variables; it should match profiles.
- Whether assets can reference a profile for credentials, or whether storage options stay self-contained as drafted.

## Prior art

Arrow Datasets (dataset, fragment, file format, partitioning, file system). DataFusion listing tables and `supports_filters_pushdown`. Spark DataSource V2 (table, scan builder, input partitions, partition reader factory, batch write, commit messages, and its small filter classes). Iceberg scan planning and `FileScanTask`. Delta Kernel's engine interface and its own predicate language. Hadoop's `fs.<scheme>.impl` registry. OpenDAL and fsspec. Intake catalogs and Frictionless data packages as descriptor formats. ADBC's own connection profiles and driver manifests, whose conventions this design copies on purpose.
