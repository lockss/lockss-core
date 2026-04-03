# `lockss-core` Release Notes

## 2.10.0 (LOCKSS 2.0.91-beta2)

### Features

* Added pagination support for AU configurations retrieval.
* Added `maybeAuid()` predicate for strings that might be an AUID.
* Default to 1000 lines per page of artifacts.
* Added `plugin_allow_start_url_error` parameter; replaced `isFailOnStartUrlError()` with `isAllowStartUrlError()`.
* Added OpenWayback / PyWb replay links to the AU detail page when those services are configured.
* Applied missing non-default AU config params from TDB at AU config time.
* Send `ConfigChanged` notification if any constituent config file has changed.
* Include startup time in the "Started" log message and alert.
* Added `isKubernetes()` and `isRuncluster()` methods to platform version; disallow blanks in platform version name and handle semantic versions.
* Allow replacement of existing uncommitted artifacts in the repository.
* Configurable REST client connection pool in `RestTemplate`.
* Allow REST client read/connect timeouts to be changed without restart; default read timeout changed to 1 hour.
* Record URL list in `CrawlEvent` only for repair crawls.
* Find the first start URL with content for the default replay URL.
* Added `requested_disposition` query parameter (ported from lockss-daemon).
* Port `ServeContent` rewrite-for-stem-map logic from lockss-daemon.
* Added CDX record endpoint improvements.
* Fully configurable and pipelined Artifact iterator.
* Change default max new content crawl rate to 1 every 5 days.
* Added error injection framework for the repository.
* Implemented support for adding specified versions of artifacts.
* Display LCAP protocol version and migration forwarding status.
* Enabled `AccountManager` by default.
* Merged MDQ and MDX services into a single MD service.
* Added `QueryUrlNormalizer`.
* Revamped network config for migration and non-migration.
* Make `HashCUS` stats human-readable.
* `ArtifactIndex` and `WarcArtifactDataStore` versioning and upgrade support.
* Long URL support.
* SQL artifact iterator replacing list construction and population for improved efficiency.
* Added support for overriding a subset of DBCP configuration parameters using LOCKSS parameters.

### Bug Fixes

* Fixed long-to-int overflow in free space comparisons for WARC file and path selection.
* Fixed database connection leak in `SQLArtifactIndex` when using paging iterator.
* Fixed NPE in `SubstanceChecker` when no substance patterns are configured.
* Fixed race conditions caused by INSERTs into the DB and introduction of LRU caches.
* Fixed masking of fetch errors on start URLs that are also permission URLs.
* Fixed corrupted V0 journal file handling.
* Fixed race condition caused by locking on journal rather than AU stream (preventing corrupted journals).
* Fixed CSV header duplication in CDX output.
* Prevented malformed URL from causing `ServeContent` to throw.
* Prevented `ServeContent` and `ViewContent` from sending incorrect (compressed) content-length.
* Do not override implementation-specific DBCP configuration with defaults when global DBCP configuration is absent.
* Check global excludes when following redirects.
* Account for `ConcurrentHashMap` throwing on null key.
* Fix `ConfigManager` race condition caused by configuring before loading of files.
* Prevent NPEs when a Spring Application Context is not provided.
* Don't rewrite `data:` URLs.
* Allow permission page redirects to take off-host excursion and return to original host/URL.
* Explicitly specify UTF-8 for AUID encoding.
* Handle `CSVRecord` corruption more gracefully.
* Do not block `LockssApp` startup by waiting for it.
* Do not signal ready if deadline is already expired.
* Avoid potential NPE in bug fix.
* Fix logger scope bug/confusion.
* Regex logic error at debug2 logging or higher; trim the candidate URL.
* Populate both maps when loading from persistent store.
* Fixed support for unzipped PostgreSQL data directory.

### Repository / Storage

* Implemented keyset pagination in `SQLArtifactIndex` for consistent byte-order sorting across all locales (`COLLATE "C"`).
* Added persistent UUID support for base paths in `WarcArtifactDataStore`.
* Introduced configurable storage URL path policy (`off`, `warn`, `strict`) to control access paths for artifacts.
* Detect content path changes and clear index before reindexing; implemented `clearIndex()` in all `ArtifactIndex` implementations.
* Implemented `clearReindexState` method and refined base path change detection logic.
* Ensure index reinitialization handles crash scenarios during base path reconfiguration.
* Added bounds check to prevent truncating WARC files beyond their current length.
* Introduced WARC truncation mechanism to rollback partial records on copy failure.
* Normalize base paths at creation to ensure consistency.
* Added LRU caches for namespace, AUID, and URL lookups to avoid unnecessary DB queries; flush caches only when items deleted from corresponding table.
* Prevent index clearing while `ArtifactIndex` is in the `RUNNING` state.
* Enhanced path validation and error handling in `WarcArtifactDataStore`.
* Datastore is now upgraded before index during startup.
* Reindex updates existing storage URL and/or committed flag if present.
* Removed unused `ArtifactResultSetIterator`, related cleaner utilities, and `invalidatedAuSizes` LRU cache.
* Fixed handling of compressed/uncompressed WARC record copies from temporary or permanent WARC files.
* Refactored compressed MIME types list to its own file; WARC record compression now also depends on `Content-Encoding`.
* Handle content-encoding="" property as null.
* Increased buffer size when reading large WARCs.
* Handle corrupted records generically in `readJournalFromWarc()`.
* Exclude journal files from temporary WARC reload.
* Introduced custom `ConcurrentMultiValuedMap` to replace thread-unsafe use of `ArrayListValuedHashMap`.
* Use `PersistentStateManager` for previous crawl state hack and "Interrupted by daemon exit" mechanism; state file writes use temp file and rename.
* Ensure index and datastore `StorageInfo` match exactly when on the same filesystem.
* Guess data/state directory for SQL index `StorageInfo` computation.
* "WARC-local" journaling support.
* WARC journal generalization; artifact state journal refactor.
* Refactored WARC reindex: removed dependency on MapDB, use new journals.
* Refactored data store upgrade logic into `BaseLockssRepository`.
* Introduced `RepositoryStatistics` for tracking iterator reiteration time.
* Isolated Solr `sortUri` field and computation to `ArtifactSolrDocument`.
* Introduced artifacts map keyed by URL for faster queries.
* Added hash index to long URL table.
* Modified `GET_NAMESPACES_QUERY` to return "active" namespaces.
* Reindex signals to start or resume a reindex.
* Refactored queries to use URL columns rather than their concatenation.

### Network Policies (Kubernetes)

* Write and apply Kubernetes `NetworkPolicy` for content access control with configurable managed ports.
* Introduced IP filter output in CIDR format (`/32` for single addresses).
* Introduced `getCidrIntersection()` for IP range computation (using IPAddress library).
* Ensure denied CIDRs are a proper subset of the IP block CIDR.
* Apply ingress rules to all ports; revert to allowing ingress from other pods.

### UI

* Added CSS to every page; switched `DaemonStatus` to use CSS classes for footnote rendering.
* Elaborated content viewing link footnotes; added PyWb before OpenWayback in links.
* Improved `ServeContent` and related link wording and footnotes.
* Added AUID to `ListObjects` header; `ListObjects` now includes comments.
* Indicate preserved file size is compressed in `AuStatus`, `ViewContent`, `ListObjects`.
* Simplified HTML5 `<!DOCTYPE>` declaration across pages.
* Direct user's attention to detailed plugin error messages.
* Improved repository namespace handling and display.
* Added read-only port configuration support in the Content Server Options servlet.
* Form login redirection pattern now defaults to `/LoginForm` (overridable via `UrlUtil.loginRedirectPattern`).
* Fix TinyUI.
* Updated LOCKSS logo to 2.0-beta2.
* Display progress at one percent intervals.

### API Changes

* Introduced `LockssArtifactAlreadyExistsException` which takes an `ArtifactIdentifier` argument.
* Removed `ConfigParamDescr.InvalidFormatException` in favor of `AuParamType.InvalidFormatException`.
* Replaced overloaded `fromArgs()` with a single varargs version.
* Propagated new `IncludeContentEnum` from laaws-crawler-service.
* Refactored URI construction for improved encoding and expandability.
* Encode continuation tokens in artifact list responses.
* Allow matching on a list of artifact versions.
* Refactored `MockLockssDaemon` construction and configuration.

### Dependencies

* Added Lombok.
* Added `json-smart` to the classpath (for the UPN plugin link rewriter).
* Added IPAddress library for CIDR intersection computation.
* Bumped `org.jwat.warc` to 1.2.0.
* Upgraded MySQL Connector/J to 9.2.0 (parameterized version).
* Upgraded XStream (CVE-2024-47072 GHSA-hfq9-hggm-c56q).
* Upgraded MySQL Connector/J version (CVE-2023-22102 GHSA-m6vm-37g8-gqvh).
* Removed moot json.org dependency (CVE-2022-45688 GHSA-3vqj-43w4-2q58, CVE-2023-5072 GHSA-4jq9-2xhw-jpx7).
* Upgraded Joda Time.
* Removed unused apiviz dependency.


## Changes Since 2.0.4.0

*   Switched to a 3-part version numbering scheme.
*   Configuration table displays source(s) of each parameter
*   PlatformConfig lists component config files in load order
*   Support multiple footnotes per item, ordered footnotes, citation styles
*   Ensure ServeContent uses legal filename in Content-Disposition.
*   updated XOAI library
*   Indicate source(s) of each param in Config table
*   updated JUnit, Xerces
*   support for start script wait until started
*   Added alerts: PluginReloaded, PluginJarNotValidated
*   Refactor logic to HttpHttpsUrlHelper (to respect the non-AU-specific API of UrlNormalizer).
*   Changed default lcap ssl proto to TLSv1.2
*   updated mysql-connector
*   Added plugin HTTP result map display
*   Updated keystore generation infrastructure.
*   Added SSL LCAP key generation servlet


## 2.0.4.0

### Features

*   ...

### Fixes

*   ...

## 2.0.3.0

### Features

*   Inter-component readiness checks remove need for most startup coordination in docker scripts.
*   Allow waiting for an external setup of a database.
*   Added repository status tables.
*   `ListObjects` enhancements to help identify situations affecting V1-V2 repository compatibility.
*   Detect, report, don't store identical content in the repository.
*   `Artifact` and `ArtifactData` caches improve performance.
*   Made the PostgreSQL schema name configurable.
*   Added configurable prefix for database names.
*   Added a `hostIP` conditional to the configuration XML parser.
*   Add service name to alert messages.
*   Disambiguate local vs. cluster Expert Config in logs and alerts.
*   Login and logout events can be excluded from auditable events.
*   Restored Reindex Metadata button in the debug panel.
*   Added confirmation screen when synchronizing subscriptions.
*   Added display of available AU count to subscription screens.
*   `SSLException` during crawl now retried be default.
*   `JsoupHtmlLinkExtractor` processes `<source>` and `<track>` tags.
*   Added HashCUS filter checkbox.
*   Added `-r <credentials_file>` at the command line for the REST credentials file.
*   Plugin packager allows loading the signing keystore as resource.
*   Plugin packager includes library JARs in the packaged plugin, including a compatibility mode (`-explodelib`) for the classic LOCKSS daemon.
*   `genkey` script produces PKCS12 keystore by default.

### Fixes

*   Fixed concurrent external update of Derby databases at startup.
*   Fixed on-demand AU instantiate/destroy logic.
*   `AuEvent` messages were not processed by components without the AU loaded.
*   Work around Apache `mod_deflate` ETag bug ([https://bz.apache.org/bugzilla/show_bug.cgi?id=45023#c22](https://bz.apache.org/bugzilla/show_bug.cgi?id=45023#c22)).
*   Fixed the poll group logic when a system changes group.
*   Properly distinguish publisher site fetch errors from repository errors.
*   Fixed incorrect version numbers in CU versions table.
*   Fixed bugs with substance checker and redirects.
*   Prevent `CrawlStarter` from exiting due to a repository error.
*   Repository errors storing permission URL were ignored.
*   Fixed incorrect extensions to the future of some synchronized subscriptions.
*   Fixed bug in `exitOnce`.
*   Multiple services were starting `ConfigDbManager`.
*   Fixed accumulating `FileBackedList` temporary files.
*   Timely delete temporary files used in PDF parsing.
*   Fix `plugin.registryJars` handling.
