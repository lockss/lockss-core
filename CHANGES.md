# Release Notes

## Major Changes Since 2.0.3.0

*   (highlights go here)

## 2.0.3.0

### Features

*   Inter-component readiness checks remove need for most startup coordination in docker scripts
*   Allow waiting for an external setup of a database
*   Added repository status tables
*   `ListObjects` enhancements to help identify situations affecting V1-V2 repository compatibility
*   Detect, report, don't store identical content in the repository
*   `Artifact` and `ArtifactData` caches improve performance
*   Made the PostgreSQL schema name configurable
*   Added configurable prefix for database names
*   Added a `hostIP` conditional to the configuration XML parser
*   Add service name to alert messages
*   Disambiguate local vs. cluster Expert Config in logs and alerts
*   Login and logout events can be excluded from auditable events
*   Restored Reindex Metadata button in the debug panel
*   Added confirmation screen when synchronizing subscriptions
*   Added display of available AU count to subscription screens
*   `SSLException` during crawl now retried be default
*   `JsoupHtmlLinkExtractor` processes `<source>` and `<track>` tags
*   Added HashCUS filter checkbox
*   Added `-r <credentials_file>` at the command line for the REST credentials file
*   Plugin packager allows loading the signing keystore as resource
*   Plugin packager includes library JARs in the packaged plugin, including a compatibility mode (`-explodelib`) for the classic LOCKSS daemon
*   `genkey` script produces PKCS12 keystore by default

### Fixes

*   Fixed concurrent external update of Derby databases at startup
*   Fixed on-demand AU instantiate/destroy logic
*   `AuEvent` messages were not processed by components without the AU loaded
*   Work around Apache `mod_deflate` ETag bug ([https://bz.apache.org/bugzilla/show_bug.cgi?id=45023#c22](https://bz.apache.org/bugzilla/show_bug.cgi?id=45023#c22))
*   Fixed the poll group logic when a system changes group
*   Properly distinguish publisher site fetch errors from repository errors
*   Fixed incorrect version numbers in CU versions table
*   Fixed bugs with substance checker and redirects
*   Prevent `CrawlStarter` from exiting due to a repository error
*   Repository errors storing permission URL were ignored
*   Fixed incorrect extensions to the future of some synchronized subscriptions
*   Fixed bug in `exitOnce`
*   Multiple services were starting `ConfigDbManager`
*   Fixed accumulating `FileBackedList` temporary files
*   Timely delete temporary files used in PDF parsing
*   Fix `plugin.registryJars` handling
