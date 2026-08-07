/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.rs.io.index.db;

import org.lockss.db.*;
import org.lockss.log.L4JLogger;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.lockss.config.db.SqlConstants.*;

public class SQLArtifactIndexDbManagerSql extends DbManagerSql {
  private static final L4JLogger log = L4JLogger.getLogger();

  private static final String
      CREATE_NAMESPACE_TABLE_QUERY = "CREATE TABLE "
      + NAMESPACE_TABLE + " ("
      + NAMESPACE_SEQ_COLUMN + " --BigintSerialPk--,"
      + NAMESPACE_COLUMN + " VARCHAR(" + MAX_NAMESPACE_COLUMN + ") NOT NULL)";

  // Query to create the table for AUIDs and their internal sequence number.
  private static final String
      CREATE_AUID_TABLE_QUERY = "create table "
      + AUID_TABLE + " ("
      + AUID_SEQ_COLUMN + " --BigintSerialPk--,"
      + AUID_COLUMN + " varchar(" + MAX_AUID_COLUMN + ") not null)";

  private static final String CREATE_URL_TABLE_QUERY = "CREATE TABLE "
      + URL_TABLE + " ("
      + URL_SEQ_COLUMN + " --BigintSerialPk--,"
//      + URL_COLUMN + " VARCHAR NOT NULL)";
      + URL_COLUMN + " --PreferUnboundedTextType--)";

  private static final String CREATE_LONG_URL_TABLE_QUERY = "CREATE TABLE "
      + LONG_URL_TABLE + " ("
      + URL_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + URL_TABLE + " (" + URL_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + LONG_URL_COLUMN + " TEXT NOT NULL"
      + ")";

  private static final String
      CREATE_ARTIFACT_TABLE_QUERY = "CREATE TABLE "
      + ARTIFACT_TABLE + " ("
//      + ARTIFACT_UUID_COLUMN + " uuid DEFAULT gen_random_uuid(),"
      + ARTIFACT_UUID_COLUMN + " CHAR(36) NOT NULL,"
      + NAMESPACE_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + NAMESPACE_TABLE + " (" + NAMESPACE_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + AUID_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + AUID_TABLE + " (" + AUID_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + URL_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + URL_TABLE + " (" + URL_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + ARTIFACT_VERSION_COLUMN + " INTEGER NOT NULL,"
      + ARTIFACT_COMMITTED_COLUMN + " BOOLEAN,"
      + ARTIFACT_STORAGE_URL_COLUMN + " VARCHAR(" + MAX_ARTIFACT_STORAGE_URL_COLUMN + ") NOT NULL,"
      + ARTIFACT_LENGTH_COLUMN + " BIGINT NOT NULL,"
      + ARTIFACT_DIGEST_COLUMN + " VARCHAR(" + MAX_ARTIFACT_DIGEST_COLUMN + ") NOT NULL,"
      + ARTIFACT_CRAWL_TIME_COLUMN + " BIGINT NOT NULL"
      + ")";

  // Query to create the table for tracking AU size statistics.
  private static final String
      CREATE_ARCHIVAL_UNIT_SIZE_TABLE_QUERY = "create table "
      + ARCHIVAL_UNIT_SIZE_TABLE + " ("
      + AUID_SEQ_COLUMN + " bigint not null references "
      + AUID_TABLE + " (" + AUID_SEQ_COLUMN + ") on delete cascade,"
      + AU_LATEST_VERSIONS_SIZE_COLUMN + " bigint not null,"
      + AU_ALL_VERSIONS_SIZE_COLUMN + " bigint not null,"
      // TODO: Set to -1 on any changes to the AU, and update only after du if -1
      + AU_DISK_SIZE_COLUMN + " bigint not null,"
      + LAST_UPDATE_TIME_COLUMN + " bigint not null"
      + ")";

  // SQL statements that create the necessary version 1 indices.
  private static final String[] VERSION_1_INDEX_CREATE_QUERIES = new String[]{
      "create unique index idx1_" + AUID_TABLE
          + " on " + AUID_TABLE + "("
          + AUID_SEQ_COLUMN + ")"
  };

  // The SQL code used to create the necessary version 1 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_1_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(AUID_TABLE, CREATE_AUID_TABLE_QUERY);
        put(ARCHIVAL_UNIT_SIZE_TABLE, CREATE_ARCHIVAL_UNIT_SIZE_TABLE_QUERY);
      }};

  // The SQL code used to create the necessary version 2 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_2_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(NAMESPACE_TABLE, CREATE_NAMESPACE_TABLE_QUERY);
        put(URL_TABLE, CREATE_URL_TABLE_QUERY);
        put(ARTIFACT_TABLE, CREATE_ARTIFACT_TABLE_QUERY);
      }};

  private static final String NAMESPACE_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx1_" + NAMESPACE_TABLE + " ON " + NAMESPACE_TABLE + "(" + NAMESPACE_COLUMN + ")";

  private static final String NAMESPACE_SEQ_INDEX_NAMESPACE_TABLE_QUERY =
      "CREATE UNIQUE INDEX idx2_" + NAMESPACE_TABLE + " ON " + NAMESPACE_TABLE + "(" + NAMESPACE_SEQ_COLUMN + ")";

  private static final String NAMESPACE_SEQ_INDEX_ARTIFACT_TABLE_QUERY =
      "CREATE INDEX idx1_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + NAMESPACE_SEQ_COLUMN + ")";

  private static final String AUID_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx2_" + AUID_TABLE + " ON " + AUID_TABLE + "(" + AUID_COLUMN + ")";

  // Not used: Created as part of version 2
  private static final String AUID_SEQ_INDEX_AUID_TABLE_QUERY =
      "CREATE UNIQUE INDEX idx2_" + AUID_TABLE + " ON " + AUID_TABLE + "(" + AUID_SEQ_COLUMN + ")";

  private static final String AUID_SEQ_INDEX_ARTIFACT_TABLE_QUERY =
      "CREATE INDEX idx2_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + AUID_SEQ_COLUMN + ")";

  private static final String UNIQUE_URL_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_COLUMN + ")";

  private static final String URL_SEQ_INDEX_URL_TABLE_QUERY =
      "CREATE UNIQUE INDEX idx2_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_SEQ_COLUMN + ")";

  private static final String URL_SEQ_INDEX_ARTIFACT_TABLE_QUERY =
      "CREATE INDEX idx3_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + URL_SEQ_COLUMN + ")";

  private static final String ARTIFACT_UUID_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx4_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + ARTIFACT_UUID_COLUMN + ")";

  private static final String ARTIFACT_VERSION_INDEX_QUERY =
      "CREATE INDEX idx5_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + ARTIFACT_VERSION_COLUMN + ")";

  private static final String ARTIFACT_STEM_INDEX_QUERY =
      "CREATE INDEX idx6_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "("
          + NAMESPACE_SEQ_COLUMN + ", "
          + AUID_SEQ_COLUMN + ", "
          + URL_SEQ_COLUMN + ")";

  private static final String ARTIFACT_TUPLE_INDEX_QUERY =
      "CREATE INDEX idx7_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "("
          + NAMESPACE_SEQ_COLUMN + ", "
          + AUID_SEQ_COLUMN + ", "
          + URL_SEQ_COLUMN  + ", "
          + ARTIFACT_VERSION_COLUMN + ")";

  private static final String[] VERSION_3_INDEX_CREATE_QUERIES = new String[]{
      NAMESPACE_INDEX_QUERY,
      NAMESPACE_SEQ_INDEX_NAMESPACE_TABLE_QUERY,
      NAMESPACE_SEQ_INDEX_ARTIFACT_TABLE_QUERY,
      AUID_INDEX_QUERY,
      // AUID_SEQ_INDEX_AUID_TABLE_QUERY,
      AUID_SEQ_INDEX_ARTIFACT_TABLE_QUERY,
      UNIQUE_URL_INDEX_QUERY,
      URL_SEQ_INDEX_URL_TABLE_QUERY,
      URL_SEQ_INDEX_ARTIFACT_TABLE_QUERY,
      ARTIFACT_UUID_INDEX_QUERY,
      ARTIFACT_VERSION_INDEX_QUERY,
      ARTIFACT_STEM_INDEX_QUERY,
      ARTIFACT_TUPLE_INDEX_QUERY
  };

  private static final String DROP_UNIQUE_URL_INDEX_QUERY =
      "DROP INDEX idx1_" + URL_TABLE;

  private static final String URL_INDEX_QUERY =
      "CREATE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_COLUMN + ")";

  private static final String LONG_URL_HASH_INDEX_QUERY =
      "CREATE INDEX idx1_" + LONG_URL_TABLE + " ON " + LONG_URL_TABLE + " USING HASH (" + LONG_URL_COLUMN + ")";

  private static final Map<String, String> VERSION_4_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(LONG_URL_TABLE, CREATE_LONG_URL_TABLE_QUERY);
      }};

  private static final String[] VERSION_4_ALTER_TABLE_QUERIES = new String[]{
      DROP_UNIQUE_URL_INDEX_QUERY,
      URL_INDEX_QUERY,
      LONG_URL_HASH_INDEX_QUERY,
  };

  // Version 5 declares the C (byte-order) collation on the text columns that are
  // ordered or range-scanned.
  //
  // For urls.url this is a performance fix: the URL prefix predicates assembled
  // by SQLArtifactIndexManagerSql are of the form
  //   url COLLATE "C" >= ? AND url COLLATE "C" < ?
  // and COLLATE "C" is required there for correctness, because a range is
  // equivalent to a literal prefix match only under byte order. While the column
  // carried the database default collation (en_US.UTF-8, inherited from the
  // cluster), idx1_urls was built in that collation and the planner could not use
  // it for a C-collated predicate, so those queries sequential-scanned.
  // Redeclaring the column rebuilds idx1_urls in the C collation, after which the
  // predicate is index-scannable.
  //
  // For auids.auid this is a correctness fix: the Java-side ordering contract
  // (ArtifactComparators, natural String order) is UTF-16 binary, while the SQL
  // side sorted under the database default. AUIDs are punctuation-dense and
  // glibc's en_US.UTF-8 deweights punctuation, so the two disagree on plain
  // ASCII input.
  //
  // namespaces.namespace is deliberately left alone: it appears only in equality
  // comparisons, which agree under any deterministic collation.
  //
  // The column types are spelled literally rather than via the
  // --PreferUnboundedTextType-- substitution token because executeDdlQueries()
  // does not run localizeCreateQuery() - only createTablesIfMissing() does - so
  // the token would reach the server unsubstituted. Spelling TEXT is exactly what
  // that token expands to on PostgreSQL, and these statements are PostgreSQL-only
  // regardless, since COLLATE is not Derby syntax. That is consistent with the
  // PostgreSQL-only constructs already used by this index (USING HASH here,
  // DISTINCT ON / ON CONFLICT / pg_try_advisory_xact_lock in the manager).
  private static final String URL_COLLATE_C_QUERY =
      "ALTER TABLE " + URL_TABLE + " ALTER COLUMN " + URL_COLUMN
      + " TYPE TEXT COLLATE \"C\"";

  private static final String AUID_COLLATE_C_QUERY =
      "ALTER TABLE " + AUID_TABLE + " ALTER COLUMN " + AUID_COLUMN
      + " TYPE VARCHAR(" + MAX_AUID_COLUMN + ") COLLATE \"C\"";

  // Note the ordering: dropping idx1_urls comes first, so that the ALTER below
  // has one fewer index to rebuild. That is not cosmetic - idx1_urls spans every
  // row of urls, and rebuilding a 108M-row btree only to drop it moments later
  // is the single largest piece of avoidable work in this migration.
  //
  // long_urls.long_url is deliberately NOT collated: this version drops the
  // table outright, so declaring a collation on a column that is about to
  // disappear is pure waste.
  private static final String[] VERSION_5_ALTER_TABLE_QUERIES = new String[]{
      URL_COLLATE_C_QUERY,
      AUID_COLLATE_C_QUERY,
  };

  // ---------------------------------------------------------------------------
  // Also version 5: fold long_urls back into urls.url and restore URL
  // uniqueness.
  //
  // v4 dropped the unique index on urls(url) because the column stores only the
  // first 2500 characters of a long URL, so two distinct long URLs sharing that
  // prefix would collide under it. Nothing replaced the guarantee, so concurrent
  // creators silently produced duplicate url_seq rows for one URL: AU sizes are
  // double-counted, URLs appear twice in listings, and getLatestArtifact's
  // LIMIT 1 picks between them arbitrarily.
  //
  // The fix moves uniqueness onto a digest. urls.url is unbounded TEXT, so it
  // can simply hold the whole URL and long_urls becomes unnecessary; a SHA-256
  // digest is 32 bytes whatever the input, so it fits a btree index row where
  // the URL itself does not. (The 2500-character threshold was never index-safe
  // anyway: the btree limit is 2704 *bytes*, which 2500 multibyte characters
  // exceed.) The digest has to be one nobody can find a collision for, since a
  // collision is a URL that can never be stored rather than merely a slow
  // lookup; see SqlConstants.URL_DIGEST_EXPRESSION for why that rules out MD5
  // and why the spelling is pgcrypto's digest() rather than the built-in
  // sha256().
  //
  // idx1_urls must go, and this is forced rather than chosen. It is a plain
  // btree over urls.url, and a btree index row cannot exceed 2704 bytes; the
  // only reason it was ever legal is that v4 truncated the column to 2500
  // characters. Once urls.url holds whole URLs, inserting a long one fails with
  // "index row size N exceeds btree version 4 maximum 2704" (or, past a page,
  // "index row requires N bytes, maximum size is 8191"). That is precisely the
  // constraint the head/tail split existed to dodge.
  //
  // Consequence: the prefix range predicates built by urlPrefixCondition() lose
  // their index and fall back to a sequential scan. Restoring it needs an index
  // on a bounded expression - CREATE INDEX ON urls (left(url, N) COLLATE "C") -
  // plus a matching leading term in the predicate, with the existing exact range
  // on urls.url kept as the recheck. That is deliberately not done here: it
  // changes urlPrefixCondition(), which belongs with the prefix work, not with
  // this migration.

  // Defensive, and a no-op on every deployment measured so far - long_urls has
  // been found empty everywhere, meaning the truncation path never fired. Kept
  // unconditional so the migration is correct on deployments not yet measured.
  private static final String MERGE_LONG_URL_TAILS_QUERY =
      "UPDATE " + URL_TABLE + " u SET " + URL_COLUMN
      + " = u." + URL_COLUMN + " || lu." + LONG_URL_COLUMN
      + " FROM " + LONG_URL_TABLE + " lu"
      + " WHERE lu." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN;

  // Duplicate groups keyed by digest rather than by the URL text. Grouping
  // 100M+ unbounded TEXT values needs a hash table far larger than any plausible
  // work_mem and degrades into an on-disk sort; md5(url)::uuid is a fixed
  // 16-byte key, which keeps the aggregate in memory. Measured on a 108M-row
  // index: the text grouping was unusable, the digest grouping completed.
  private static final String CREATE_DUP_URL_DIGESTS_QUERY =
      "CREATE TEMP TABLE dup_url_digests AS SELECT md5(" + URL_COLUMN + ")::uuid AS h"
      + " FROM " + URL_TABLE + " GROUP BY 1 HAVING count(*) > 1";

  private static final String INDEX_DUP_URL_DIGESTS_QUERY =
      "CREATE INDEX ON dup_url_digests (h)";

  // Maps every url_seq in a duplicate group to that group's survivor. Note the
  // PARTITION BY is on the URL *text*, not on the digest: if two genuinely
  // distinct URLs ever shared an md5, they would land in separate partitions and
  // each would keep its own url_seq, dropping out of the repoint below. The
  // digest narrows the candidate set; the text decides.
  //
  // That is what makes the md5 above safe to keep, and it is only safe because
  // the constraint built at the end of the migration is on SHA-256. Two
  // md5-colliding but distinct URLs survive this step by design - so had the
  // constraint also been md5, CREATE UNIQUE INDEX would have failed on exactly
  // the rows this step deliberately preserved, aborting the schema upgrade with
  // a raw "Key (md5(url))=(...) is duplicated". Under SHA-256 they index
  // normally, and the two steps no longer contradict each other.
  private static final String CREATE_URL_CANON_QUERY =
      "CREATE TEMP TABLE url_canon AS SELECT u." + URL_SEQ_COLUMN
      + ", min(u." + URL_SEQ_COLUMN + ") OVER (PARTITION BY u." + URL_COLUMN
      + ") AS keep_seq FROM " + URL_TABLE + " u"
      + " JOIN dup_url_digests d ON md5(u." + URL_COLUMN + ")::uuid = d.h";

  private static final String INDEX_URL_CANON_QUERY =
      "CREATE INDEX ON url_canon (" + URL_SEQ_COLUMN + ")";

  // Collapsing url_seqs can, in principle, put two artifacts on the same
  // (namespace, auid, url_seq, version). artifacts has no unique index on that
  // tuple - only on uuid - so the repoint succeeds and leaves two artifacts
  // claiming the same version of the same URL, which reads then resolve
  // arbitrarily via LIMIT 1.
  //
  // The migration proceeds anyway rather than aborting: blocking a schema
  // upgrade on a handful of ambiguous rows is worse operationally than
  // completing it and leaving evidence. The evidence is COLLISION_TABLE, whose
  // existence after the migration is itself the signal that a human needs to
  // decide which artifact in each group is authoritative.
  //
  // Measured zero on a 108M-row production index, which is expected rather than
  // lucky: version assignment filters on the URL text (GET_LATEST_ARTIFACT_-
  // VERSION_QUERY), so duplicate url_seqs already shared one version counter.
  private static final String COUNT_VERSION_COLLISIONS_QUERY =
      "SELECT count(*) FROM (SELECT a." + NAMESPACE_SEQ_COLUMN
      + ", a." + AUID_SEQ_COLUMN + ", c.keep_seq, a." + ARTIFACT_VERSION_COLUMN
      + " FROM url_canon c JOIN " + ARTIFACT_TABLE + " a"
      + " ON a." + URL_SEQ_COLUMN + " = c." + URL_SEQ_COLUMN
      + " GROUP BY 1,2,3,4 HAVING count(*) > 1) d";

  /**
   * Name of the CSV report listing colliding artifacts, written into the
   * repository's data directory. Written only when there is something to
   * report, so that its presence is the alert.
   * <p>
   * A file rather than a table: it survives the database being dumped, reloaded
   * or rebuilt, it can be handed to someone who has no database access, and it
   * does not leave migration scaffolding behind in the schema.
   */
  static final String COLLISION_REPORT_FILE =
      "artifact-index-v5-url-dedup-collisions.csv";

  /** Header of {@link #COLLISION_REPORT_FILE}, matching the SELECT below. */
  private static final String COLLISION_REPORT_HEADER =
      "namespace,auid,url,version,uuid,digest,storage_url,crawl_time";

  // One row per artifact involved in a collision, not one per group, carrying
  // what is actually needed to adjudicate: digest says whether the colliding
  // artifacts are the same bytes recorded twice (the benign case, safe to keep
  // the earliest crawl_time), crawl_time orders them, and storage_url locates
  // each one. Namespace and AUID are resolved to their text rather than left as
  // sequence numbers so the report stands alone without a database to join back
  // to. Ordered so that the members of a group are adjacent.
  private static final String SELECT_VERSION_COLLISIONS_QUERY =
      "SELECT ns." + NAMESPACE_COLUMN
      + ", au." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_UUID_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM url_canon c"
      + " JOIN " + ARTIFACT_TABLE + " a ON a." + URL_SEQ_COLUMN + " = c." + URL_SEQ_COLUMN
      + " JOIN " + URL_TABLE + " u ON u." + URL_SEQ_COLUMN + " = c.keep_seq"
      + " JOIN " + NAMESPACE_TABLE + " ns ON ns." + NAMESPACE_SEQ_COLUMN
      + " = a." + NAMESPACE_SEQ_COLUMN
      + " JOIN " + AUID_TABLE + " au ON au." + AUID_SEQ_COLUMN
      + " = a." + AUID_SEQ_COLUMN
      + " WHERE (a." + NAMESPACE_SEQ_COLUMN + ", a." + AUID_SEQ_COLUMN
      + ", c.keep_seq, a." + ARTIFACT_VERSION_COLUMN + ") IN ("
      + "SELECT a2." + NAMESPACE_SEQ_COLUMN + ", a2." + AUID_SEQ_COLUMN
      + ", c2.keep_seq, a2." + ARTIFACT_VERSION_COLUMN
      + " FROM url_canon c2"
      + " JOIN " + ARTIFACT_TABLE + " a2 ON a2." + URL_SEQ_COLUMN + " = c2." + URL_SEQ_COLUMN
      + " GROUP BY 1,2,3,4 HAVING count(*) > 1)"
      + " ORDER BY 1, 2, 3, 4, 8";

  private static final String REPOINT_ARTIFACTS_QUERY =
      "UPDATE " + ARTIFACT_TABLE + " a SET " + URL_SEQ_COLUMN + " = c.keep_seq"
      + " FROM url_canon c WHERE a." + URL_SEQ_COLUMN + " = c." + URL_SEQ_COLUMN
      + " AND c.keep_seq <> a." + URL_SEQ_COLUMN;

  private static final String DELETE_DUPLICATE_URLS_QUERY =
      "DELETE FROM " + URL_TABLE + " u USING url_canon c"
      + " WHERE u." + URL_SEQ_COLUMN + " = c." + URL_SEQ_COLUMN
      + " AND c.keep_seq <> c." + URL_SEQ_COLUMN;

  private static final String DROP_URL_CANON_QUERY = "DROP TABLE url_canon";

  private static final String DROP_DUP_URL_DIGESTS_QUERY =
      "DROP TABLE dup_url_digests";

  private static final String DROP_LONG_URL_TABLE_QUERY =
      "DROP TABLE " + LONG_URL_TABLE;

  // See the note above: a plain btree over an unbounded urls.url is not a legal
  // index, so this cannot outlive the merge.
  private static final String DROP_URL_BTREE_INDEX_QUERY =
      "DROP INDEX idx1_" + URL_TABLE;

  // ...and its replacement: the same prefix-range capability, over a bounded
  // expression that always fits a btree row. Rebuilt under the original name
  // because it plays the same role.
  //
  // A prefix query for P is answered by the range [left(P,N), successor) on this
  // index. For any P no longer than N that is exact - left(U,N) starts with P if
  // and only if U does - so no recheck is needed. For a longer P the range
  // collapses to equality on left(P,N) and urlPrefixCondition() adds the exact
  // comparison against urls.url as a recheck.
  private static final String URL_PREFIX_INDEX_QUERY =
      "CREATE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE
      + " (left(" + URL_COLUMN + ", " + URL_PREFIX_INDEX_LENGTH + ") COLLATE \"C\")";

  // Supplies digest(), which carries the uniqueness constraint below. See
  // SqlConstants.URL_DIGEST_EXPRESSION for why the constraint needs a function
  // this extension provides rather than one in core PostgreSQL.
  //
  // IF NOT EXISTS because a repository may share a database with something that
  // already installed it. No WITH SCHEMA clause: the extension lands in the
  // first schema on the connection's search_path, which is by construction a
  // schema every later statement resolves through - the same way this class's
  // unqualified table names resolve. Naming a schema explicitly would demand
  // CREATE on that schema instead.
  //
  // pgcrypto is a trusted extension from PostgreSQL 13 on, so the database
  // owner can install it without being a superuser. Verified against
  // PostgreSQL 14 as a NOSUPERUSER NOCREATEDB role owning the database.
  private static final String CREATE_PGCRYPTO_EXTENSION_QUERY =
      "CREATE EXTENSION IF NOT EXISTS pgcrypto";

  // The uniqueness that v4 removed, on a key that fits. Must be created after
  // the dedup: it fails outright if any duplicate remains.
  private static final String UNIQUE_URL_DIGEST_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx4_" + URL_TABLE + " ON " + URL_TABLE
      + " (" + URL_DIGEST_EXPRESSION + ")";

  /**
   * Constructor.
   *
   * @param dataSource          A DataSource with the datasource that provides the connection.
   * @param dataSourceClassName A String with the data source class name.
   * @param dataSourceUser      A String with the data source user name.
   * @param maxRetryCount       An int with the maximum number of retries to be attempted.
   * @param retryDelay          A long with the number of milliseconds to wait between consecutive
   *                            retries.
   * @param fetchSize           An int with the SQL statement fetch size.
   */
  protected SQLArtifactIndexDbManagerSql(DbManager dbMgr, DataSource dataSource, String dataSourceClassName,
                                         String dataSourceUser, int maxRetryCount, long retryDelay, int fetchSize) {
    super(dbMgr, dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
        retryDelay, fetchSize);
  }

  /**
   * Sets up the database to version 1.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void setUpDatabaseVersion1(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_1_TABLE_CREATE_QUERIES);

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_1_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 1 to version 2.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void updateDatabaseFrom1To2(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_2_TABLE_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 2 to version 3.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void updateDatabaseFrom2To3(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_3_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 3 to version 4.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void updateDatabaseFrom3To4(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_4_TABLE_CREATE_QUERIES);

    // Queries to alter tables and indicies
    executeDdlQueries(conn, VERSION_4_ALTER_TABLE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 4 to version 5.
   * <p>
   * Two changes that have to happen together, because each makes the other
   * cheaper or possible:
   * <ul>
   *   <li>Redeclares the ordered / range-scanned text columns with the C
   *       collation, so SQL ordering matches the Java-side contract in
   *       {@code ArtifactComparators}. See {@link #VERSION_5_ALTER_TABLE_QUERIES}.
   *   <li>Folds {@code long_urls} back into {@code urls.url}, removes the
   *       duplicate URL rows that accumulated while no unique constraint
   *       existed, and restores uniqueness on a SHA-256 digest of the URL. See
   *       the version 5 query constants for why the digest carries the
   *       constraint rather than the column, and why it is SHA-256.
   * </ul>
   * <p>
   * Collapsing duplicate URLs can put two artifacts on the same (namespace,
   * auid, url_seq, version). The migration does not stop for that: it records
   * the affected artifacts in {@value #COLLISION_TABLE} and continues, so a
   * handful of ambiguous rows cannot block a schema upgrade. The table is
   * created only when there is something to record, so its presence afterwards
   * is the signal that someone needs to decide which artifact in each group is
   * authoritative. Expected to be empty - measured zero on a 108M-row index.
   * <p>
   * Note for operators:
   * <ul>
   *   <li>This migration installs the {@code pgcrypto} extension, whose
   *       {@code digest} function the restored uniqueness constraint is
   *       declared on. It runs first, so a server without the contrib package
   *       or a connection without database-owner rights fails immediately
   *       rather than after the expensive steps below.
   *   <li>Each {@code ALTER TABLE ... ALTER COLUMN ... TYPE} takes an ACCESS
   *       EXCLUSIVE lock on its table and rebuilds every index on that table. A
   *       collation-only change of an otherwise identical type does not rewrite
   *       the heap, so the cost is index rebuild time - but the table is fully
   *       locked throughout.
   *   <li>The dedup itself is cheap - measured at 98,791 repointed artifact rows
   *       and 16,062 deleted URL rows on a 108M-row index, seconds of work.
   *   <li>{@code CREATE UNIQUE INDEX} over that table is the expensive step. A
   *       {@code CONCURRENTLY} build of the equivalent index measured 16 minutes
   *       there; a plain build under the lock is typically well under that, but
   *       budget for it.
   *   <li>Set {@code maintenance_work_mem} (not {@code work_mem}) and
   *       {@code max_parallel_maintenance_workers} on the migrating connection.
   *       At the 64MB / 2 defaults the index build spills and runs far longer.
   *       TODO: Write code to set these DB parameters - ask Claude
   *   <li>Run {@code ANALYZE urls; ANALYZE auids;} afterwards. {@code ALTER
   *       COLUMN ... TYPE} discards column statistics, and the planner falls
   *       back to default selectivity guesses until they are rebuilt.
   *       Deliberately not done here, so it does not extend the lock.
   *       TODO: Write code to do this
   * </ul>
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void updateDatabaseFrom4To5(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Before anything else, and deliberately: this is the one step that can
    // fail for an environmental reason rather than a data one. Failing it here
    // costs nothing, whereas failing it where digest() is first needed would
    // abort after the table rewrites and the index build - the expensive part
    // of the migration - with no more of the schema converted than this.
    createPgcryptoExtension(conn);

    // First, for two independent reasons. It has to precede the merge, because
    // everything after that widens urls.url past what this index can hold: on a
    // deployment where long_urls is not empty, the merge would fail trying to
    // write an oversized index row. And it has to precede the ALTER below, so
    // that a 108M-row btree is not rebuilt only to be dropped moments later.
    executeDdlQuery(conn, DROP_URL_BTREE_INDEX_QUERY);

    // Queries to alter tables and indicies
    executeDdlQueries(conn, VERSION_5_ALTER_TABLE_QUERIES);

    // Reassemble any truncated URLs before anything depends on urls.url being
    // whole. Expected to affect no rows.
    executeDdlQuery(conn, MERGE_LONG_URL_TAILS_QUERY);

    // Identify the duplicate groups and their survivors.
    executeDdlQuery(conn, CREATE_DUP_URL_DIGESTS_QUERY);
    executeDdlQuery(conn, INDEX_DUP_URL_DIGESTS_QUERY);
    executeDdlQuery(conn, CREATE_URL_CANON_QUERY);
    executeDdlQuery(conn, INDEX_URL_CANON_QUERY);

    // Collapsing duplicate URLs can leave two artifacts claiming the same
    // version of the same URL. That is a data question rather than a schema
    // one, so it is reported and the migration continues.
    long collisions = countVersionCollisions(conn);

    if (collisions > 0) {
      log.warn("Found " + collisions + " URL collisions");
      try {
        File report = writeCollisionReport(conn);

        log.error("Collapsing duplicate URLs left {} (namespace, auid, url,"
                + " version) group(s) holding more than one artifact. The migration"
                + " completed, but those artifacts are now indistinguishable to reads,"
                + " which resolve them arbitrarily. Each group needs a decision about"
                + " which artifact is authoritative; the members are listed in {}"
                + " (identical digests within a group mean the same bytes were"
                + " recorded twice and the earliest crawl_time can simply be kept).",
            collisions, report);
      } catch (Exception e) {
        log.error("Failed to write collision report", e);
      }
    }

    // Collapse the duplicates.
    executeDdlQuery(conn, REPOINT_ARTIFACTS_QUERY);
    executeDdlQuery(conn, DELETE_DUPLICATE_URLS_QUERY);
    executeDdlQuery(conn, DROP_URL_CANON_QUERY);
    executeDdlQuery(conn, DROP_DUP_URL_DIGESTS_QUERY);

    // urls.url now holds every URL whole, so the tail table has no purpose.
    executeDdlQuery(conn, DROP_LONG_URL_TABLE_QUERY);

    // Restore the uniqueness v4 removed. Fails if any duplicate survived above.
    executeDdlQuery(conn, UNIQUE_URL_DIGEST_INDEX_QUERY);

    // Rebuild the prefix index over a bounded expression. Last, so it is built
    // once over the final row set rather than maintained through the dedup.
    executeDdlQuery(conn, URL_PREFIX_INDEX_QUERY);

    log.debug2("Done.");
  }

  /**
   * Installs the {@code pgcrypto} extension, which supplies the {@code digest}
   * function that {@code SqlConstants.URL_DIGEST_EXPRESSION} is built on.
   * <p>
   * The two ways this can fail are environmental rather than data-dependent,
   * and neither is self-explanatory from the PostgreSQL message alone, so the
   * failure is re-reported with what an operator has to do about it: the
   * extension's files may not be installed on the server (they ship in the
   * contrib package, which some distributions package separately), or the
   * database user may be neither a superuser nor the database owner.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if the extension could not be installed.
   */
  private void createPgcryptoExtension(Connection conn) throws SQLException {
    try {
      executeDdlQuery(conn, CREATE_PGCRYPTO_EXTENSION_QUERY);
    } catch (SQLException sqle) {
      String message = "Cannot install the pgcrypto extension, which supplies"
          + " the digest() function that URL uniqueness is declared on."
          + " Install the PostgreSQL contrib package on the server if it is"
          + " missing, and run the migration as the database owner or a"
          + " superuser. SQL = '" + CREATE_PGCRYPTO_EXTENSION_QUERY + "'.";

      log.error(message, sqle);

      // Carry the original SQLState through: the two-argument (String,
      // Throwable) constructor would null it out.
      throw new SQLException(message, sqle.getSQLState(), sqle);
    }
  }

  /**
   * Counts the (namespace, auid, surviving url_seq, version) groups that would
   * hold more than one artifact after duplicate URLs are collapsed.
   *
   * @param conn A Connection with the database connection to be used.
   * @return The number of colliding groups; zero means the repoint is safe.
   * @throws SQLException if any problem occurred accessing the database.
   */
  private long countVersionCollisions(Connection conn) throws SQLException {
    try (PreparedStatement ps =
             prepareStatement(conn, COUNT_VERSION_COLLISIONS_QUERY);
         ResultSet rs = executeQuery(ps)) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }

  /**
   * Writes the colliding artifacts to {@link #COLLISION_REPORT_FILE} in the
   * repository's data directory.
   * <p>
   * Must be called before the repoint, since it reads {@code url_canon} against
   * the pre-repoint {@code artifacts.url_seq}.
   *
   * @param conn A Connection with the database connection to be used.
   * @return The file written.
   * @throws SQLException if the report could not be produced. A migration that
   *         cannot record what it is about to make ambiguous should not proceed
   *         silently, so an I/O failure here is not swallowed.
   */
  private File writeCollisionReport(Connection conn) throws SQLException {
    File dir = getDataDir();
    File report = new File(dir, COLLISION_REPORT_FILE);

    try (PreparedStatement ps =
             prepareStatement(conn, SELECT_VERSION_COLLISIONS_QUERY);
         ResultSet rs = executeQuery(ps);
         BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
             new FileOutputStream(report), StandardCharsets.UTF_8))) {

      int columns = rs.getMetaData().getColumnCount();

      out.write(COLLISION_REPORT_HEADER);
      out.newLine();

      while (rs.next()) {
        StringBuilder line = new StringBuilder();

        for (int i = 1; i <= columns; i++) {
          if (i > 1) {
            line.append(',');
          }
          line.append(csvQuote(rs.getString(i)));
        }

        out.write(line.toString());
        out.newLine();
      }
    } catch (IOException ioe) {
      String message = "Cannot write the URL dedup collision report to " + report;
      log.error(message, ioe);
      throw new SQLException(message, ioe);
    }

    return report;
  }

  /**
   * The directory the collision report is written to: the first configured
   * platform disk space path, falling back to the configured temp directory.
   * <p>
   * Resolved from configuration rather than from the repository, because the
   * database migration runs during {@code startService()}, before a repository
   * exists to ask. This mirrors how {@code BaseLockssManager} picks its default
   * root directory, so the report lands beside the rest of the repository's
   * data.
   *
   * @return An existing, writable directory.
   */
  private File getDataDir() {
    Configuration config = ConfigManager.getCurrentConfig();

    @SuppressWarnings("unchecked")
    List<String> diskSpaceList =
        config.getList(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST);

    String dir = diskSpaceList != null && !diskSpaceList.isEmpty()
        ? diskSpaceList.get(0)
        : config.get(ConfigManager.PARAM_TMPDIR);

    if (dir == null) {
      dir = System.getProperty("java.io.tmpdir");
    }

    File file = new File(dir);
    file.mkdirs();

    return file;
  }

  /**
   * Renders a value as a CSV field. Always quoted: URLs and AUIDs routinely
   * contain commas, and AUIDs contain quotes often enough that unquoted output
   * would be silently malformed.
   */
  private static String csvQuote(String value) {
    if (value == null) {
      return "\"\"";
    }

    return "\"" + value.replace("\"", "\"\"") + "\"";
  }
}
