/*

Copyright (c) 2000-2025, Board of Trustees of Leland Stanford Jr. University

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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.tuple.Pair;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.rest.repo.model.VersionsEnum;
import org.lockss.util.time.TimeBase;

import java.lang.ref.Cleaner;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.lockss.config.db.SqlConstants.*;

/**
 * Manages SQL interactions for the Artifact Index, handling queries and operations
 * related to namespaces, AUIDs, URLs, and artifacts. This class is critical for
 * facilitating the storage, retrieval, and manipulation of artifact data in the
 * underlying database.
 *
 * The class supports operations such as creating or finding sequences for namespaces,
 * AUIDs, and URLs, fetching artifacts using keyset pagination, and performing updates
 * and deletions of artifacts or metadata.
 */
public class SQLArtifactIndexManagerSql {
  private static final L4JLogger log = L4JLogger.getLogger();

  protected final SQLArtifactIndexDbManager idxDbManager;

  /** Page size for PagingArtifactIterator. Default is PagingArtifactIterator.DEFAULT_PAGE_SIZE. */
  private int pageSize = PagingArtifactIterator.DEFAULT_PAGE_SIZE;

  private static final String EMPTY_STRING = "";

  /**
   * Matches a URL exactly, driving off the unique index on {@code md5(url)}.
   *
   * <p>Uniqueness is enforced on a digest rather than on the column because a
   * btree index row is capped at 2704 bytes and a URL is not bounded at all.
   * The {@code md5} term is the part the index can serve; the {@code url}
   * equality term is what makes the match exact, so a digest collision can
   * return no row but never the wrong one. Both terms are required.
   *
   * <p>No {@code LIMIT 1}: the unique index guarantees at most one match.
   */
  private static final String FIND_URL_SEQ_QUERY = "SELECT "
      + URL_SEQ_COLUMN
      + " FROM " + URL_TABLE
      + " WHERE md5(" + URL_COLUMN + ") = md5(?)"
      + " AND " + URL_COLUMN + " = ?";

  /**
   * Race-free URL insert. Returns the new sequence, or no row at all when a
   * concurrent transaction won the race.
   *
   * <p>{@code DO NOTHING} rather than {@code DO UPDATE}: during a re-crawl
   * nearly every URL already exists, and {@code DO UPDATE} would burn a
   * sequence value and leave a dead tuple on every repeat. The caller falls
   * back to {@link #FIND_URL_SEQ_QUERY} on the empty result, which is the rare
   * path because the LRU absorbs most repeats before they reach the database.
   *
   * <p>The conflict target must be spelled exactly as the index expression is.
   */
  private static final String UPSERT_URL_QUERY = "INSERT INTO "
      + URL_TABLE
      + "(" + URL_SEQ_COLUMN
      + "," + URL_COLUMN
      + ") VALUES (default,?)"
      + " ON CONFLICT (md5(" + URL_COLUMN + ")) DO NOTHING"
      + " RETURNING " + URL_SEQ_COLUMN;

  private static final String FIND_NAMESPACE_SEQ_QUERY = "SELECT "
      + NAMESPACE_SEQ_COLUMN
      + " FROM " + NAMESPACE_TABLE
      + " WHERE " + NAMESPACE_COLUMN + " = ?"
      + " LIMIT 1";

  /**
   * Race-free namespace insert, driving off the unique index on
   * {@code namespaces(namespace)}.
   *
   * <p>{@code DO UPDATE} rather than {@code DO NOTHING}: it always returns a
   * row, so there is no fallback path to write. That costs a burned sequence
   * value and a dead tuple per conflict, which is irrelevant here because
   * namespaces are created a handful of times in a repository's life. The URL
   * upsert makes the opposite trade for the opposite reason.
   */
  private static final String UPSERT_NAMESPACE_QUERY = "INSERT INTO "
      + NAMESPACE_TABLE
      + "(" + NAMESPACE_SEQ_COLUMN
      + "," + NAMESPACE_COLUMN
      + ") VALUES (default,?)"
      + " ON CONFLICT (" + NAMESPACE_COLUMN + ") DO UPDATE SET "
      + NAMESPACE_COLUMN + " = EXCLUDED." + NAMESPACE_COLUMN
      + " RETURNING " + NAMESPACE_SEQ_COLUMN;

  private static final String GET_NAMESPACES_QUERY = "SELECT DISTINCT "
      + "ns." + NAMESPACE_COLUMN
      + " FROM " + NAMESPACE_TABLE + " ns"
      + " WHERE EXISTS ( SELECT FROM "
      + ARTIFACT_TABLE + " a,"
      + NAMESPACE_TABLE + " ns"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN + ")";

  // Query to find an AUID's internal AUID sequence number
  private static final String FIND_AUID_SEQ_QUERY = "select "
      + AUID_SEQ_COLUMN
      + " from " + AUID_TABLE
      + " where " + AUID_COLUMN + " = ?"
      + " LIMIT 1";

  /**
   * Race-free AUID insert, driving off the unique index on
   * {@code auids(auid)}. {@code DO UPDATE} for the same reason as
   * {@link #UPSERT_NAMESPACE_QUERY}: AUID creation is once-per-AU, so an
   * occasional dead tuple is not worth a fallback path.
   */
  private static final String UPSERT_AUID_QUERY = "insert into "
      + AUID_TABLE
      + "(" + AUID_SEQ_COLUMN
      + "," + AUID_COLUMN
      + ") values (default,?)"
      + " ON CONFLICT (" + AUID_COLUMN + ") DO UPDATE SET "
      + AUID_COLUMN + " = EXCLUDED." + AUID_COLUMN
      + " RETURNING " + AUID_SEQ_COLUMN;

  // Query for the AU sizes of an AU associated with an AUID
  private static final String GET_AU_SIZE_QUERY = "select "
      + "s." + AU_LATEST_VERSIONS_SIZE_COLUMN
      + ", s." + AU_ALL_VERSIONS_SIZE_COLUMN
      + ", s." + AU_DISK_SIZE_COLUMN
      + ", s." + LAST_UPDATE_TIME_COLUMN
      + " from " + AUID_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_SIZE_TABLE + " s"
      + " where a." + AUID_COLUMN + " = ?"
      + " and a." + AUID_SEQ_COLUMN + " = s."
      + AUID_SEQ_COLUMN;

  private static final String GET_ARTIFACT_BY_UUID_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", au." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " au"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = au." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND a." + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String GET_LATEST_ARTIFACT_VERSION_QUERY = "SELECT "
      + " MAX(a." + ARTIFACT_VERSION_COLUMN + ")"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?";

  private static final String GET_LATEST_ARTIFACT_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?";

  private static final String ARTIFACT_COMMITTED_STATUS_CONDITION =
      " AND a." + ARTIFACT_COMMITTED_COLUMN + " = ?";

  private static final String ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE =
      " AND a." + ARTIFACT_COMMITTED_COLUMN + " IS TRUE";

  /**
   * Placeholder for the literal URL-prefix range predicate, replaced at query
   * assembly time with the fragment built by
   * {@link #urlPrefixCondition(String, boolean)}. Runtime substitution is
   * required because the two forms of the predicate bind different numbers of
   * parameters.
   *
   * <p>Deliberately <em>not</em> spelled {@code --UrlPrefixCondition--} like the
   * sibling {@code --KeysetCondition--} placeholder: {@code --} starts a SQL line
   * comment, and because these queries are assembled as single-line Java string
   * concatenations, an unreplaced {@code --Token--} would silently comment out
   * everything after it, including {@code ORDER BY} and {@code LIMIT}. An
   * unreplaced {@code @@UrlPrefixCondition@@} is a syntax error instead, which
   * fails loudly.
   */
  private static final String URL_PREFIX_CONDITION_TOKEN = " @@UrlPrefixCondition@@ ";

  /**
   * The AUID sort key, used both as the secondary {@code ORDER BY} term of the
   * all-AUIDs queries and as the corresponding term of
   * {@link #KEYSET_WHERE_CLAUSE_ALL_AUIDS}. The two <em>must</em> use the same
   * expression: if the keyset comparison and the sort disagree on collation,
   * pagination silently skips or repeats rows at page boundaries.
   *
   * <p>{@code COLLATE "C"} makes the SQL ordering byte-order, matching the
   * Java-side contract in {@code ArtifactComparators}, whose AUID tiebreaker is a
   * bare {@code thenComparing(Artifact::getAuid)}, i.e. natural (UTF-16 binary)
   * {@code String} order. The database default collation is inherited from the
   * cluster and is generally a locale collation; glibc's {@code en_US.UTF-8}
   * deweights punctuation at the primary level, and AUIDs are punctuation-dense,
   * so the two orderings genuinely disagree on plain ASCII input.
   *
   * <p>Redundant once {@code auids.auid} is declared {@code COLLATE "C"} by
   * schema version 5 - the planner normalizes an explicit collation that matches
   * the column's declared collation, and still uses the index - but retained for
   * the same reason as the {@code COLLATE "C"} on the URL predicates: it keeps
   * the queries correct if the database is ever restored into a
   * differently-collated cluster.
   */
  private static final String SORT_AUID_EXPR =
      "auid." + AUID_COLUMN + " COLLATE \"C\"";

  private static final String GET_ARTIFACT_WITH_VERSION_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?"
      + " AND a." + ARTIFACT_VERSION_COLUMN + " = ?";

  private static final String MAX_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY = "SELECT "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN + ","
      + " MAX(" + ARTIFACT_VERSION_COLUMN + ") latest_version"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " --CommittedStatusCondition-- "
      + " GROUP BY "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN;

  private static final String MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY = "SELECT "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN + ","
      + " MAX(" + ARTIFACT_VERSION_COLUMN + ") latest_version"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " GROUP BY "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN;

  private static final String GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_AUID_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " INNER JOIN ( --MaxVersionAllUrlsWithNamespaceAndAuid-- ) m ON"
      + " m." + NAMESPACE_SEQ_COLUMN + " = a." + NAMESPACE_SEQ_COLUMN
      + " AND m." + AUID_SEQ_COLUMN + " = a." + AUID_SEQ_COLUMN
      + " AND m." + URL_SEQ_COLUMN + " = a." + URL_SEQ_COLUMN
      + " AND m.latest_version = a." + ARTIFACT_VERSION_COLUMN
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_ARTIFACTS_WITH_NAMESPACE_AND_AUID_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " --CommittedStatusCondition-- "
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_COMMITTED_ARTIFACTS_WITH_NAMESPACE_AUID_URL_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?"
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + ARTIFACT_VERSION_COLUMN + " DESC";

  // Latest version artifact for each AUID, for a given namespace and URL
  private static final String MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_URL_QUERY = "SELECT "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN + ","
      + " MAX(" + ARTIFACT_VERSION_COLUMN + ") latest_version"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?"
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " GROUP BY "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN;

  // Latest version artifact for each AUID, for a given namespace and URL
  // Latest version artifact for each AUID, for a given namespace and URL prefix
  private static final String MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_URL_PREFIX_QUERY = "SELECT "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN + ","
      + " MAX(" + ARTIFACT_VERSION_COLUMN + ") latest_version"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + URL_PREFIX_CONDITION_TOKEN
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " GROUP BY "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN;

  // Latest version artifact for each AUID, for a given namespace and URL prefix
  private static final String GET_ARTIFACTS_WITH_NAMESPACE_AND_URL_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?"
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + " " + SORT_AUID_EXPR + " ASC,"
      + " a." + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_ARTIFACTS_WITH_NAMESPACE_AND_URL_PREFIX_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + URL_PREFIX_CONDITION_TOKEN
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + " " + SORT_AUID_EXPR + " ASC,"
      + " a." + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_URL_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " INNER JOIN (" + MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_URL_QUERY + ") m ON"
      + " m." + NAMESPACE_SEQ_COLUMN + " = a." + NAMESPACE_SEQ_COLUMN
      + " AND m." + AUID_SEQ_COLUMN + " = a." + AUID_SEQ_COLUMN
      + " AND m." + URL_SEQ_COLUMN + " = a." + URL_SEQ_COLUMN
      + " AND m.latest_version = a." + ARTIFACT_VERSION_COLUMN
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + " " + SORT_AUID_EXPR + " ASC,"
      + " a." + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_URL_PREFIX_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " INNER JOIN (" + MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_URL_PREFIX_QUERY + ") m ON"
      + " m." + NAMESPACE_SEQ_COLUMN + " = a." + NAMESPACE_SEQ_COLUMN
      + " AND m." + AUID_SEQ_COLUMN + " = a." + AUID_SEQ_COLUMN
      + " AND m." + URL_SEQ_COLUMN + " = a." + URL_SEQ_COLUMN
      + " AND m.latest_version = a." + ARTIFACT_VERSION_COLUMN
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + " " + SORT_AUID_EXPR + " ASC,"
      + " a." + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_ARTIFACTS_WITH_NAMESPACE_AUID_URL_PREFIX_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + URL_PREFIX_CONDITION_TOKEN
      + ARTIFACT_COMMITTED_STATUS_CONDITION
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AUID_AND_URL_PREFIX_QUERY = "SELECT "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN + ","
      + " MAX(" + ARTIFACT_VERSION_COLUMN + ") latest_version"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + URL_PREFIX_CONDITION_TOKEN
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE
      + " GROUP BY "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN;

  private static final String GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AUID_URL_PREFIX_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + ", replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\" sortUri"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " INNER JOIN (" + MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AUID_AND_URL_PREFIX_QUERY + ") m ON"
      + " m." + NAMESPACE_SEQ_COLUMN + " = a." + NAMESPACE_SEQ_COLUMN
      + " AND m." + AUID_SEQ_COLUMN + " = a." + AUID_SEQ_COLUMN
      + " AND m." + URL_SEQ_COLUMN + " = a." + URL_SEQ_COLUMN
      + " AND m.latest_version = a." + ARTIFACT_VERSION_COLUMN
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " --KeysetCondition-- "
      + " ORDER BY "
      + " sortUri ASC,"
      + ARTIFACT_VERSION_COLUMN + " DESC";

  private static final String GET_AUIDS_BY_NAMESPACE_QUERY = "SELECT DISTINCT "
      + "auid." + AUID_COLUMN
      + " FROM " + AUID_TABLE + " auid"
      + " WHERE EXISTS ( SELECT FROM "
      + ARTIFACT_TABLE + " a,"
      + NAMESPACE_TABLE + " ns"
      + " WHERE a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + ")";

  private static final String UPDATE_ARTIFACT_COMMITTED_QUERY = "UPDATE " + ARTIFACT_TABLE
      + " SET " + ARTIFACT_COMMITTED_COLUMN + " = ?"
      + " WHERE " + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String UPDATE_ARTIFACT_STORAGE_URL_QUERY = "UPDATE " + ARTIFACT_TABLE
      + " SET " + ARTIFACT_STORAGE_URL_COLUMN + " = ?"
      + " WHERE " + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String DELETE_ARTIFACT_QUERY = "DELETE FROM " + ARTIFACT_TABLE
      + " WHERE " + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String DELETE_ORPHANED_NAMESPACE_QUERY = "DELETE FROM " + NAMESPACE_TABLE
      + " WHERE NOT EXISTS ( SELECT DISTINCT ON (" + NAMESPACE_SEQ_COLUMN + ") " + NAMESPACE_SEQ_COLUMN + " FROM " + ARTIFACT_TABLE + " )";

  private static final String DELETE_ORPHANED_AUID_QUERY = "DELETE FROM " + AUID_TABLE
      + " WHERE NOT EXISTS ( SELECT DISTINCT ON (" + AUID_SEQ_COLUMN + ") " + AUID_SEQ_COLUMN + " FROM " + ARTIFACT_TABLE + " )";

  private static final String DELETE_ORPHANED_URL_QUERY = "DELETE FROM " + URL_TABLE
      + " WHERE NOT EXISTS ( SELECT DISTINCT ON (" + URL_SEQ_COLUMN + ") " + URL_SEQ_COLUMN + " FROM " + ARTIFACT_TABLE + " )";

  // Query to delete AU sizes of an AU associated with an AUID
  private static final String DELETE_AU_SIZE_QUERY =
      "delete from " + ARCHIVAL_UNIT_SIZE_TABLE
          + " where " + AUID_SEQ_COLUMN + " = ?";

  // Query to add AU sizes of an AU
  private static final String ADD_AU_SIZE_QUERY = "insert into "
      + ARCHIVAL_UNIT_SIZE_TABLE
      + "(" + AUID_SEQ_COLUMN
      + "," + AU_LATEST_VERSIONS_SIZE_COLUMN
      + "," + AU_ALL_VERSIONS_SIZE_COLUMN
      + "," + AU_DISK_SIZE_COLUMN
      + "," + LAST_UPDATE_TIME_COLUMN
      + ") values (?,?,?,?,?)";

  private static final String INSERT_ARTIFACT_QUERY = "INSERT INTO "
      + ARTIFACT_TABLE
      + "(" + ARTIFACT_UUID_COLUMN
      + "," + NAMESPACE_SEQ_COLUMN
      + "," + AUID_SEQ_COLUMN
      + "," + URL_SEQ_COLUMN
      + "," + ARTIFACT_VERSION_COLUMN
      + "," + ARTIFACT_COMMITTED_COLUMN
      + "," + ARTIFACT_STORAGE_URL_COLUMN
      + "," + ARTIFACT_LENGTH_COLUMN
      + "," + ARTIFACT_DIGEST_COLUMN
      + "," + ARTIFACT_CRAWL_TIME_COLUMN
      + " ) VALUES (?,?,?,?,?,?,?,?,?,?)";

  private static final String UPSERT_ARTIFACT_FOR_REINDEX_QUERY = INSERT_ARTIFACT_QUERY
      + " ON CONFLICT (" + ARTIFACT_UUID_COLUMN + ") DO UPDATE SET "
      + ARTIFACT_COMMITTED_COLUMN + " = ? , " + ARTIFACT_STORAGE_URL_COLUMN + " = ?";

  // Idempotent variant of INSERT_ARTIFACT_QUERY used by the bulk addArtifacts()
  // path. Because that path now commits per batch, a retried finishBulkStore
  // (e.g. after a partial failure) can re-present artifacts that already landed;
  // ON CONFLICT refreshes the committed flag and storage URL instead of failing
  // on the UUID unique constraint. Uses EXCLUDED so it binds the same 10
  // parameters as INSERT_ARTIFACT_QUERY (see bindArtifactInsertParams).
  private static final String UPSERT_ARTIFACT_QUERY = INSERT_ARTIFACT_QUERY
      + " ON CONFLICT (" + ARTIFACT_UUID_COLUMN + ") DO UPDATE SET "
      + ARTIFACT_COMMITTED_COLUMN + " = EXCLUDED." + ARTIFACT_COMMITTED_COLUMN
      + ", " + ARTIFACT_STORAGE_URL_COLUMN + " = EXCLUDED." + ARTIFACT_STORAGE_URL_COLUMN;

  private static final String GET_SIZE_OF_ARTIFACTS_QUERY = "SELECT "
      + " SUM(" + ARTIFACT_LENGTH_COLUMN + ") total_size"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE;

  private static final String GET_SIZE_OF_LATEST_ARTIFACTS_QUERY = "SELECT "
      + " SUM(" + ARTIFACT_LENGTH_COLUMN + ") total_size"
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " INNER JOIN (" + MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY + ") m ON"
      + " m." + NAMESPACE_SEQ_COLUMN + " = a." + NAMESPACE_SEQ_COLUMN
      + " AND m." + AUID_SEQ_COLUMN + " = a." + AUID_SEQ_COLUMN
      + " AND m." + URL_SEQ_COLUMN + " = a." + URL_SEQ_COLUMN
      + " AND m.latest_version = a." + ARTIFACT_VERSION_COLUMN
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN;

  // Keyset pagination WHERE clause fragment.
  /// sortUri is a computed column: replace(concat(url, long_url), '/', '\t')
  // Ordering is sortUri ASC, artifact_version DESC
  // This clause selects rows AFTER the given (sortUri, version) position
  // Note: --SortUriExpr-- is a placeholder that must be replaced with the actual expression
  // because PostgreSQL doesn't allow column aliases in WHERE clauses
  private static final String KEYSET_WHERE_CLAUSE =
      " AND (--SortUriExpr-- > ? OR (--SortUriExpr-- = ? AND " + ARTIFACT_VERSION_COLUMN + " < ?))";

  // Extended keyset clause for all-AUIDs queries where ordering is: sortUri ASC, auid ASC, version DESC
  // This provides unique cursor positioning across multiple AUIDs with the same URL and version
  private static final String KEYSET_WHERE_CLAUSE_ALL_AUIDS =
      " AND (--SortUriExpr-- > ?"
      + " OR (--SortUriExpr-- = ? AND " + SORT_AUID_EXPR + " > ?)"
      + " OR (--SortUriExpr-- = ? AND " + SORT_AUID_EXPR + " = ? AND " + ARTIFACT_VERSION_COLUMN + " < ?))";

  // sortUri expression. COLLATE "C" ensures byte-order sorting consistent across
  // all PostgreSQL locales. Formerly split into long-URL and short-URL variants;
  // with the head/tail split gone, urls.url holds the whole URL and one
  // expression serves every query.
  private static final String SORT_URI_EXPR =
      "replace(u." + URL_COLUMN + ", '/', '\u0009') COLLATE \"C\"";

  /**
   * Constructor.
   *
   * @param idxDbManager A SQLArtifactIndexDbManager with the database manager.
   */
  public SQLArtifactIndexManagerSql(SQLArtifactIndexDbManager idxDbManager) {
    this.idxDbManager = idxDbManager;
  }

  /**
   * Sets the page size used by paging artifact iterators.
   * Primarily useful for testing with smaller page sizes.
   *
   * @param pageSize the page size (must be at least 1)
   * @throws IllegalArgumentException if pageSize < 1
   */
  public void setPageSize(int pageSize) {
    if (pageSize < 1) {
      throw new IllegalArgumentException("Page size must be at least 1");
    }
    this.pageSize = pageSize;
  }

  /**
   * Returns the current page size used by paging artifact iterators.
   *
   * @return the page size
   */
  public int getPageSize() {
    return pageSize;
  }

  /**
   * Provides a connection to the database.
   *
   * @return a Connection with the connection to the database.
   * @throws DbException if any problem occurred accessing the database.
   */
  private Connection getConnection() throws DbException {
    return idxDbManager.getConnection();
  }

  protected Long findOrCreateUrlSeq(Connection conn, String url)
      throws DbException {

    log.debug2("url = {}", url);

    // Single lookup: containsKey() followed by get() is not atomic on a
    // synchronizedMap, so a concurrent LRU eviction or cache flush between the
    // two calls would turn a hit into a null return.
    Long cachedUrlSeq = lru_urls_seqs.get(url);
    if (cachedUrlSeq != null) {
      return cachedUrlSeq;
    }

    // Insert-first rather than select-then-insert: the upsert is atomic, so
    // there is no window for two transactions to both create the URL. The
    // former retry-on-duplicate-key loop could never succeed on PostgreSQL --
    // the violation aborts the transaction, so the retry's SELECT failed with
    // "current transaction is aborted" rather than finding the winner's row.
    Long urlSeq = addUrl(conn, url);

    if (urlSeq == null) {
      // A concurrent transaction created it first, so DO NOTHING suppressed the
      // insert and returned no row. Its row is visible to us only once it has
      // committed; under READ COMMITTED our next statement sees it.
      urlSeq = findUrlSeq(conn, url);
      log.trace("lost insert race, found urlSeq = {}", urlSeq);

      if (urlSeq == null) {
        // Not reachable: ON CONFLICT DO NOTHING waits for the conflicting
        // transaction to end, and only suppresses the insert if that
        // transaction committed. Fail loudly rather than returning null, which
        // callers unbox into a long.
        throw new DbException(
            "Insert of url reported a conflict but no row exists: " + url);
      }
    }

    log.debug2("urlSeq = {}", urlSeq);
    return urlSeq;
  }

  protected Long findUrlSeq(Connection conn, String url)
      throws DbException {

    log.debug2("url = {}", url);

    Long urlSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find url";

    String sqlQuery = FIND_URL_SEQ_QUERY;

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      // Populate the query. Both parameters are the same URL: one feeds the
      // indexed md5 term, the other the exact-equality term.
      ps.setString(1, url);
      ps.setString(2, url);

      // Get the URL row
      resultSet = idxDbManager.executeQuery(ps);

      // Check whether a result was obtained.
      if (resultSet.next()) {
        // Yes: Get the URL sequence
        urlSeq = resultSet.getLong(URL_SEQ_COLUMN);
        log.trace("Found urlSeq = {}", urlSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", sqlQuery);
      log.error("url = {}", url);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("urlSeq = {}", urlSeq);
    return urlSeq;
  }

  /**
   * Inserts a URL, or does nothing if a concurrent transaction already
   * inserted it.
   *
   * @param conn A Connection with the database connection to be used
   * @param url  The URL to insert
   * @return The new sequence number, or {@code null} if the URL already
   *         existed and no row was inserted
   * @throws DbException if any problem occurred accessing the database
   */
  private Long addUrl(Connection conn, String url) throws DbException {
    log.debug2("url = {}", url);

    Long urlSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add url";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, UPSERT_URL_QUERY);

      // Populate the query
      ps.setString(1, url);

      // Add the URL. RETURNING makes this a result-producing statement, so it
      // is executed as a query rather than an update.
      resultSet = idxDbManager.executeQuery(ps);

      // No row means ON CONFLICT DO NOTHING suppressed the insert because a
      // concurrent transaction created this URL first. That is a normal
      // outcome, not an error; the caller resolves it with a lookup.
      if (!resultSet.next()) {
        log.debug2("url already existed, no row inserted");
        return null;
      }

      urlSeq = resultSet.getLong(1);
      log.debug2("urlSeq = {}", urlSeq);

      // Count this new urls-table row toward the geometric ANALYZE cadence.
      // Only reached when a row was actually inserted.
      noteUrlRowCreated();

      return urlSeq;
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPSERT_URL_QUERY);
      log.error("url = {}", url);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", UPSERT_URL_QUERY);
      log.error("url = {}", url);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  /** Caches SEQ number for namespaces, AUIDs, and URLs.  Avoids 2-3
   * DB accesses per artifact store.  (Updates are locked per
   * (namespace, AUID), so synchronizing these maps just for
   * individual accesses is sufficient.) */
  private Map<String, Long> lru_namespace_seqs = Collections.synchronizedMap(new HashMap<>());
  private Map<String, Long> lru_auids_seqs = Collections.synchronizedMap(new LRUMap<>(100));
  private Map<String, Long> lru_urls_seqs = Collections.synchronizedMap(new LRUMap<>(1000));

  protected Long findOrCreateNamespaceSeq(Connection conn, String namespace)
      throws DbException {

    log.debug2("namespace = {}", namespace);

    // Single lookup; see findOrCreateUrlSeq() for why containsKey()+get() is
    // unsafe here.
    Long cachedNamespaceSeq = lru_namespace_seqs.get(namespace);
    if (cachedNamespaceSeq != null) {
      return cachedNamespaceSeq;
    }

    // Atomic upsert; see findOrCreateUrlSeq() for why the former
    // retry-on-duplicate-key loop could not work.
    Long namespaceSeq = addNamespace(conn, namespace);

    log.debug2("namespaceSeq = {}", namespaceSeq);
    return namespaceSeq;
  }

  protected Long findNamespaceSeq(Connection conn, String namespace)
      throws DbException {

    log.debug2("namespace = {}", namespace);

    Long namespaceSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find namespace";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, FIND_NAMESPACE_SEQ_QUERY);

      // Populate the query
      ps.setString(1, namespace);

      // Get the namespace row
      resultSet = idxDbManager.executeQuery(ps);

      // Check whether a result was obtained.
      if (resultSet.next()) {
        // Yes: Get the namespace sequence
        namespaceSeq = resultSet.getLong(NAMESPACE_SEQ_COLUMN);
        log.trace("Found namespaceSeq = {}", namespaceSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_NAMESPACE_SEQ_QUERY);
      log.error("namespace = {}", namespace);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("namespaceSeq = {}", namespaceSeq);
    return namespaceSeq;
  }

  private Long addNamespace(Connection conn, String namespace) throws DbException {
    log.debug2("namespace = {}", namespace);

    Long namespaceSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add namespace";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, UPSERT_NAMESPACE_QUERY);

      // Populate the query
      ps.setString(1, namespace);

      // Add the namespace. RETURNING makes this a result-producing statement.
      resultSet = idxDbManager.executeQuery(ps);

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
        // DO UPDATE always produces a row, so an empty result is impossible.
        String message =
            "Unable to create row in namespace table for namespace = " + namespace;
        log.error(message);
        throw new DbException(message);
      }

      // No: Get the namespace database identifier.
      namespaceSeq = resultSet.getLong(1);
      log.trace("Added namespaceSeq = {}", namespaceSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPSERT_NAMESPACE_QUERY);
      log.error("namespace = {}", namespace);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", UPSERT_NAMESPACE_QUERY);
      log.error("namespace = {}", namespace);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("namespaceSeq = {}", namespaceSeq);
    return namespaceSeq;
  }

  public List<String> getNamespaces() throws DbException {
    Connection conn = null;

    try {
      conn = getConnection();
      return getNamespaces(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private List<String> getNamespaces(Connection conn) throws DbException {
    List<String> result = new ArrayList<>();
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get namespaces";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, GET_NAMESPACES_QUERY);

      resultSet = idxDbManager.executeQuery(ps);

      while (resultSet.next()) {
        result.add(resultSet.getString(NAMESPACE_COLUMN));
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_NAMESPACES_QUERY);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("result = {}", result);
    return result;
  }

  public Iterable<String> findAuids(String namespace) throws DbException {
    Connection conn = null;

    try {
      conn = getConnection();
      return findAuids(conn, namespace);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Iterable<String> findAuids(Connection conn, String namespace) throws DbException {
    List<String> result = new ArrayList<>();
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AUIDs in namespace";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, GET_AUIDS_BY_NAMESPACE_QUERY);
      ps.setString(1, namespace);

      resultSet = idxDbManager.executeQuery(ps);

      while (resultSet.next()) {
        result.add(resultSet.getString(AUID_COLUMN));
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AUIDS_BY_NAMESPACE_QUERY);
      log.error("namespace = {}", namespace);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("result = {}", result);
    return result;
  }

  protected Long findOrCreateAuidSeq(Connection conn, String auid)
      throws DbException {

    log.debug2("auid = {}", auid);

    // Single lookup; see findOrCreateUrlSeq() for why containsKey()+get() is
    // unsafe here.
    Long cachedAuidSeq = lru_auids_seqs.get(auid);
    if (cachedAuidSeq != null) {
      return cachedAuidSeq;
    }

    // Atomic upsert; see findOrCreateUrlSeq() for why the former
    // retry-on-duplicate-key loop could not work.
    Long auidSeq = addAuid(conn, auid);

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  protected Long findAuidSeq(Connection conn, String auid)
      throws DbException {

    log.debug2("auid = {}", auid);

    Long auidSeq = null;
    PreparedStatement findAuid = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find AUID";

    try {
      // Prepare the query
      findAuid = idxDbManager.prepareStatement(conn, FIND_AUID_SEQ_QUERY);

      // Populate the query
      findAuid.setString(1, auid);

      // Get the AUID row
      resultSet = idxDbManager.executeQuery(findAuid);

      // Check whether a result was obtained.
      if (resultSet.next()) {
        // Yes: Get the AUID sequence
        auidSeq = resultSet.getLong(AUID_SEQ_COLUMN);
        log.trace("Found auidSeq = {}", auidSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_AUID_SEQ_QUERY);
      log.error("auid = {}", auid);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAuid);
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  private Long addAuid(Connection conn, String auid) throws DbException {
    log.debug2("auid = {}", auid);

    Long auidSeq = null;
    PreparedStatement insertAuid = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add AUID";

    try {
      // Prepare the query
      insertAuid = idxDbManager.prepareStatement(conn, UPSERT_AUID_QUERY);

      // Populate the query
      insertAuid.setString(1, auid);

      // Add the AUID. RETURNING makes this a result-producing statement.
      resultSet = idxDbManager.executeQuery(insertAuid);

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
        // DO UPDATE always produces a row, so an empty result is impossible.
        String message =
            "Unable to create row in AUID table for auid = " + auid;
        log.error(message);
        throw new DbException(message);
      }

      // No: Get the AUID database identifier.
      auidSeq = resultSet.getLong(1);
      log.trace("Added auidSeq = {}", auidSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPSERT_AUID_QUERY);
      log.error("auid = {}", auid);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", UPSERT_AUID_QUERY);
      log.error("auid = {}", auid);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertAuid);
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  public Artifact getArtifact(String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    Connection conn = null;

    try {
      conn = getConnection();
      return getArtifact(conn, uuid);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Artifact getArtifact(Connection conn, String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifact";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, GET_ARTIFACT_BY_UUID_QUERY);

      // Populate the query
      ps.setString(1, uuid);

      resultSet = idxDbManager.executeQuery(ps);

      Artifact result = getArtifactFromResultSet(resultSet);
      log.debug2("result = {}", result);
      return result;
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_ARTIFACT_BY_UUID_QUERY);
      log.error("artifactId = {}", uuid);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  public Artifact getArtifact(String namespace, String auid, String url, int version, boolean includeUncommitted) throws DbException {
    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("url = {}", url);
    log.debug2("version = {}", version);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    Connection conn = null;

    try {
      conn = getConnection();
      return getArtifact(conn, namespace, auid, url, version, includeUncommitted);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Artifact getArtifact(Connection conn, String namespace, String auid, String url, int version, boolean includeUncommitted)
      throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifact";

    if (StringUtil.isNullString(url)) {
      url = EMPTY_STRING;
    }

    String sqlQuery = GET_ARTIFACT_WITH_VERSION_QUERY;

    try {
      if (!includeUncommitted) {
        sqlQuery += ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE;
      }

      sqlQuery += " LIMIT 1";

      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      // Populate the query
      ps.setString(1, namespace);
      ps.setString(2, auid);
      ps.setString(3, url);
      ps.setInt(4, version);

      resultSet = idxDbManager.executeQuery(ps);

      Artifact result = getArtifactFromResultSet(resultSet);
      log.debug2("result = {}", result);
      return result;
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", sqlQuery);
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("url = {}", url);
      log.error("version = {}", version);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  // =========================================================================
  // Page fetch methods for PagingArtifactIterator
  // These methods fetch a single page and close the connection immediately.
  // =========================================================================

  /**
   * Fetches a page of latest artifacts (one per URL) using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * @param namespace The namespace
   * @param auid The AUID
   * @param includeUncommitted Whether to include uncommitted artifacts
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchLatestArtifactsPage(
      String namespace, String auid, boolean includeUncommitted,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, auid={}, includeUncommitted={}, cursor={}, limit={}",
        namespace, auid, includeUncommitted, cursor, limit);

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();

    try {
      conn = getConnection();

      // Build base query
      String sqlQuery = GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_AUID_QUERY;
      String latestVersionsQuery = MAX_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY;

      // Add committed status condition if needed
      latestVersionsQuery = latestVersionsQuery.replace("--CommittedStatusCondition--",
          !includeUncommitted ? ARTIFACT_COMMITTED_STATUS_CONDITION : EMPTY_STRING);

      sqlQuery = sqlQuery.replace("--MaxVersionAllUrlsWithNamespaceAndAuid--", latestVersionsQuery);

      // Add keyset WHERE clause if not first page
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE.replace("--SortUriExpr--", SORT_URI_EXPR) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      // Prepare statement
      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      // Bind parameters
      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);
      ps.setString(paramIndex++, auid);
      if (!includeUncommitted) {
        ps.setBoolean(paramIndex++, true);
      }

      // Bind keyset parameters if not first page
      if (hasCursor) {
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      // Execute query
      rs = idxDbManager.executeQuery(ps);

      // Collect results
      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, auid={}", namespace, auid);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Fetches a page of all versions of all URLs using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * @param namespace The namespace
   * @param auid The AUID
   * @param includeUncommitted Whether to include uncommitted artifacts
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchAllVersionsArtifactsPage(
      String namespace, String auid, boolean includeUncommitted,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, auid={}, includeUncommitted={}, cursor={}, limit={}",
        namespace, auid, includeUncommitted, cursor, limit);

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();

    try {
      conn = getConnection();

      String sqlQuery = GET_ARTIFACTS_WITH_NAMESPACE_AND_AUID_QUERY;

      sqlQuery = sqlQuery.replace("--CommittedStatusCondition--",
          !includeUncommitted ? ARTIFACT_COMMITTED_STATUS_CONDITION_TRUE : EMPTY_STRING);

      // Add keyset WHERE clause if not first page
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE.replace("--SortUriExpr--", SORT_URI_EXPR) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);
      ps.setString(paramIndex++, auid);

      if (hasCursor) {
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      rs = idxDbManager.executeQuery(ps);

      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, auid={}", namespace, auid);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Fetches a page of all committed versions of a specific URL using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * @param namespace The namespace
   * @param auid The AUID
   * @param url The URL
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchArtifactsForUrlPage(
      String namespace, String auid, String url,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, auid={}, url={}, cursor={}, limit={}",
        namespace, auid, url, cursor, limit);

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();

    String sqlQuery = GET_COMMITTED_ARTIFACTS_WITH_NAMESPACE_AUID_URL_QUERY;

    try {
      conn = getConnection();

      // Add keyset WHERE clause if not first page
      String sortUriExpr = SORT_URI_EXPR;
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE.replace("--SortUriExpr--", sortUriExpr) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);
      ps.setString(paramIndex++, auid);

        ps.setString(paramIndex++, url);

      if (hasCursor) {
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      rs = idxDbManager.executeQuery(ps);

      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, auid={}, url={}", namespace, auid, url);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Fetches a page of artifacts for a URL across all AUIDs in a namespace using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * <p>Uses the extended keyset clause (sortUri, auid, version) because the same URL
   * can exist in multiple AUIDs with the same version number.</p>
   *
   * @param namespace The namespace
   * @param url The URL
   * @param versions Whether to return all versions or only latest
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchArtifactsForUrlAllAuidsPage(
      String namespace, String url, VersionsEnum versions,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, url={}, versions={}, cursor={}, limit={}",
        namespace, url, versions, cursor, limit);

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();
    String sqlQuery;

      sqlQuery = versions == VersionsEnum.LATEST ?
          GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_URL_QUERY :
          GET_ARTIFACTS_WITH_NAMESPACE_AND_URL_QUERY;

    try {
      conn = getConnection();

      // Add keyset WHERE clause if not first page
      // Use extended clause with auid because same URL can exist across multiple AUIDs
      String sortUriExpr = SORT_URI_EXPR;
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE_ALL_AUIDS.replace("--SortUriExpr--", sortUriExpr) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);

        ps.setString(paramIndex++, url);

      if (hasCursor) {
        // Extended keyset: (sortUri > ?) OR (sortUri = ? AND auid > ?) OR (sortUri = ? AND auid = ? AND version < ?)
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getAuid());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getAuid());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      rs = idxDbManager.executeQuery(ps);

      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, url={}, versions={}", namespace, url, versions);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Builds the SQL fragment that matches {@code column} against a literal URL
   * prefix, for substitution in place of {@link #URL_PREFIX_CONDITION_TOKEN}.
   *
   * <p>A range predicate is used rather than {@code LIKE prefix || '%'} because
   * {@code %} and {@code _} are {@code LIKE} metacharacters, as is {@code \} in
   * PostgreSQL (which supplies a default escape character when the predicate has
   * no {@code ESCAPE} clause). A caller-supplied URL prefix must be matched
   * literally, i.e. equivalently to {@code String.startsWith}.
   *
   * <p>{@code COLLATE "C"} is required, not decorative: the range is equivalent
   * to a literal prefix match only under byte-order (equivalently, code point
   * order) comparison. The index database is created from {@code template0} with
   * no explicit {@code LC_COLLATE}, so it inherits a cluster default that is
   * generally a locale collation, under which comparison is not
   * character-by-character.
   *
   * <p>The range is expressed over {@code left(column, URL_PREFIX_INDEX_LENGTH)}
   * rather than over the column itself, because that is the expression
   * {@code idx1_urls} is built on: {@code urls.url} is unbounded and a btree
   * index row cannot exceed 2704 bytes, so the index cannot cover the whole
   * column. For a prefix no longer than that bound the truncated range is
   * <em>exact</em> - {@code left(U,N)} starts with P if and only if {@code U}
   * does - so no further comparison is needed. Only a longer prefix, where the
   * truncated range degrades to equality, needs the exact recheck against the
   * column.
   *
   * @param column The qualified column holding the URL
   * @param bounded Whether an exclusive upper bound is also to be bound; false
   *                when {@link #prefixUpperBound} found none, in which case the
   *                lower bound alone is the correct (trivially true) predicate
   * @param recheck Whether the prefix is longer than {@link
   *                SqlConstants#URL_PREFIX_INDEX_LENGTH}, so that the truncated
   *                range alone does not decide the match
   * @return The SQL fragment; binds the truncated bounds first, then the exact
   *         bounds when {@code recheck}
   */
  private static String urlPrefixCondition(String column, boolean idxBounded,
                                           boolean recheck,
                                           boolean exactBounded) {
    String truncated =
        "left(" + column + ", " + URL_PREFIX_INDEX_LENGTH + ") COLLATE \"C\"";

    StringBuilder sb = new StringBuilder();

    sb.append(" AND ").append(truncated).append(" >= ? ");
    if (idxBounded) {
      sb.append(" AND ").append(truncated).append(" < ? ");
    }

    if (recheck) {
      String collated = column + " COLLATE \"C\"";
      sb.append(" AND ").append(collated).append(" >= ? ");
      if (exactBounded) {
        sb.append(" AND ").append(collated).append(" < ? ");
      }
    }

    return sb.toString();
  }

  /**
   * The first {@code n} code points of {@code s}, matching what PostgreSQL's
   * {@code left(s, n)} returns.
   *
   * <p>Not {@code substring(0, n)}: that counts UTF-16 code units, so on a
   * string containing any non-BMP character it would both cut at the wrong
   * place and, in the worst case, split a surrogate pair. {@code left()} counts
   * characters, and the two must agree exactly or the index-driving predicate
   * stops matching the index expression.
   *
   * @param s The string to truncate; must not be null
   * @param n The number of code points to keep
   * @return {@code s} itself when it is no longer than {@code n} code points,
   *         otherwise its first {@code n} code points
   */
  static String leftCodePoints(String s, int n) {
    if (s.codePointCount(0, s.length()) <= n) {
      return s;
    }

    return s.substring(0, s.offsetByCodePoints(0, n));
  }

  /**
   * Returns the exclusive upper bound for a literal-prefix range match under C
   * (byte-order) collation: the prefix with its last code point incremented,
   * carrying left past any U+10FFFF. Returns null when no upper bound exists
   * (empty prefix, or a prefix consisting entirely of U+10FFFF), meaning the
   * range is open-ended.
   *
   * <p>The increment is by code point rather than by {@code char} because UTF-8
   * byte order is code point order, and code point order is what
   * {@code COLLATE "C"} compares.
   *
   * @param prefix The literal prefix; must not be null
   * @return The exclusive upper bound, or null if the range is open-ended
   * @throws IllegalArgumentException if {@code prefix} is null
   */
  static String prefixUpperBound(String prefix) {
    if (prefix == null) {
      throw new IllegalArgumentException("prefix must not be null");
    }

    int i = prefix.length();

    while (i > 0) {
      int cp = prefix.codePointBefore(i);
      int start = i - Character.charCount(cp);
      int next = cp + 1;

      // Code points in the surrogate range are not Unicode scalar values.
      if (next == Character.MIN_SURROGATE) {
        next = Character.MAX_SURROGATE + 1;
      }

      if (next <= Character.MAX_CODE_POINT) {
        return prefix.substring(0, start) + new String(Character.toChars(next));
      }

      // cp was U+10FFFF: drop it and carry left.
      i = start;
    }

    return null;
  }

  /**
   * Fetches a page of artifacts by URL prefix across all AUIDs in a namespace using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * <p>Uses the extended keyset clause (sortUri, auid, version) because the same URL
   * can exist in multiple AUIDs with the same version number.</p>
   *
   * @param namespace The namespace
   * @param prefix The URL prefix
   * @param versions Whether to return all versions or only latest
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchArtifactsByPrefixAllAuidsPage(
      String namespace, String prefix, VersionsEnum versions,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, prefix={}, versions={}, cursor={}, limit={}",
        namespace, prefix, versions, cursor, limit);

    if (StringUtil.isNullString(prefix)) {
      prefix = EMPTY_STRING;
    }

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();
    String sqlQuery;

      sqlQuery = versions == VersionsEnum.LATEST ?
          GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_URL_PREFIX_QUERY :
          GET_ARTIFACTS_WITH_NAMESPACE_AND_URL_PREFIX_QUERY;

    try {
      conn = getConnection();

      // Add keyset WHERE clause if not first page
      // Use extended clause with auid because same URL can exist across multiple AUIDs
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE_ALL_AUIDS.replace("--SortUriExpr--", SORT_URI_EXPR) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add the literal URL prefix condition. In the long URL branch the prefix
      // head is matched by an '=' predicate and only the tail is ranged over.
      String prefixColumn = "u." + URL_COLUMN;
      // idx1_urls is built on left(url, N), so the range that can drive it is
      // over the truncated prefix. That range is exact whenever the prefix itself
      // fits within N; only a longer prefix needs the exact bounds as a recheck.
      // Both upper bounds are computed independently: a truncated prefix can be
      // open-ended where the full one is not. See urlPrefixCondition().
      String idxLowerBound = leftCodePoints(prefix, URL_PREFIX_INDEX_LENGTH);
      String idxUpperBound = prefixUpperBound(idxLowerBound);
      boolean recheck = !idxLowerBound.equals(prefix);
      String lowerBound = prefix;
      String upperBound = prefixUpperBound(lowerBound);

      sqlQuery = sqlQuery.replace(URL_PREFIX_CONDITION_TOKEN,
          urlPrefixCondition(prefixColumn, idxUpperBound != null, recheck,
              upperBound != null));

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);

      ps.setString(paramIndex++, idxLowerBound);

      if (idxUpperBound != null) {
        ps.setString(paramIndex++, idxUpperBound);
      }

      if (recheck) {
        ps.setString(paramIndex++, lowerBound);

        if (upperBound != null) {
          ps.setString(paramIndex++, upperBound);
        }
      }

      if (hasCursor) {
        // Extended keyset: (sortUri > ?) OR (sortUri = ? AND auid > ?) OR (sortUri = ? AND auid = ? AND version < ?)
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getAuid());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getAuid());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      rs = idxDbManager.executeQuery(ps);

      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, prefix={}, versions={}", namespace, prefix, versions);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Fetches a page of latest versions of URLs matching a prefix using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * @param namespace The namespace
   * @param auid The AUID
   * @param urlPrefix The URL prefix
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchLatestArtifactsWithPrefixPage(
      String namespace, String auid, String urlPrefix,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, auid={}, urlPrefix={}, cursor={}, limit={}",
        namespace, auid, urlPrefix, cursor, limit);

    if (StringUtil.isNullString(urlPrefix)) {
      urlPrefix = EMPTY_STRING;
    }

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();

    String sqlQuery = GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AUID_URL_PREFIX_QUERY;

    try {
      conn = getConnection();

      // Add keyset WHERE clause if not first page
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE.replace("--SortUriExpr--", SORT_URI_EXPR) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add the literal URL prefix condition. In the long URL branch the prefix
      // head is matched by an '=' predicate and only the tail is ranged over.
      String prefixColumn = "u." + URL_COLUMN;
      // idx1_urls is built on left(url, N), so the range that can drive it is
      // over the truncated prefix. That range is exact whenever the prefix itself
      // fits within N; only a longer prefix needs the exact bounds as a recheck.
      // Both upper bounds are computed independently: a truncated prefix can be
      // open-ended where the full one is not. See urlPrefixCondition().
      String idxLowerBound = leftCodePoints(urlPrefix, URL_PREFIX_INDEX_LENGTH);
      String idxUpperBound = prefixUpperBound(idxLowerBound);
      boolean recheck = !idxLowerBound.equals(urlPrefix);
      String lowerBound = urlPrefix;
      String upperBound = prefixUpperBound(lowerBound);

      sqlQuery = sqlQuery.replace(URL_PREFIX_CONDITION_TOKEN,
          urlPrefixCondition(prefixColumn, idxUpperBound != null, recheck,
              upperBound != null));

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);
      ps.setString(paramIndex++, auid);

      ps.setString(paramIndex++, idxLowerBound);

      if (idxUpperBound != null) {
        ps.setString(paramIndex++, idxUpperBound);
      }

      if (recheck) {
        ps.setString(paramIndex++, lowerBound);

        if (upperBound != null) {
          ps.setString(paramIndex++, upperBound);
        }
      }

      if (hasCursor) {
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      rs = idxDbManager.executeQuery(ps);

      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, auid={}, urlPrefix={}", namespace, auid, urlPrefix);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Fetches a page of all versions of URLs matching a prefix using keyset pagination.
   * Connection is opened, used, and closed within this method.
   *
   * @param namespace The namespace
   * @param auid The AUID
   * @param urlPrefix The URL prefix
   * @param cursor The cursor from the last artifact of the previous page (null for first page)
   * @param limit Maximum number of artifacts to return
   * @return List of artifacts
   * @throws DbException if database error occurs
   */
  List<Artifact> fetchAllVersionsArtifactsWithPrefixPage(
      String namespace, String auid, String urlPrefix,
      PagingCursor cursor, int limit) throws DbException {

    log.debug2("namespace={}, auid={}, urlPrefix={}, cursor={}, limit={}",
        namespace, auid, urlPrefix, cursor, limit);

    if (StringUtil.isNullString(urlPrefix)) {
      urlPrefix = EMPTY_STRING;
    }

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    boolean hasCursor = !cursor.isInitial();

    String sqlQuery = GET_ARTIFACTS_WITH_NAMESPACE_AUID_URL_PREFIX_QUERY;

    try {
      conn = getConnection();

      // Add keyset WHERE clause if not first page
      String keysetClause = hasCursor ?
          KEYSET_WHERE_CLAUSE.replace("--SortUriExpr--", SORT_URI_EXPR) : EMPTY_STRING;
      sqlQuery = sqlQuery.replace("--KeysetCondition--", keysetClause);

      // Add the literal URL prefix condition. In the long URL branch the prefix
      // head is matched by an '=' predicate and only the tail is ranged over.
      String prefixColumn = "u." + URL_COLUMN;
      // idx1_urls is built on left(url, N), so the range that can drive it is
      // over the truncated prefix. That range is exact whenever the prefix itself
      // fits within N; only a longer prefix needs the exact bounds as a recheck.
      // Both upper bounds are computed independently: a truncated prefix can be
      // open-ended where the full one is not. See urlPrefixCondition().
      String idxLowerBound = leftCodePoints(urlPrefix, URL_PREFIX_INDEX_LENGTH);
      String idxUpperBound = prefixUpperBound(idxLowerBound);
      boolean recheck = !idxLowerBound.equals(urlPrefix);
      String lowerBound = urlPrefix;
      String upperBound = prefixUpperBound(lowerBound);

      sqlQuery = sqlQuery.replace(URL_PREFIX_CONDITION_TOKEN,
          urlPrefixCondition(prefixColumn, idxUpperBound != null, recheck,
              upperBound != null));

      // Add LIMIT clause
      sqlQuery += " LIMIT ?";

      log.trace("SQL = '{}'", sqlQuery);

      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      int paramIndex = 1;
      ps.setString(paramIndex++, namespace);
      ps.setString(paramIndex++, auid);

      ps.setString(paramIndex++, idxLowerBound);

      if (idxUpperBound != null) {
        ps.setString(paramIndex++, idxUpperBound);
      }

      if (recheck) {
        ps.setString(paramIndex++, lowerBound);

        if (upperBound != null) {
          ps.setString(paramIndex++, upperBound);
        }
      }

      ps.setBoolean(paramIndex++, true);

      if (hasCursor) {
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setString(paramIndex++, cursor.getSortUri());
        ps.setInt(paramIndex++, cursor.getVersion());
      }

      ps.setInt(paramIndex++, limit);

      rs = idxDbManager.executeQuery(ps);

      List<Artifact> artifacts = new ArrayList<>(limit);
      while (rs.next()) {
        artifacts.add(getArtifactFromCurrentRow(rs));
      }

      log.debug2("Returning {} artifacts", artifacts.size());
      return artifacts;

    } catch (SQLException e) {
      log.error("Cannot fetch artifact page", e);
      log.error("namespace={}, auid={}, urlPrefix={}", namespace, auid, urlPrefix);
      throw new DbException("Cannot fetch artifact page", e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
    }
  }

  // =========================================================================
  // End of page fetch methods
  // =========================================================================

  public Iterable<Artifact> findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(
      String namespace, String auid, boolean includeUncommitted) throws DbException {
    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    // Create a page fetcher that captures the query parameters
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchLatestArtifactsPage(namespace, auid, includeUncommitted, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize));
  }

  public Iterable<Artifact> findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(String namespace, String auid, boolean includeUncommitted) throws DbException {
    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    // Create a page fetcher that captures the query parameters
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchAllVersionsArtifactsPage(namespace, auid, includeUncommitted, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize));
  }

  public Iterable<Artifact> findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(String namespace, String auid, String url)
      throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("url = {}", url);

    // Create a page fetcher that captures the query parameters
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchArtifactsForUrlPage(namespace, auid, url, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize));
  }

  public Iterable<Artifact> findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(
      String namespace, String url, VersionsEnum versions) throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("url = {}", url);
    log.debug2("versions = {}", versions);

    // Create a page fetcher that captures the query parameters
    // Use ALL_AUIDS cursor extractor because same URL can exist across multiple AUIDs
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchArtifactsForUrlAllAuidsPage(namespace, url, versions, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize,
        PagingArtifactIterator.ALL_AUIDS_CURSOR_EXTRACTOR));
  }

  public Iterable<Artifact> findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
      String namespace, String prefix, VersionsEnum versions) throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("prefix = {}", prefix);
    log.debug2("versions = {}", versions);

    // Create a page fetcher that captures the query parameters
    // Use ALL_AUIDS cursor extractor because same URL can exist across multiple AUIDs
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchArtifactsByPrefixAllAuidsPage(namespace, prefix, versions, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize,
        PagingArtifactIterator.ALL_AUIDS_CURSOR_EXTRACTOR));
  }

  public Iterable<Artifact> findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
      String namespace, String auid, String urlPrefix) throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("urlPrefix = {}", urlPrefix);

    // Create a page fetcher that captures the query parameters
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchLatestArtifactsWithPrefixPage(namespace, auid, urlPrefix, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize));
  }

  public Iterable<Artifact> findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
      String namespace, String auid, String urlPrefix) throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("urlPrefix = {}", urlPrefix);

    // Create a page fetcher that captures the query parameters
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) ->
        fetchAllVersionsArtifactsWithPrefixPage(namespace, auid, urlPrefix, cursor, limit);

    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize));
  }

  private static Artifact getArtifactFromResultSet(ResultSet resultSet) throws SQLException {
    // Get the single result, if any
    if (resultSet.next()) {
      return getArtifactFromCurrentRow(resultSet);
    }

    return null;
  }

  private static Artifact getArtifactFromCurrentRow(ResultSet resultSet) throws SQLException {
    Artifact result = new Artifact();

    result.setUuid(resultSet.getString(ARTIFACT_UUID_COLUMN));
    result.setNamespace(resultSet.getString(NAMESPACE_COLUMN));
    result.setAuid(resultSet.getString(AUID_COLUMN));
    result.setUri(resultSet.getString(URL_COLUMN));
    result.setVersion(resultSet.getInt(ARTIFACT_VERSION_COLUMN));
    result.setCommitted(resultSet.getBoolean(ARTIFACT_COMMITTED_COLUMN));
    result.setStorageUrl(resultSet.getString(ARTIFACT_STORAGE_URL_COLUMN));
    result.setContentLength(resultSet.getLong(ARTIFACT_LENGTH_COLUMN));
    result.setContentDigest(resultSet.getString(ARTIFACT_DIGEST_COLUMN));
    result.setCollectionDate(resultSet.getLong(ARTIFACT_CRAWL_TIME_COLUMN));

    return result;
  }

  public Artifact getLatestArtifact(String namespace, String auid, String url, boolean includeUncommitted)
      throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("url = {}", url);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    Connection conn = null;

    try {
      conn = getConnection();
      return getLatestArtifact(conn, namespace, auid, url, includeUncommitted);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Artifact getLatestArtifact(Connection conn,
                                     String namespace, String auid, String url, boolean includeUncommitted)
      throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifact";
    String sqlQuery = null;
    String latestVersionQuery;

      sqlQuery = GET_LATEST_ARTIFACT_QUERY;
      latestVersionQuery = GET_LATEST_ARTIFACT_VERSION_QUERY;

    try {
      if (!includeUncommitted) {
        latestVersionQuery += ARTIFACT_COMMITTED_STATUS_CONDITION;
      }

      sqlQuery += " AND a." + ARTIFACT_VERSION_COLUMN + " = (" + latestVersionQuery + ")";
      sqlQuery += " LIMIT 1";

      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      // Populate the query
      int idx = 1;
      ps.setString(idx++, namespace);
      ps.setString(idx++, auid);

        ps.setString(idx++, url);

      ps.setString(idx++, namespace);
      ps.setString(idx++, auid);

        ps.setString(idx++, url);

      if (!includeUncommitted) {
        ps.setBoolean(idx, true);
      }

      resultSet = idxDbManager.executeQuery(ps);

      Artifact result = getArtifactFromResultSet(resultSet);
      log.debug2("result = {}", result);
      return result;
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", sqlQuery);
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("url = {}", url);
      log.error("includeUncommitted = {}", includeUncommitted);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  public void commitArtifact(String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    Connection conn = null;

    try {
      conn = getConnection();
      commitArtifact(conn, uuid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private void commitArtifact(Connection conn, String uuid) throws DbException {
    PreparedStatement ps = null;
    String errorMessage = "Cannot update committed status for artifact";

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, UPDATE_ARTIFACT_COMMITTED_QUERY);

      // Populate the query
      ps.setBoolean(1, true);
      ps.setString(2, uuid);

      idxDbManager.executeUpdate(ps);
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", UPDATE_ARTIFACT_COMMITTED_QUERY);
      log.error("uuid = {}", uuid);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public AuSize findAuSize(String auid) throws DbException {
    log.debug2("auid = {}", auid);

    Connection conn = null;

    try {
      // Get a connection to the database
      conn = getConnection();

      return findAuSize(conn, auid);
    } catch (DbException dbe) {
      String message = "Cannot find AU size";
      log.error(message, dbe);
      log.error("auid = {}", auid);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private AuSize findAuSize(Connection conn, String auid) throws DbException {
    log.debug2("auid = {}", auid);

    AuSize result = null;
    PreparedStatement getAuSize = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU size";

    try {
      // Prepare the query
      getAuSize = idxDbManager.prepareStatement(conn, GET_AU_SIZE_QUERY);

      // Populate the query
      getAuSize.setString(1, auid);

      // Get the AU size of the AU associated with this AUID
      resultSet = idxDbManager.executeQuery(getAuSize);

      // Get the single result, if any
      if (resultSet.next()) {
        result = new AuSize();

        // Populate the AuSize
        result.setTotalLatestVersions(
            resultSet.getLong(AU_LATEST_VERSIONS_SIZE_COLUMN));

        result.setTotalAllVersions(
            resultSet.getLong(AU_ALL_VERSIONS_SIZE_COLUMN));

        result.setTotalWarcSize(resultSet.getLong(AU_DISK_SIZE_COLUMN));
      }
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_AU_SIZE_QUERY);
      log.error("auid = {}", auid);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuSize);
    }

    log.debug2("result = {}", result);
    return result;
  }

  public Long updateAuSize(String auid, AuSize auSize)
      throws DbException {

    log.debug2("auid = {}", auid);
    log.debug2("auSize = {}", auSize);

    Long result = null;
    Connection conn = null;

    try {
      // Get a connection to the database
      conn = getConnection();

      // Update the AU size
      result = updateAuSize(conn, auid, auSize);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  public void addArtifact(Artifact artifact) throws DbException {
    log.debug2("artifact = {}", artifact);

    Connection conn = null;

    try {
      conn = getConnection();

      long namespaceSeq = findOrCreateNamespaceSeq(conn, artifact.getNamespace());
      long auidSeq = findOrCreateAuidSeq(conn, artifact.getAuid());
      long urlSeq = findOrCreateUrlSeq(conn, artifact.getUri());

      addArtifact(conn, auidSeq, namespaceSeq, urlSeq, artifact);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);

      // Update LRU caches - note that these must happen *after* the transaction is
      // committed;  otherwise they may not reflect the actual database state
      lru_namespace_seqs.putIfAbsent(artifact.getNamespace(), namespaceSeq);
      lru_auids_seqs.putIfAbsent(artifact.getAuid(), auidSeq);
      lru_urls_seqs.putIfAbsent(artifact.getUri(), urlSeq);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  // How many artifact INSERTs to accumulate before flushing via executeBatch()
  private static final int ARTIFACT_INSERT_BATCH_SIZE = 1000;

  // Geometric ANALYZE cadence for the urls table (see maybeAnalyzeUrls()). The
  // seq-scan -> index-scan plan flip for FIND_URL_SEQ_QUERY happens once, very
  // early; refreshing stats past that point does not change the equality-lookup
  // plan. So we ANALYZE densely while the table is small and stop once it is
  // large, letting autovacuum own steady state. A fixed row interval would be
  // both too sparse early and pointlessly frequent at scale (100M / interval).
  //
  // URL_ANALYZE_MIN_ROWS: floor so the first refresh happens on a cold table.
  // URL_ANALYZE_CAP_ROWS: above this the plan is settled; hand off to autovacuum.
  // Package-visible so shouldAnalyzeUrls() can be unit-tested against them.
  static final long URL_ANALYZE_MIN_ROWS = 1000;
  static final long URL_ANALYZE_CAP_ROWS = 1_000_000;

  // Running estimate of the number of rows in the urls table. Seeded lazily from
  // the database (the index is not necessarily empty at startup) and incremented
  // as new url rows are created; drives the geometric ANALYZE cadence. -1 means
  // "not yet seeded".
  private final AtomicLong urlTableRowEstimate = new AtomicLong(-1);
  // The estimate as of the last urls ANALYZE. Guarded by urlAnalyzeLock.
  private long urlRowsAtLastAnalyze;
  private final Object urlAnalyzeLock = new Object();

  // Advisory-lock namespace used to serialize out-of-band ANALYZE across
  // concurrent bulk loads so they don't collide on a table's catalog rows
  // (which produces PostgreSQL "tuple concurrently updated" errors). Combined
  // with a per-table key in pg_try_advisory_xact_lock(int, int).
  private static final int ANALYZE_ADVISORY_LOCK_NAMESPACE = 0x4C4B5341; // "LKSA"

  /** Add all the Artifacts to the DB, return a Set containing all
   * unique (Namespace, AUID) pairs */
  public Set<Pair<String,String>> addArtifacts(Iterable<Artifact> artifacts)
      throws DbException {
    Connection conn = null;
    PreparedStatement ps = null;
    Set<Pair<String,String>> nsAuids = new HashSet<>();
    int count = 0;

    // Sequence numbers created for the rows accumulated in the current (not yet
    // committed) batch. They are published to the shared LRU caches only after
    // the batch commits, since the sequences do not durably exist until then.
    Map<String, Long> pending_ns_seqs = new HashMap<>();
    Map<String, Long> pending_auid_seqs = new HashMap<>();
    Map<String, Long> pending_url_seqs = new HashMap<>();

    try {
      conn = getConnection();
      // Idempotent insert so a retried finishBulkStore that re-presents artifacts
      // already committed by an earlier partial run updates rather than fails.
      ps = idxDbManager.prepareStatement(conn, UPSERT_ARTIFACT_QUERY);

      for (Artifact artifact : artifacts) {
        nsAuids.add(Pair.of(artifact.getNamespace(), artifact.getAuid()));

        Long namespaceSeq = pending_ns_seqs.get(artifact.getNamespace());
        if (namespaceSeq == null) {
          namespaceSeq = findOrCreateNamespaceSeq(conn, artifact.getNamespace());
        }

        Long auidSeq = pending_auid_seqs.get(artifact.getAuid());
        if (auidSeq == null) {
          auidSeq = findOrCreateAuidSeq(conn, artifact.getAuid());
        }

        Long urlSeq = pending_url_seqs.get(artifact.getUri());
        if (urlSeq == null) {
          urlSeq = findOrCreateUrlSeq(conn, artifact.getUri());
        }

        try {
          bindArtifactInsertParams(ps, namespaceSeq, auidSeq, urlSeq, artifact);
          ps.addBatch();
          count++;
        } catch (SQLException e) {
          throw new DbException("Error binding artifact INSERT parameters", e);
        }

        pending_ns_seqs.put(artifact.getNamespace(), namespaceSeq);
        pending_auid_seqs.put(artifact.getAuid(), auidSeq);
        pending_url_seqs.put(artifact.getUri(), urlSeq);

        if (count % ARTIFACT_INSERT_BATCH_SIZE == 0) {
          // Flush and commit this batch so its rows are durable and visible to
          // the out-of-band ANALYZE below. Committing per batch also bounds the
          // blast radius of a mid-load failure to a single batch instead of the
          // whole AU.
          flushArtifactBatch(ps);
          DbManager.commitOrRollback(conn, log);
          publishSeqLrus(pending_ns_seqs, pending_auid_seqs, pending_url_seqs);
          pending_ns_seqs.clear();
          pending_auid_seqs.clear();
          pending_url_seqs.clear();

          // Refresh URL-table planner stats on a geometric, whole-table cadence
          // so findOrCreateUrlSeq keeps using the index rather than degrading to
          // a seq scan while the table is small. Rows are committed above, so the
          // out-of-band ANALYZE sees them.
          maybeAnalyzeUrls();
        }
      }

      // Flush and commit any partial trailing batch.
      if (count % ARTIFACT_INSERT_BATCH_SIZE != 0) {
        flushArtifactBatch(ps);
        DbManager.commitOrRollback(conn, log);
        publishSeqLrus(pending_ns_seqs, pending_auid_seqs, pending_url_seqs);
      }
    } finally {
      DbManager.safeCloseStatement(ps);
      DbManager.safeRollbackAndClose(conn);
    }

    // Refresh artifact-table stats once, after all rows are committed, so
    // post-load reads don't have to wait for autovacuum to catch up. Done
    // out-of-band for the same safety reason as the URL analyze.
    if (count > 0) {
      analyzeTableOutOfBand(ARTIFACT_TABLE);
    }

    return nsAuids;
  }

  /** Publish committed sequence numbers to the shared LRU caches. */
  private void publishSeqLrus(Map<String, Long> nsSeqs,
                              Map<String, Long> auidSeqs,
                              Map<String, Long> urlSeqs) {
    lru_namespace_seqs.putAll(nsSeqs);
    lru_auids_seqs.putAll(auidSeqs);
    lru_urls_seqs.putAll(urlSeqs);
  }

  private void flushArtifactBatch(PreparedStatement ps) throws DbException {
    try {
      ps.executeBatch();
      ps.clearBatch();
    } catch (SQLException e) {
      throw new DbException("Error executing artifact INSERT batch", e);
    }
  }

  // Refresh planner statistics on the given table on a dedicated connection,
  // OUTSIDE any data-loading transaction. This matters for correctness as much
  // as performance: ANALYZE run inside the load transaction can fail (notably
  // "tuple concurrently updated" when concurrent bulk loads touch the same
  // table's catalog rows), and because that error aborts the whole transaction,
  // a subsequent commit is silently turned into a rollback by PostgreSQL --
  // discarding every insert in the batch with no exception thrown. Running it
  // here means such a failure only loses the (best-effort) stats refresh.
  //
  // Why it still helps: autovacuum can't ANALYZE another connection's in-flight
  // tuples, so during a bulk load pg_class.reltuples stays stale and the planner
  // seq-scans tables the load is filling -- e.g. FIND_URL_SEQ_QUERY degrades to
  // O(N) per row, O(N²) over the load. Because callers commit each batch before
  // calling this, the rows ARE visible here; ANALYZE refreshes stats and its
  // cross-backend cache invalidation makes the loader replan onto the index.
  //
  // A per-table advisory lock serializes concurrent refreshes so they don't
  // collide (and so we skip redundant work: the holder's ANALYZE benefits us
  // too). The advisory lock is transaction-scoped and released by the commit.
  private void analyzeTableOutOfBand(String table) {
    Connection conn = null;
    try {
      conn = getConnection();

      boolean locked;
      try (PreparedStatement lockPs =
               conn.prepareStatement("SELECT pg_try_advisory_xact_lock(?, ?)")) {
        lockPs.setInt(1, ANALYZE_ADVISORY_LOCK_NAMESPACE);
        lockPs.setInt(2, table.hashCode());
        try (ResultSet rs = lockPs.executeQuery()) {
          locked = rs.next() && rs.getBoolean(1);
        }
      }

      if (locked) {
        try (Statement st = conn.createStatement()) {
          st.execute("ANALYZE " + table);
        }
      }

      // Commit to release the advisory lock (and the ANALYZE's catalog updates).
      DbManager.commitOrRollback(conn, log);
    } catch (SQLException | DbException e) {
      // Best-effort: a failed ANALYZE (e.g. "tuple concurrently updated") aborts
      // only this throwaway transaction; the finally rolls it back and closes.
      log.warn("Out-of-band ANALYZE {} failed (ignored): {}", table, e.getMessage());
    } finally {
      if (conn != null) {
        DbManager.safeRollbackAndClose(conn);
      }
    }
  }

  /** Record that one new row was added to the urls table, seeding the estimate
   *  first if this is the first url we've seen (the table may be non-empty at
   *  startup). */
  private void noteUrlRowCreated() {
    seedUrlEstimateIfNeeded();
    urlTableRowEstimate.incrementAndGet();
  }

  /** Lazily seed the urls-table row estimate from the database, since the index
   *  is not necessarily empty when we start. Uses pg_class.reltuples (an
   *  estimate; no full table scan). If the table has never been analyzed
   *  (reltuples < 0) or the estimate can't be read, it seeds to 0, which only
   *  makes us ANALYZE early -- harmless and self-correcting. */
  private long seedUrlEstimateIfNeeded() {
    long cur = urlTableRowEstimate.get();
    if (cur >= 0) {
      return cur;
    }
    long seed = 0;
    try {
      seed = estimatedRowCount(URL_TABLE);
    } catch (DbException e) {
      log.warn("Could not seed urls row estimate; assuming empty: {}", e.getMessage());
    }
    // Only the first thread to seed wins; others adopt whatever value was set.
    if (urlTableRowEstimate.compareAndSet(-1, seed)) {
      synchronized (urlAnalyzeLock) {
        urlRowsAtLastAnalyze = seed;
      }
      return seed;
    }
    return urlTableRowEstimate.get();
  }

  /** Return PostgreSQL's estimated live row count for a table from
   *  pg_class.reltuples, clamped at 0 (reltuples is -1 when never analyzed).
   *  Package-visible for testing the non-empty-at-startup seed path. */
  long estimatedRowCount(String table) throws DbException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      ps = idxDbManager.prepareStatement(conn,
          "SELECT reltuples::bigint FROM pg_class WHERE relname = ?");
      ps.setString(1, table);
      rs = idxDbManager.executeQuery(ps);
      long n = rs.next() ? rs.getLong(1) : 0;
      DbManager.commitOrRollback(conn, log);
      return Math.max(n, 0);
    } catch (SQLException e) {
      throw new DbException("Could not read estimated row count for " + table, e);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /** Refresh urls-table planner stats on a geometric cadence scoped to the whole
   *  table (not one addArtifacts call, since the table grows across many AUs):
   *  ANALYZE once the table has roughly doubled since the last refresh, with a
   *  floor for the initial cold-table flip, and not at all once the table is
   *  large enough that the seq-scan -> index-scan plan is settled -- autovacuum
   *  owns steady state from there. */
  private void maybeAnalyzeUrls() {
    seedUrlEstimateIfNeeded();
    long cur = urlTableRowEstimate.get();

    boolean doAnalyze = false;
    synchronized (urlAnalyzeLock) {
      if (shouldAnalyzeUrls(cur, urlRowsAtLastAnalyze)) {
        urlRowsAtLastAnalyze = cur;
        doAnalyze = true;
      }
    }

    if (doAnalyze) {
      analyzeTableOutOfBand(URL_TABLE);
    }
  }

  /**
   * Decide whether the urls table is due for an ANALYZE, given its current
   * estimated row count and the estimate as of the previous ANALYZE.
   *
   * <p>The cadence is geometric: refresh stats each time the table has
   * <em>doubled</em> since the last ANALYZE. Two bounds shape that:
   * <ul>
   *   <li><b>floor</b> ({@link #URL_ANALYZE_MIN_ROWS}): a near-empty table
   *       "doubles" after a handful of rows (2&nbsp;&times;&nbsp;0&nbsp;==&nbsp;0),
   *       which would fire on almost every batch, so we first wait for at least
   *       this much growth; and
   *   <li><b>cap</b> ({@link #URL_ANALYZE_CAP_ROWS}): once the table is this
   *       large the seq-scan&nbsp;&rarr;&nbsp;index-scan plan for the url lookup
   *       is settled and further refreshes won't change it, so we stop and let
   *       autovacuum own steady state.
   * </ul>
   *
   * @param currentRows       current estimated urls row count
   * @param rowsAtLastAnalyze the estimate at the time of the last ANALYZE
   * @return whether an ANALYZE should be run now
   */
  static boolean shouldAnalyzeUrls(long currentRows, long rowsAtLastAnalyze) {
    // Past the cap: plan is settled, leave stats to autovacuum.
    if (currentRows >= URL_ANALYZE_CAP_ROWS) {
      return false;
    }
    // Fire once the table reaches double its size at the last ANALYZE, but never
    // before it has grown by at least the floor.
    long doubledSize = rowsAtLastAnalyze * 2;
    long flooredSize = rowsAtLastAnalyze + URL_ANALYZE_MIN_ROWS;
    long nextAnalyzeAt = Math.max(doubledSize, flooredSize);
    return currentRows >= nextAnalyzeAt;
  }

  private void addArtifact(Connection conn, long auidSeq, long namespaceSeq, long urlSeq, Artifact artifact)
      throws DbException {

    PreparedStatement ps = idxDbManager.prepareStatement(conn, INSERT_ARTIFACT_QUERY);

    try {
      bindArtifactInsertParams(ps, namespaceSeq, auidSeq, urlSeq, artifact);
      idxDbManager.executeUpdate(ps);
    } catch (SQLException e) {
      log.error("Error preparing SQL statement", e);
      throw new DbException("Error preparing SQL statement", e);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  private static void bindArtifactInsertParams(PreparedStatement ps,
      long namespaceSeq, long auidSeq, long urlSeq, Artifact artifact)
      throws SQLException {
    ArtifactIdentifier artifactId = artifact.getIdentifier();
    ps.setString(1, artifactId.getUuid());
    ps.setLong(2, namespaceSeq);
    ps.setLong(3, auidSeq);
    ps.setLong(4, urlSeq);
    ps.setInt(5, artifactId.getVersion());
    ps.setBoolean(6, artifact.isCommitted());
    ps.setString(7, artifact.getStorageUrl());
    ps.setLong(8, artifact.getContentLength());
    ps.setString(9, artifact.getContentDigest());
    ps.setLong(10, artifact.getCollectionDate());
  }

  public void upsertArtifactForReindex(Artifact artifact) throws DbException {
    log.debug2("artifact = {}", artifact);

    Connection conn = null;

    try {
      conn = getConnection();
      upsertArtifactForReindex(conn, artifact);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  public void upsertArtifactsForReindex(Iterable<Artifact> artifacts) throws DbException {
    Connection conn = null;

    try {
      conn = getConnection();

      for (Artifact artifact : artifacts) {
        upsertArtifactForReindex(conn, artifact);
      }

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private void upsertArtifactForReindex(Connection conn, Artifact artifact) throws DbException {
    long namespaceSeq = findOrCreateNamespaceSeq(conn, artifact.getNamespace());
    long auidSeq = findOrCreateAuidSeq(conn, artifact.getAuid());
    long urlSeq = findOrCreateUrlSeq(conn, artifact.getUri());
    upsertArtifactForReindex(conn, auidSeq, namespaceSeq, urlSeq, artifact);
  }

  private void upsertArtifactForReindex(Connection conn, long auidSeq, long namespaceSeq, long urlSeq, Artifact artifact)
      throws DbException {

    PreparedStatement ps = idxDbManager.prepareStatement(conn, UPSERT_ARTIFACT_FOR_REINDEX_QUERY);
    ArtifactIdentifier artifactId = artifact.getIdentifier();

    try {
      ps.setString(1, artifactId.getUuid());
      ps.setLong(2, namespaceSeq);
      ps.setLong(3, auidSeq);
      ps.setLong(4, urlSeq);
      ps.setInt(5, artifactId.getVersion());
      ps.setBoolean(6, artifact.isCommitted());
      ps.setString(7, artifact.getStorageUrl());
      ps.setLong(8, artifact.getContentLength());
      ps.setString(9, artifact.getContentDigest());
      ps.setLong(10, artifact.getCollectionDate());
      ps.setBoolean(11, artifact.isCommitted());
      ps.setString(12, artifact.getStorageUrl());

      idxDbManager.executeUpdate(ps);
    } catch (SQLException e) {
      log.error("Error preparing SQL statement", e);
      throw new DbException("Error preparing SQL statement", e);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public int updateStorageUrl(String uuid, String storageUrl) throws DbException {
    log.debug2("uuid = {}", uuid);
    log.debug2("storageUrl = {}", storageUrl);

    Connection conn = null;
    int rowsUpdated = 0;

    try {
      // Get a connection to the database
      conn = getConnection();

      // Update the storage URL
      rowsUpdated = updateStorageUrl(conn, uuid, storageUrl);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
      return rowsUpdated;
    }
  }

  private int updateStorageUrl(Connection conn, String uuid, String storageUrl) throws DbException {
    log.debug2("artifactId = {}", uuid);
    log.debug2("storageUrl = {}", storageUrl);

    PreparedStatement ps = null;
    String errorMessage = "Cannot update storage URL";

    try {
      // Prepare the query.
      ps = idxDbManager.prepareStatement(conn, UPDATE_ARTIFACT_STORAGE_URL_QUERY);
      ps.setString(1, storageUrl);
      ps.setString(2, uuid);

      // Execute the query
      return idxDbManager.executeUpdate(ps);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPDATE_ARTIFACT_STORAGE_URL_QUERY);
      log.error("artifactId = {}", uuid);
      log.error("storageUrl = {}", storageUrl);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public int deleteArtifact(String uuid) throws DbException {
    log.debug2("uuid = {}", uuid);

    Connection conn = null;
    int rowsDeleted = 0;

    try {
      // Get a connection to the database
      conn = getConnection();

      // Delete the artifact
      rowsDeleted = deleteArtifact(conn, uuid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
      return rowsDeleted;
    }
  }

  private int deleteArtifact(Connection conn, String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    PreparedStatement ps = null;
    String errorMessage = "Cannot delete artifact";

    try {
      // Prepare the query.
      ps = idxDbManager.prepareStatement(conn, DELETE_ARTIFACT_QUERY);
      ps.setString(1, uuid);


      // Execute the query
      int rows = idxDbManager.executeUpdate(ps);
      // If a row was deleted, delete any orphaned itmes from the
      // Namespace, AUID and URL tables, flushing the respective
      // caches for any deleted orphans.  (Could be more selective and
      // only delete the cache entries for the items that were
      // deleted, but deletion is fairly rare and the queries would
      // have to be changes to return the SEQ numbers
      if (rows > 0) {
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_NAMESPACE_QUERY);
        if (idxDbManager.executeUpdate(ps) > 0) {
          lru_namespace_seqs.clear();
        }
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_AUID_QUERY);
        if (idxDbManager.executeUpdate(ps) > 0) {
          lru_auids_seqs.clear();
        }
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_URL_QUERY);
        if (idxDbManager.executeUpdate(ps) > 0) {
          lru_urls_seqs.clear();
        }
      }

      return rows;
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_ARTIFACT_QUERY);
      log.error("artifactId = {}", uuid);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public void clearAllArtifacts() throws DbException {
    Connection conn = null;
    try {
      conn = getConnection();
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DELETE FROM " + ARCHIVAL_UNIT_SIZE_TABLE);
        stmt.executeUpdate("DELETE FROM " + ARTIFACT_TABLE);
        stmt.executeUpdate("DELETE FROM " + URL_TABLE);
        stmt.executeUpdate("DELETE FROM " + AUID_TABLE);
        stmt.executeUpdate("DELETE FROM " + NAMESPACE_TABLE);
      }
      DbManager.commitOrRollback(conn, log);
      lru_namespace_seqs.clear();
      lru_auids_seqs.clear();
      lru_urls_seqs.clear();
      log.info("Cleared all artifact index tables");
    } catch (SQLException | DbException e) {
      throw new DbException("Could not clear artifact index", e);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  public void deleteAuSize(String auid) throws DbException {
    log.debug2("auid = {}", auid);

    Connection conn = null;

    try {
      // Get a connection to the database
      conn = getConnection();

      // Update the AU size
      deleteAuSize(conn, auid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Long updateAuSize(Connection conn, String auid, AuSize auSize)
      throws DbException {

    log.debug2("auid = {}", auid);
    log.debug2("auSize = {}", auSize);

    Long auidSeq = null;

    try {
      // Find the AUID sequence number, or add an entry for it
      auidSeq = findOrCreateAuidSeq(conn, auid);

      // TODO: Replace with UPDATE operation

      // Delete any existing AuSize of the AU associated with this AUID
      int deletedCount = deleteAuSize(conn, auidSeq);
      log.trace("deletedCount = {}", deletedCount);

      // Add the new AuSize of the AU associated with this AUID
      int addedCount = addAuSize(conn, auidSeq, auSize);
      log.trace("addedCount = {}", addedCount);
    } catch (DbException e) {
      String message = "Cannot update AU size";
      log.error(message, e);
      log.error("auid = {}", auid);
      log.error("auSize = {}", auSize);
      throw e;
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  private long deleteAuSize(Connection conn, String auid)
      throws DbException {
    log.debug2("auid = {}", auid);

    Long auidSeq = null;

    try {
      // Find the AUID sequence number, or add an entry for it
      auidSeq = findOrCreateAuidSeq(conn, auid);

      // Delete any existing AuSize of the AU associated with this AUID
      int deletedCount = deleteAuSize(conn, auidSeq);
      log.trace("deletedCount = {}", deletedCount);
    } catch (DbException e) {
      String message = "Cannot update AU size";
      log.error(message, e);
      log.error("auid = {}", auid);
      throw e;
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  /**
   * Deletes from the database the AU sizes of an AU.
   *
   * @param conn    A Connection with the database connection to be used.
   * @param auidSeq A Long with the database identifier of the AUID.
   * @return an int with the count of database rows deleted.
   * @throws DbException if any problem occurred accessing the database.
   */
  private int deleteAuSize(Connection conn, Long auidSeq) throws DbException {
    log.debug2("auidSeq = {}", auidSeq);

    int result = -1;
    PreparedStatement deleteAuSize = null;
    String errorMessage = "Cannot delete AU size";

    try {
      // Prepare the query.
      deleteAuSize = idxDbManager.prepareStatement(conn,
          DELETE_AU_SIZE_QUERY);

      // Populate the query.
      deleteAuSize.setLong(1, auidSeq);

      // Execute the query
      result = idxDbManager.executeUpdate(deleteAuSize);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_AU_SIZE_QUERY);
      log.error("auidSeq = {}", auidSeq);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(deleteAuSize);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the sizes of an AU.
   *
   * @param conn    A Connection with the database connection to be used.
   * @param auidSeq A Long with the database identifier of the AUID.
   * @param auSize  An {@link AuSize} with the AU's content sizes.
   * @return an int with the count of database rows added.
   * @throws DbException if any problem occurred accessing the database.
   */
  private int addAuSize(Connection conn, Long auidSeq, AuSize auSize)
      throws DbException {
    log.debug2("auidSeq = {}", auidSeq);
    log.debug2("auSize = {}", auSize);

    PreparedStatement addAuSize = null;
    String errorMessage = "Cannot add AU size";

    try {
      // Prepare the query.
      addAuSize = idxDbManager.prepareStatement(conn, ADD_AU_SIZE_QUERY);

      // Populate the query.
      addAuSize.setLong(1, auidSeq);
      addAuSize.setLong(2, auSize.getTotalLatestVersions());
      addAuSize.setLong(3, auSize.getTotalAllVersions());
      addAuSize.setLong(4, auSize.getTotalWarcSize());
      addAuSize.setLong(5, TimeBase.nowMs());

      // Execute the query
      int count = idxDbManager.executeUpdate(addAuSize);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_SIZE_QUERY);
      log.error("auidSeq = {}", auidSeq);
      log.error("auSize = {}", auSize);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(addAuSize);
    }
  }

  public long getSizeOfArtifacts(String namespace, String auid, VersionsEnum versions)
      throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("versions = {}", versions);

    Connection conn = null;

    try {
      conn = getConnection();
      return getSizeOfArtifacts(conn, namespace, auid, versions);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private long getSizeOfArtifacts(
      Connection conn, String namespace, String auid, VersionsEnum versions) throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifacts";

    String sqlQuery = versions == VersionsEnum.LATEST ?
        GET_SIZE_OF_LATEST_ARTIFACTS_QUERY :
        GET_SIZE_OF_ARTIFACTS_QUERY;

    try {
      // Prepare the query
      ps = idxDbManager.prepareStatement(conn, sqlQuery);

      // Populate the query
      ps.setString(1, namespace);
      ps.setString(2, auid);

      resultSet = idxDbManager.executeQuery(ps);

      if (resultSet.next()) {
        return resultSet.getLong("total_size");
      } else {
        // Q: Is this right?
        return 0L;
      }
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", sqlQuery); // FIXME
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("versions = {}", versions);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }
}
