# SQLArtifactIndexManagerSql — Code Review Findings

**Date:** 2026-08-04. **Last updated:** 2026-08-05.

**Reviewed:** `src/main/java/org/lockss/rs/io/index/db/SQLArtifactIndexManagerSql.java` (3514 lines at review time, 3335 now, PostgreSQL-backed), together with its schema (`SQLArtifactIndexDbManagerSql.java`), its iterator (`PagingArtifactIterator.java`), and the callers and sibling `ArtifactIndex` implementations reachable from it.

Findings were produced by a multi-lens review (SQL semantics, transaction/concurrency behaviour, resource handling, API contract, schema consistency), and the high-severity ones were verified directly against the source rather than inferred.

### State as of this update

Four findings moved in this update — 4, 5, 6 and 11. Everything else is unfixed and was re-verified against the current tree on 2026-08-05; it is recorded for later action, not because it is being worked on. Standing as of today:

- **Fixed:** 0 (prefix `LIKE` escaping — fixed before this update, but its description of the shipped fix has been corrected), 4 (duplicate-key retry loops), 5 (`urls.url` uniqueness), 15b (AUID collation).
- **Dissolved** — the code the finding described no longer exists: 6 (2500-character head collision), finding 14's `long_urls` unique-constraint bullet.
- **Partially fixed:** 11 (`addUrl`'s statement leak is gone with the long-URL branch; `deleteArtifact` still leaks three).
- **Unfixed:** 1, 2, 3, 7, 8, 9, 10, 12, 13, the rest of 14, and 15a / 15c.

Two changes account for all of it, and they are at different stages:

- **Committed** — `253429ec`, "Fold long_urls into urls.url and restore URL uniqueness (schema v5)". Removes `long_urls`, restores uniqueness on a digest, converts the `findOrCreate*Seq` paths to `INSERT … ON CONFLICT … RETURNING`, and rebuilds `idx1_urls` over a bounded prefix expression. This commit spells the digest `md5(url)`.
- **Working tree, uncommitted** — replaces that `md5` with pgcrypto's `digest(url, 'sha256')`, adds `SqlConstants.URL_DIGEST_EXPRESSION` / `URL_DIGEST_PARAM_EXPRESSION`, installs the extension as the first step of the v5 migration, and adds a collision test. Anything below that says SHA-256 describes the working tree, not `253429ec`.

**Anchors:** `file:line` anchors on **unfixed** findings are current as of `253429ec` plus that working tree. Anchors on **fixed** or **dissolved** findings are historical — they point at the revision reviewed on 2026-08-04 and are kept only so the analysis can be followed.

Verified green on 2026-08-05 against the current tree (JDK 17, embedded PostgreSQL): `TestSQLArtifactIndexManagerSqlPaging` 86/86, `TestSQLArtifactIndexManagerSqlPrefixUpperBound` 10/10, `TestSQLArtifactIndexManagerSqlPrefixWildcards` 9/9, `TestSQLArtifactIndexUpsertRace` 5/5, `TestSQLArtifactIndexDbCollation` 5/5 — 115 tests, 0 failures. The paging suite matters most here: `253429ec` rewrote every paging query, and 86/86 still pass.

## Summary

Anchors on unfixed rows are current; anchors on fixed rows are historical.

| # | Finding | Severity | Primary anchor | Status |
|---|---|---|---|---|
| 0 | URL prefix predicates interpolate into SQL `LIKE` without escaping | HIGH | `SQLArtifactIndexManagerSql.java:2146` (historical) | **FIXED** |
| 1 | `getNamespaces()` returns every namespace, always | HIGH | `SQLArtifactIndexManagerSql.java:146-152` | Unfixed |
| 2 | Orphan cleanup never deletes anything | HIGH | `SQLArtifactIndexManagerSql.java:690-698` | Unfixed |
| 3 | `return` inside `finally` swallows every exception | HIGH | `SQLArtifactIndexManagerSql.java:3047-3049` | Unfixed |
| 4 | Duplicate-key retry loops can never succeed | HIGH | `SQLArtifactIndexManagerSql.java:1273-1303` (historical) | **FIXED** |
| 5 | `urls.url` uniqueness dropped in v4; duplicate URL rows split version chains | HIGH | `SQLArtifactIndexDbManagerSql.java:193-197` (historical) | **FIXED** |
| 6 | A URL of exactly 2500 chars can collide with a long URL's stored head | MEDIUM | `SQLArtifactIndexManagerSql.java:77-81` (historical) | **DISSOLVED** |
| 7 | The keyset tuple is not provably unique | MEDIUM-HIGH | `SQLArtifactIndexManagerSql.java:775-783` | Unfixed |
| 8 | Returned `Iterable<Artifact>` is single-use | MEDIUM-HIGH | `SQLArtifactIndexManagerSql.java:2223` | Unfixed |
| 9 | Mid-iteration DB failure escapes as a bare `RuntimeException` | MEDIUM-HIGH | `PagingArtifactIterator.java:206-211` | Unfixed |
| 10 | Paging is not a consistent snapshot, and this is undocumented | MEDIUM | `SQLArtifactIndexManagerSql.java:1421` | Unfixed |
| 11 | `PreparedStatement` leaks | MEDIUM | `SQLArtifactIndexManagerSql.java:3074-3084` | Partially fixed (`addUrl` clean; `deleteArtifact` unfixed) |
| 12 | ANALYZE-cadence path acquires a second connection while holding the first | MEDIUM | `SQLArtifactIndexManagerSql.java:971` | Unfixed |
| 13 | Substitution tokens are SQL line comments | LOW (latent) | `SQLArtifactIndexManagerSql.java:775-783` | Unfixed (new sites use a safe token) |
| 14 | Miscellaneous | LOW | `SQLArtifactIndexManagerSql.java:2931` | Unfixed |
| 15 | SQL and Volatile `ArtifactIndex` implementations disagree on result ordering | MEDIUM | `ArtifactIndex.java:218` | Partially fixed (15b fixed; 15a and 15c unfixed) |

## The `long_urls` redesign — landed

This section previously described a planned redesign. It shipped in `253429ec` and is recorded here as history, because three findings below were resolved by it rather than by anything aimed at them.

What landed. `urls.url` is unbounded `TEXT` and holds every URL whole. `long_urls`, `LONG_URL_THRESHOLD`, the 13 `LONG_URL_*` query constants, the 30 `concat(u.url, lu.long_url)` expressions, the 20 `LEFT JOIN long_urls` clauses and the head/tail branch in nine methods are all gone. Uniqueness moved onto a digest of the URL — `md5` as committed, `digest(url, 'sha256')` in the working tree — because a btree index row caps at 2704 bytes and a URL is not bounded. The read path already reassembled the full URL through `concat()`, which ignores NULL, so the rewrites are value-preserving on existing data.

Resolved by it:

- **Finding 5** — fixed. Uniqueness is restored on the digest, and the insert path is `INSERT … ON CONFLICT`, exactly as the fix direction proposed.
- **Finding 6** — dissolved. There is no head, no tail and no threshold left to collide on.
- **Finding 14's `long_urls` unique-constraint bullet** — dissolved with the table.

**Finding 15c survived, as predicted.** The prediction was that when `long_urls` went, the sort expression would simply lose its `concat(...)` and become `replace(url, '/', E'\t') COLLATE "C"`, with every 15c divergence intact. That is now confirmed against the source — `SORT_URI_EXPR:789-790` is precisely that, and the single expression replaced both of the former long/short variants. 15c is unchanged and unfixed.

---

## 0. URL prefix predicates interpolate into SQL `LIKE` without escaping — HIGH (FIXED)

**Anchors (historical):** `SQLArtifactIndexManagerSql.java:2146/2150`, `:2241/2245`, `:2332/2337`

*What follows describes the code as it stood on 2026-08-04. It is retained so the reasoning can be followed; the outcome is at the end of the section.*

Three page fetchers build a `LIKE` pattern by string concatenation and bind it to a `LIKE ?` predicate. There is no `ESCAPE` clause on any of the eight `LIKE ?` predicates in the file, and the repo contains no LIKE-escaping helper.

```java
      if (isLongUrl) {
        String pattern = urlPrefix.substring(LONG_URL_THRESHOLD) + "%";
        ps.setString(paramIndex++, urlPrefix.substring(0, LONG_URL_THRESHOLD));
        ps.setString(paramIndex++, pattern);
      } else {
        String pattern = urlPrefix + "%";
        ps.setString(paramIndex++, pattern);
      }
```

**What's wrong**

- `%` and `_` in a caller's prefix act as wildcards. The result is an over-match — a strict superset, so no rows are lost, but rows that do not share the prefix are returned.
- `_` is the more common real-world trigger: underscores are ubiquitous in URLs, whereas `%` mostly arrives via percent-encoding.
- A backslash **under**-matches, because PostgreSQL treats `\` as `LIKE`'s default escape character when no `ESCAPE` clause is given. This is dialect-dependent: Derby follows the standard and has no default escape.
- Behaviour depends on prefix length. In the long-URL branch the first 2500 characters bind to `u.url = ?` (a literal comparison) and only `substring(2500)` reaches the `LIKE`, so the same prefix behaves differently either side of the threshold.

**Failure scenario**

Caller asks for all artifacts under prefix `http://example.org/a_b/`. The `_` matches any character, so `http://example.org/aXb/` and every other single-character variant is returned as well. The caller iterates them as if they were under the requested prefix.

This is reachable straight from the public API: the Q5/Q6/Q7 wrappers apply no `startsWith` filter of their own, so `BaseLockssRepository.getArtifactsWithUrlPrefixFromAllAus` hands wildcard-expanded results back to its caller.

It also diverges from the sibling implementations, which both treat the prefix literally — `VolatileArtifactIndex` uses `filterByURIPrefix` (a `startsWith`), and `SolrArtifactIndex` uses `{!prefix f=uri}`.

**Fix — landed**

`LIKE ?` was replaced with a `COLLATE "C"` range predicate. What shipped, **as amended by the `long_urls` fold** — the fold rewrote the predicate's shape, so the original description of this fix is no longer accurate and is corrected here:

- All 8 prefix `LIKE ?` predicates were replaced by a runtime-substituted token `@@UrlPrefixCondition@@`. Zero `LIKE` predicates remain in the file — the only textual occurrences of the word are javadoc explaining the change.
- **The token's expansion is no longer a plain range on the column.** It was `AND col COLLATE "C" >= ?` plus, when an upper bound existed, `AND col COLLATE "C" < ?`. Since `urls.url` became unbounded, a plain btree over it is not a legal index, so `urlPrefixCondition(column, idxBounded, recheck, …)` at `:1777-1799` now emits its leading term over `left(col, 600) COLLATE "C"` — the expression `idx1_urls` is built on — and adds the exact comparison against the column itself only as a **recheck**, when the prefix is longer than that bound and the truncated range has collapsed to equality. For any prefix of 600 characters or fewer the leading term is exact on its own (`left(U,600)` starts with P iff U does), so no recheck is emitted.
- A package-private `static String prefixUpperBound(String prefix)` computes the bound. It increments the last **code point** (not the last `char`), carries left past `U+10FFFF`, skips the surrogate range `D800`–`DFFF`, returns `null` when no bound exists (empty prefix, or a prefix consisting entirely of `U+10FFFF`), and throws `IllegalArgumentException` on `null` input. When it returns `null` the predicate degrades to `>= ?` alone, which is correct: `>= ''` is trivially true. This is unchanged by the fold, and is now applied twice per site — once to the truncated prefix for the index term, once to the full prefix for the exact range.
- 12 prefix query constants were changed. The four `MAX_COMMITTED_VERSION_*_PREFIX_QUERY` constants are inlined into the LATEST variants, so the token appears exactly once per assembled query.
- **Placeholder alignment: not re-verified since the fold.** The original count was 24 prefix assemblies — 12 pre-existing bounded shapes, 6 long-branch, 6 short-unbounded — all aligned. The long/short dimension no longer exists, so that breakdown is void; the new dimension is bounded/unbounded × recheck/no-recheck. This needs redoing against the current assemblies.
- Tests, re-run 2026-08-05 against the current tree: `TestSQLArtifactIndexManagerSqlPrefixWildcards` 9/9 green (all 8 were failing before the fix), `TestSQLArtifactIndexManagerSqlPrefixUpperBound` 10/10 green, and `TestSQLArtifactIndexManagerSqlPaging` still 86/86 green after the fold rewrote its queries.

### Design note on the fix — do not undo this

The `COLLATE "C"` is required, not decorative. This is now empirically confirmed, not merely argued: the embedded PostgreSQL used by the test suite reports `datcollate = en_US.UTF-8`, i.e. **not** `C`, so the suite genuinely exercises the collate clause rather than passing by accident on a C-locale database. The specific assertion that depends on it is `testLengthAsymmetryOfPercentHandling_Q5` case 2: prefix tail `q%r/` yields upper bound `q%r0`, and the decoy `qZZr/fileE.html` is excluded only because `Z` (0x5A) sorts above `%` (0x25) in byte order — an ordering that `en_US.UTF-8` punctuation weighting does not guarantee. Remove the `COLLATE "C"` and that test fails on the deployed collation.

The underlying reason. The database is created via `create database ... with template template0` (`DbManagerSql.java:117`) with no `LC_COLLATE`, so it inherits the cluster default, which is generally not `C`. Under a locale collation, comparison is not character-by-character, so `url >= p AND url < upper(p)` is **not** equivalent to a literal prefix match. This is exactly why PostgreSQL requires C locale or `text_pattern_ops` before it will optimize `LIKE 'x%'` into a range scan.

**Performance — the index win was realized in database version 5, then re-shaped by the `long_urls` fold.** Three stages, in order:

1. When the range predicate first landed it was performance-neutral. `idx1_urls` was a plain btree in the default collation, so a `COLLATE "C"` range predicate could not use it — though neither could the `LIKE` it replaced, for the same reason.
2. v5 declared `urls.url` and `auids.auid` as `COLLATE "C"`, which made `idx1_urls` usable by the range predicate. Measured then on embedded PostgreSQL 14.10 at `datcollate = en_US.UTF-8` — a genuine non-C locale — over 20k rows, the plan went from `Seq Scan on urls (cost=0.00..487.00 rows=1 width=8)` to a `Bitmap Index Scan on idx1_urls` with `Index Cond: ((url >= '...ab') AND (url < '...ac'))`, and the full production query drove `urls` through `idx1_urls` over 50k artifacts. The mechanism worth recording: `pg_index.indcollation` on `idx1_urls` went from `default` to `C`. That — not the column declaration itself — is what the planner matches a range predicate against.
3. **That exact plan no longer applies.** Once `urls.url` became unbounded, a plain btree over it stopped being a legal index — a btree index row cannot exceed 2704 bytes, and the only reason the old index was legal is that v4 truncated the column to 2500 characters. `idx1_urls` is now rebuilt over a bounded expression, `left(url, 600) COLLATE "C"` (`SQLArtifactIndexDbManagerSql.java:460-462`; the bound is 600 rather than 2000 because the btree limit is in bytes and UTF-8 runs to four bytes per character). `urlPrefixCondition:1777-1799` drives the scan off that expression and keeps the exact range on `urls.url` as a recheck, added only when the prefix is longer than the bound — at which point the truncated range degrades to equality. **The stage-2 EXPLAIN above has not been re-measured against this index shape.**

**Keep the redundant explicit `COLLATE "C"` in the SQL.** Plans were byte-identical with and without it, because the planner normalizes an explicit collation that matches the column's declared one. It therefore costs nothing, and it remains the safety net if the database is ever restored into a differently-collated cluster.

### Migration runbook for v5

**Superseded in part.** This runbook was written when v5 was a collation-only change. v5 now also folds `long_urls` into `urls.url`, dedups URL rows, drops and rebuilds `idx1_urls`, and builds a new unique index over the whole table. What each original bullet is still worth is marked below; the authoritative operator notes now live in the javadoc on `updateDatabaseFrom4To5` (`SQLArtifactIndexDbManagerSql.java:586-639`), whose figures come from a 108M-row production index rather than from the 20k/50k-row measurements here.

The steps, in the order `updateDatabaseFrom4To5:640-709` runs them: install `pgcrypto` → drop `idx1_urls` → the two `COLLATE "C"` ALTERs → merge long-URL tails → build the dedup temp tables → count and report version collisions → repoint artifacts, delete duplicate URLs → drop `long_urls` → `CREATE UNIQUE INDEX` on the digest → rebuild `idx1_urls` over `left(url, 600) COLLATE "C"`.

- **New, and the first thing that can fail: `pgcrypto`.** The uniqueness constraint is declared on `digest()`, which that extension supplies, so the migration issues `CREATE EXTENSION IF NOT EXISTS pgcrypto` before anything else — deliberately, so a server missing the contrib package or a connection without database-owner rights fails immediately rather than after the expensive steps. `pgcrypto` has been a trusted extension since PostgreSQL 13, so the database owner can install it without superuser; verified against PostgreSQL 14 as a `NOSUPERUSER NOCREATEDB` role owning the database. `createPgcryptoExtension:725-741` re-reports the failure with what an operator has to do about it. *(Working tree only — `253429ec` uses `md5()`, which is built in and needs no extension.)*
- **No heap rewrite — still true, and still only about the collation ALTERs.** `urls` and `auids` kept their relfilenode; PostgreSQL skips the rewrite for a collation-only change on an otherwise identical type. Only the indexes are rebuilt — `idx1_urls` 16393→16412 and `idx2_auids` 16403→16413, taking 15 ms and 9 ms at 50k and 20k rows respectively. **Do not read this as the cost of the migration.** It is no longer the dominant step, and `idx1_urls` is now dropped *before* the ALTER precisely so a 108M-row btree is not rebuilt only to be discarded moments later.
- **Lock window — no longer just the ALTERs, of which there are now two rather than three.** `long_urls.long_url` is deliberately not collated, because this same migration drops the table. Every step above runs on one connection, so the whole sequence holds its locks together, and the expensive part is now `CREATE UNIQUE INDEX` over the digest rather than the collation change. A `CONCURRENTLY` build of the equivalent index measured 16 minutes on the 108M-row index; a plain build under the lock is typically well under that, but budget for it. Set `maintenance_work_mem` (not `work_mem`) and `max_parallel_maintenance_workers` on the migrating connection — at the 64 MB / 2 defaults that build spills and runs far longer. The dedup itself is cheap: 98,791 repointed artifact rows and 16,062 deleted URL rows, seconds of work.
- **Required after migrating: `ANALYZE urls; ANALYZE auids;` — unchanged.** The ALTER discards column statistics; `pg_stats` rows went 1 → 0 for both. Immediately post-ALTER the plan still used the index but estimated 250 rows against an actual ~1992, a default-selectivity guess, which corrected after `ANALYZE`. At production scale a bad estimate on these joined queries can flip join order even with the index available. `ANALYZE` is deliberately left out of `updateDatabaseFrom4To5` so it does not extend the exclusive lock; `analyzeTableOutOfBand` and the geometric cadence are the intended vehicle.
- **New: check for `artifact-index-v5-url-dedup-collisions.csv` afterwards.** Collapsing duplicate URLs can leave two artifacts on the same `(namespace, auid, url, version)`. The migration reports rather than resolves that — it writes the affected artifacts to that file in the repository's data directory and continues, on the reasoning that a handful of ambiguous rows should not block a schema upgrade. The file is written only when there is something to report, so its presence *is* the alert, and each group needs a human decision about which artifact is authoritative. Identical digests within a group mean the same bytes were recorded twice, and the earliest `crawl_time` can simply be kept. Measured zero on the production index, which is expected rather than lucky: version assignment already filtered on the URL text, so duplicate `url_seq`s shared one version counter.
- **`long_urls` probing — moot.** The old caveat was that its `USING HASH` index had not been measured. The table is dropped by this migration, so the question no longer arises. The tail merge that precedes the drop was a no-op on every deployment measured — `long_urls` has been found empty everywhere, meaning the truncation path never fired — but it is run unconditionally so the migration is correct on deployments not yet measured.

---

## 1. `getNamespaces()` returns every namespace, always — HIGH

**Anchor:** `SQLArtifactIndexManagerSql.java:146-152`

The `EXISTS` subquery re-declares `namespaces ns` in its own `FROM` list, shadowing the outer alias. The correlation is therefore lost and the predicate degenerates to "does any artifact exist at all".

Assembled SQL:

```sql
SELECT DISTINCT ns.namespace
  FROM namespaces ns
 WHERE EXISTS ( SELECT FROM artifacts a, namespaces ns
                 WHERE a.namespace_seq = ns.namespace_seq )
```

The inner `ns` is a fresh scan of `namespaces`; nothing ties it to the outer row. The predicate is constant across all outer rows.

Contrast `GET_AUIDS_BY_NAMESPACE_QUERY` at `:668-677`, which is correctly correlated — its outer alias is `auid`, so re-declaring `ns` inside is harmless:

```sql
SELECT DISTINCT auid.auid
  FROM auids auid
 WHERE EXISTS ( SELECT FROM artifacts a, namespaces ns
                 WHERE a.auid_seq = auid.auid_seq
                   AND a.namespace_seq = ns.namespace_seq
                   AND ns.namespace = ? )
```

That the sibling query works is evidence the shadowing in `GET_NAMESPACES_QUERY` is accidental rather than intentional.

**Failure scenario**

Namespaces `nsA` and `nsB` both exist. Delete every artifact in `nsB`. `getNamespaces()` still returns `nsB`, because artifacts still exist in `nsA`.

**Fix direction**

Drop the redundant table from the subquery and correlate on the outer alias: `EXISTS (SELECT 1 FROM artifacts a WHERE a.namespace_seq = ns.namespace_seq)`.

**Test gap**

`TestSQLArtifactIndexDbManager.java:515-520` only checks the empty → one-namespace transition, which passes with either the correct or the broken predicate.

---

## 2. Orphan cleanup never deletes anything — HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:690-698` (the three queries), `:3061-3086` (the delete path)

All three `DELETE_ORPHANED_*` queries use an uncorrelated `NOT EXISTS`. The subquery references no outer column, so the predicate means "is the `artifacts` table empty".

Assembled SQL:

```sql
DELETE FROM namespaces
 WHERE NOT EXISTS ( SELECT DISTINCT ON (namespace_seq) namespace_seq FROM artifacts );

DELETE FROM auids
 WHERE NOT EXISTS ( SELECT DISTINCT ON (auid_seq) auid_seq FROM artifacts );

DELETE FROM urls
 WHERE NOT EXISTS ( SELECT DISTINCT ON (url_seq) url_seq FROM artifacts );
```

Zero rows are deleted under normal operation; the entire table is wiped when `artifacts` happens to be empty.

**Consequences**

- `urls`, `auids` and `namespaces` grow monotonically for the life of the index.
- The `lru_*.clear()` calls at `:3076`, `:3080` and `:3084` are gated on the update count being `> 0`, so they never fire — caches are never invalidated on delete.
- The ever-growing `urls` row count skews the row estimate feeding the ANALYZE cadence at `:2832`.

**Additional hazard when the branch does fire**

The `clear()` calls run inside the still-open transaction, before `commitOrRollback`:

```java
      if (rows > 0) {
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_NAMESPACE_QUERY);
        if (idxDbManager.executeUpdate(ps) > 0) {
          lru_namespace_seqs.clear();
        }
        ...
      }
```

A concurrent `findOrCreateUrlSeq` can therefore re-cache a `url_seq` that the pending commit then deletes.

**Failure scenario**

Delete the last artifact referencing `http://example.org/gone`. The `urls` row survives forever, and its `url_seq` remains in `lru_urls_seqs`. Separately, on a freshly emptied index, a single `deleteArtifact` wipes all three lookup tables while other sessions hold cached seq numbers for them.

**Fix direction**

Correlate each subquery against the outer row (e.g. `NOT EXISTS (SELECT 1 FROM artifacts a WHERE a.url_seq = u.url_seq)`), and move cache invalidation to after the commit.

---

## 3. `return` inside `finally` swallows every exception — HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:2998-3002` (`updateStorageUrl`), `:3045-3049` (`deleteArtifact`)

```java
      // Delete the artifact
      rowsDeleted = deleteArtifact(conn, uuid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
      return rowsDeleted;
    }
```

A `return` in a `finally` block aborts any exception propagating out of the `try`. The declared `throws DbException` becomes unreachable in practice.

**Failure scenario**

`deleteArtifact` executes the DELETE successfully, then `commitOrRollback` fails (deadlock, connection reset, constraint). The transaction is rolled back, so the index row still exists. The `DbException` is discarded by the `return`, and the method returns `1`. The caller concludes the index entry is gone and proceeds to delete the WARC content — index and storage diverge silently, with no error anywhere.

The same shape also turns a `getConnection()` failure into a "not found" result: `rowsDeleted` is still `0` at that point, so the method returns `0` instead of throwing.

**Fix direction**

Move the `return` out of the `finally` and leave the block with only `safeRollbackAndClose(conn)`.

---

## 4. The duplicate-key retry loops can never succeed — HIGH (FIXED)

**Anchors (historical):** `SQLArtifactIndexManagerSql.java:1273-1303` (namespace), `:1484-1514` (auid), `:1108-1138` (url)

*What follows describes the code as it stood on 2026-08-04. It is retained so the reasoning can be followed; the outcome is at the end of the section.*

```java
      if (namespaceSeq == null) {
        try {
          // Add the namespace to the database
          namespaceSeq = addNamespace(conn, namespace);
          log.trace("new namespaceSeq = {}", namespaceSeq);
        } catch (DbException e) {
          Throwable cause = e.getCause();
          if (cause != null && cause.getMessage().startsWith(DUPLICATE_KEY_ERROR_MESSAGE)) {
            log.warn("Race caused duplicate key violation on namespace: {}; retrying...", namespace);
            continue;
          }
          throw e;
        }
      }
```

Connections here are `autoCommit=false` (`DbManager.java:609-616` → `DbManagerSql.java:468`, whose javadoc states "Autocommit is disabled to allow the client code to manage transactions"). In PostgreSQL a unique violation aborts the **entire** transaction, not just the failing statement. After the `continue`, the loop re-runs `findNamespaceSeq(conn, …)` — which sits *outside* the inner try/catch — and that immediately fails with SQLState `25P02`, "current transaction is aborted, commands ignored until end of transaction block".

**Failure scenario**

Two threads insert the same new namespace concurrently. The loser hits the unique violation, logs a misleading "retrying..." warning, retries, and dies on `25P02`. A recoverable race becomes a hard failure. Inside `addArtifacts`, the aborted transaction kills the whole batch, not just the one artifact.

**Fix — landed (`253429ec`)**

The second of the two proposed directions was taken: all three paths are now `INSERT … ON CONFLICT … RETURNING`, and the retry loops are gone entirely.

- `UPSERT_URL_QUERY:113-119` uses `ON CONFLICT (<digest>) DO NOTHING RETURNING url_seq`. `findOrCreateUrlSeq:834-883` reads no returned row as "a concurrent transaction created it first" and falls back to `FIND_URL_SEQ_QUERY`. That is correct only because `DO NOTHING` waits for the conflicting transaction to end and suppresses the insert *only* if it committed, so the winner's row is visible to the fallback under READ COMMITTED.
- `UPSERT_NAMESPACE_QUERY:137-144` and `UPSERT_AUID_QUERY:167-174` use `DO UPDATE SET col = EXCLUDED.col RETURNING seq` — a no-op update whose purpose is that `DO UPDATE` always returns a row, so those two paths need no fallback at all.
- **Both secondary defects went with the loops.** `DUPLICATE_KEY_ERROR_MESSAGE` no longer exists anywhere in the file, so neither the NPE on a null cause message nor the English-server-string match remains. The suggestion to match SQLState `23505` is therefore moot rather than outstanding.
- A third defect was fixed in passing: `findOrCreateUrlSeq` had a non-atomic `containsKey()`/`get()` pair on the sequence LRU, which could return null on a concurrent eviction and be unboxed into a `long`. It is now a single `get()` (`:842-845`).

`TestSQLArtifactIndexUpsertRace` pins these semantics against a real PostgreSQL rather than against the documentation — including that `DO NOTHING` returns no row only after a committed winner, and that `DO UPDATE` always returns one. 5/5 green.

**One residue, deliberately unreachable.** When the fallback SELECT also finds nothing, `findOrCreateUrlSeq:861-878` throws rather than returning null. Reaching it requires a committed row for this digest holding a *different* URL — i.e. a digest collision. Under the working tree's SHA-256 that is unreachable; under `253429ec`'s `md5` it is reachable, and cheaply so, by anyone able to choose two URLs the repository will crawl. See finding 5.

---

## 5. `urls.url` uniqueness dropped in v4; duplicate URL rows split version chains — HIGH (FIXED)

**Anchors (historical):** `SQLArtifactIndexDbManagerSql.java:149-150` (v3), `:193` (drop), `:196-197` (replacement); `SQLArtifactIndexManagerSql.java:1108-1138`, `:77-81`, `:299-315`

> Fixed by the `long_urls` redesign — see that section above. The scale was worse than this review could establish from source alone: a production index measured **15,698 duplicate groups across 108M URL rows**.

*What follows describes the code as it stood on 2026-08-04. It is retained so the reasoning can be followed; the outcome is at the end of the section.*

Schema v3 created a unique index on the URL:

```java
  private static final String UNIQUE_URL_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_COLUMN + ")";
```

Schema v4 drops it and replaces it with a plain, non-unique index:

```java
  private static final String DROP_UNIQUE_URL_INDEX_QUERY =
      "DROP INDEX idx1_" + URL_TABLE;

  private static final String URL_INDEX_QUERY =
      "CREATE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_COLUMN + ")";
```

(Assembled: `CREATE UNIQUE INDEX idx1_urls ON urls(url)` becomes `DROP INDEX idx1_urls` + `CREATE INDEX idx1_urls ON urls(url)`.)

`findOrCreateUrlSeq` is a SELECT-then-INSERT with nothing serializing it, and `urls` is global across all AUs, so two AUs crawling the same URL race with no protection at all. Two rows result. The duplicate-key retry at `:1126-1133` is now dead code, because the constraint it was catching no longer exists.

**Failure scenario**

AU-1 and AU-2 both first encounter `http://example.org/p`. Both `findUrlSeq` calls return null; both insert; `urls` now holds seq 10 and seq 11 for the same URL.

- `MAX_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY:299-315` groups by `url_seq`, so the URL is reported twice, with two different "latest" versions.
- `findUrlSeq`'s `LIMIT 1` picks arbitrarily, so version 3 of the URL can attach to seq 10 while versions 1 and 2 sit under seq 11 — the version chain is permanently split.

**Fix — landed (`253429ec`, plus an uncommitted change to the digest)**

The fix direction proposed a partial unique index for short URLs *plus* uniqueness on a digest of the reassembled URL. What shipped is simpler, because the head/tail split went away: `urls.url` holds every URL whole, so one unique index on a digest of the column covers every case, and the partial index is unnecessary.

- `UNIQUE_URL_DIGEST_INDEX_QUERY` (`SQLArtifactIndexDbManagerSql.java:483-485`) creates `idx4_urls` as `CREATE UNIQUE INDEX … ON urls (<digest>)`. It runs after the dedup, so it fails outright if any duplicate survived — which is the intended behaviour.
- The insert path is `INSERT … ON CONFLICT`, as proposed. See finding 4.
- Existing duplicates are collapsed by the migration, not left to be cleaned up later: duplicate groups are found by digest (grouping 108M unbounded `TEXT` values needs a hash table larger than any plausible `work_mem` and degrades into an on-disk sort; a fixed 16-byte key keeps the aggregate in memory), then the survivor within each group is chosen by `min(url_seq) OVER (PARTITION BY url)` — partitioned on the **text**, not the digest, so two genuinely distinct URLs sharing a digest keep separate `url_seq`s rather than being wrongly merged. The digest narrows the candidate set; the text decides.

**The digest choice is the live part of this fix.** `253429ec` uses `md5(url)`; the working tree replaces it with pgcrypto's `digest(url, 'sha256')`, and the reasoning is worth keeping because it is not the usual one. The cost of a collision here is not a slow lookup or a wrong row — every lookup pairs the digest with an exact `url = ?` comparison, so a wrong row is impossible. The cost is that the second of two colliding URLs **can never be stored at all**: `ON CONFLICT DO NOTHING` suppresses the insert, the fallback SELECT correctly refuses to match the other URL, and `findOrCreateUrlSeq` throws. URLs are attacker-supplied, and colliding MD5 inputs are constructible in seconds on ordinary hardware, so under `md5` a publisher could make a chosen URL permanently unindexable. SHA-256 has no known collision.

Two secondary points recorded so nobody re-derives them:

- The spelling is pgcrypto's `digest(text, 'sha256')` rather than the built-in `sha256()`, and that is forced rather than preferred. `sha256()` takes `bytea`, and neither route from `text` works: `sha256(convert_to(url, 'UTF8'))` is rejected at `CREATE INDEX` because `convert_to` is STABLE, not IMMUTABLE; and `sha256(url::bytea)` is an I/O conversion cast, so it *parses* the URL as a bytea literal — `http://host/e\b` fails with "invalid input syntax for type bytea", and a URL beginning `\x` is silently read as hex and hashed as different bytes. Both verified against PostgreSQL 14.
- The migration's dedup step keeps `md5` for grouping even under the SHA-256 constraint, and that combination is deliberate rather than an oversight. Two md5-colliding but distinct URLs survive the dedup by design; had the *constraint* also been md5, `CREATE UNIQUE INDEX` would then have failed on exactly the rows the dedup preserved, aborting the upgrade with a raw "Key (md5(url))=(…) is duplicated". Under SHA-256 the two steps no longer contradict each other.

`TestSQLArtifactIndexUpsertRace.testDigestCollisionSuppressesDistinctUrlAndExactFallbackFindsNothing` pins the collision behaviour without needing a real colliding pair: it substitutes the production digest truncated to zero bytes, which drives the identical `ON CONFLICT DO NOTHING → exact fallback SELECT` control flow with every URL colliding. `TestSQLArtifactIndexDbCollation` asserts the *expression* under `idx4_urls`, not merely that the index exists — a weaker digest would leave an index that exists, passes, and can be collided on demand.

---

## 6. A URL of exactly 2500 chars can collide with a long URL's stored head — MEDIUM (DISSOLVED)

**Anchor (historical):** `SQLArtifactIndexManagerSql.java:77-81` (`FIND_URL_SEQ_QUERY`)

> Dissolved by the `long_urls` redesign — see that section above. There is no head, no tail and no `LONG_URL_THRESHOLD` left to collide on: `urls.url` holds every URL whole, `FIND_URL_SEQ_QUERY:92-96` is a digest term plus an exact `url = ?`, and the unique index makes it a single-row match with no `LIMIT 1`. The seven other read paths this finding listed are covered by the same change.

*What follows describes the code as it stood on 2026-08-04. It is retained so the reasoning can be followed; the outcome is at the end of the section.*

`urls.url` holds either a whole short URL (length ≤ 2500) or the head of exactly 2500 characters of a long URL, whose tail lives in `long_urls`. There is no discriminator column, and — per finding 5 — no uniqueness either.

```sql
SELECT url_seq FROM urls WHERE url = ? LIMIT 1
```

Nothing excludes rows that have a `long_urls` row attached.

**Failure scenario**

1. Store a 3000-character URL `Y`. This creates `urls` row `(url_seq = 7, url = Y[0:2500])` plus a `long_urls` tail row.
2. Store `X`, a URL of exactly 2500 characters where `X == Y[0:2500]`.
3. `findUrlSeq(X)` matches row 7 and returns it. `X`'s artifacts are filed under `Y`'s `url_seq`, and read back with URI `Y`.

The same missing predicate affects every short-URL exact-match read path:

- `GET_LATEST_ARTIFACT_VERSION_QUERY:171`
- `GET_LATEST_ARTIFACT_QUERY:199`
- `GET_ARTIFACT_WITH_VERSION_QUERY:251`
- `GET_COMMITTED_ARTIFACTS_WITH_NAMESPACE_AUID_URL_QUERY:393`
- `MAX_COMMITTED_VERSION_OF_URL_WITH_NAMESPACE_AND_URL_QUERY:452`
- `GET_ARTIFACTS_WITH_NAMESPACE_AND_URL_QUERY:531`
- `GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_URL_QUERY:648`

**Fix direction — moot**

The proposal was `AND NOT EXISTS (SELECT 1 FROM long_urls lu WHERE lu.url_seq = u.url_seq)` on the short-URL match, or an explicit discriminator column on `urls`. Neither is needed: there is no `long_urls` to exclude and nothing to discriminate between.

---

## 7. The keyset tuple is not provably unique — MEDIUM-HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:775-783`, `:738-752`, `:350-378`, `:754-767`, `:2338`; `SQLArtifactIndexDbManagerSql.java:182-187`

`idx7_artifacts` on `(namespace_seq, auid_seq, url_seq, version)` is a plain, non-unique index:

```java
  private static final String ARTIFACT_TUPLE_INDEX_QUERY =
      "CREATE INDEX idx7_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "("
          + NAMESPACE_SEQ_COLUMN + ", "
          + AUID_SEQ_COLUMN + ", "
          + URL_SEQ_COLUMN  + ", "
          + ARTIFACT_VERSION_COLUMN + ")";
```

and `UPSERT_ARTIFACT_QUERY:738-752` conflicts on `uuid` only, so two artifact rows can legitimately share the tuple. No `ORDER BY` in the file carries a unique tiebreaker — `a.uuid` appears in none of them.

```java
  private static final String KEYSET_WHERE_CLAUSE =
      " AND (--SortUriExpr-- > ? OR (--SortUriExpr-- = ? AND " + ARTIFACT_VERSION_COLUMN + " < ?))";
```

**Failure A — silent row loss in pagination**

A page boundary lands on a row whose duplicate was not included in the page. The next page's predicate is `sortUri > ?` OR (`sortUri = ?` AND `version < ?`); the duplicate has the same `sortUri` and the same `version`, so it is excluded by both disjuncts and drops out of the iteration entirely. The 3-column `KEYSET_WHERE_CLAUSE_ALL_AUIDS` variant does not help, because `auid` is equal too.

**Failure B — uncommitted twin leaks through the LATEST queries**

Same root cause. The LATEST queries filter on `committed` only inside the `m` subquery and never on the outer `a`:

- `GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_AUID_QUERY:350-378` can return an uncommitted artifact when `includeUncommitted=false`.
- `GET_SIZE_OF_LATEST_ARTIFACTS_QUERY:754-767` double-counts its length in `AuSize.totalLatestVersions`.
- `getLatestArtifact():2338` can return the uncommitted twin via its `LIMIT 1`.

**Fix direction**

Add a unique constraint on `(namespace_seq, auid_seq, url_seq, version)`, or add `a.uuid` as a final `ORDER BY` column and keyset component. Independently, add `AND a.committed IS TRUE` to the **outer** query of the three LATEST paths above.

---

## 8. Returned `Iterable<Artifact>` is single-use — MEDIUM-HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:2223, 2235, 2249, 2264, 2280, 2295, 2309`

```java
    return IteratorUtils.asIterable(new PagingArtifactIterator(fetcher, pageSize));
```

`IteratorUtils.asIterable` is commons-collections4's `IteratorIterable(iterator, false)` — every call to `iterator()` returns the same wrapped iterator. (`asMultipleUseIterable` is the re-iterable variant.)

**Failure scenario**

A caller does one `for` loop to count and a second to process. The second loop yields zero elements and throws no error — indistinguishable from an empty AU. Two threads sharing the returned object corrupt each other's `pageBuffer` and `lastCursor` (`PagingArtifactIterator.java:91-100`, mutable and unsynchronized).

The same `ArtifactIndex` method is re-iterable on one implementation and single-use on another: `VolatileArtifactIndex.getArtifactsWithPrefix:474-486` returns a materialized collection. `ArtifactIndex.java:227-231` documents the return type as a plain `Iterable`, with no warning.

No in-repo caller currently iterates twice — checked `RepositoryManager.java:753`, `LockssRepositoryStatus.java:507-509`, `BaseCachedUrlSet.java:511/518`, `BaseCachedUrl.java:187`, `DebugPanel.java:583`, `DispatchingArtifactIndex.java:353` — so this is latent rather than live.

**Fix direction**

Use `asMultipleUseIterable`, or return an `Iterable` that constructs a fresh iterator per `iterator()` call, or change the return type to `Iterator`/`Stream` so the contract is explicit.

---

## 9. Mid-iteration DB failure escapes as a bare `RuntimeException` — MEDIUM-HIGH

**Anchor:** `PagingArtifactIterator.java:206-211`

```java
    try {
      fetchNextPage();
    } catch (DbException e) {
      log.error("Database error fetching artifact page", e);
      throw new RuntimeException("Database error fetching artifact page", e);
    }
```

The public `findArtifacts…` methods declare `throws DbException` but cannot actually throw it — they touch no database before returning (`SQLArtifactIndexManagerSql.java:2220-2223`). Consequently `SQLArtifactIndex.java:373-375`'s `catch (DbException e)` → `IOException` translation is unreachable.

**Failure scenario**

The connection drops on page 4 of a walk. The failure surfaces inside the caller's `for` loop as an unchecked `RuntimeException`, so the `catch (IOException)` handlers in `BaseCachedUrl.java:187`, `DebugPanel.java:583` and elsewhere do not catch it, and the error propagates past every layer that was written to handle index failures.

Related: `getArtifactFromCurrentRow:2321` calls `Artifact.setUri(...)`, which throws `IllegalArgumentException` on a null or empty URL (`Artifact.java:198-201`), dying through the same unguarded path.

**Fix direction**

Throw a declared, documented unchecked exception type that callers can reasonably catch, or expose a checked/closeable iterator so the failure is part of the signature.

---

## 10. Paging is not a consistent snapshot, and this is undocumented — MEDIUM

**Anchors:** `SQLArtifactIndexManagerSql.java:1421, 1511, 1588, 1668, 1882, 1999, 2109`

Each `fetchXxxPage` method opens and closes its own connection, so every page is a separate READ COMMITTED transaction. Each one ends the same way — note `safeCloseConnection` rather than the `safeRollbackAndClose` used elsewhere in the file:

```java
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
      DbManager.safeCloseConnection(conn);
```

Writes committed between pages are partially visible. Rows that move before the cursor are missed entirely. For the LATEST queries the `MAX(version)` grouping is re-evaluated on every page, so a version committed mid-walk changes which row represents a URL — the URL can be reported at a stale version, or omitted altogether.

**Failure scenario**

A caller walks an AU while a crawl is committing into it (`BaseCachedUrlSet.java:511`, `RepositoryManager.java:745-763` both do this). A URL whose latest version is committed just after the walk passed its `sortUri` position is either reported at the old version or skipped, with no indication that the listing is inconsistent. `ArtifactIndex.java:217-231` promises a well-ordered listing and states no isolation caveat.

**Fix direction**

Document the caveat on all seven public `findArtifacts…` methods, or hold a single REPEATABLE READ transaction for the whole traversal.

**Two smaller points in the same area**

- The page fetchers use `safeCloseConnection` (no rollback) where the rest of the file uses `safeRollbackAndClose`. With a pooling `DataSource` this can return a connection to the pool with an open read transaction.
- `PagingArtifactIterator.java:45` has a dangling `{@link ArtifactResultSetIterator}` to a class that no longer exists in `src`.

---

## 11. `PreparedStatement` leaks — MEDIUM (PARTIALLY FIXED)

**Anchors:** `SQLArtifactIndexManagerSql.java:3074`, `:3078` and `:3082` after `:3061`, with only the last closed at `:3095` (`deleteArtifact(Connection, String)`). The `addUrl` half is fixed — historical anchors `:1231` and `:1256`.

**`addUrl` — fixed, incidentally.** The leak was one `INSERT_URL_QUERY` statement per URL longer than 2500 characters, and it existed only because the long-URL branch reassigned `ps` to insert the tail. That branch went with `long_urls`. `addUrl:939-988` now prepares exactly one statement and closes it in the `finally`. Nothing needs doing here; the original failure scenario describes code that no longer exists.

**`deleteArtifact(Connection, String)` — unfixed.** The same shape survives untouched:

```java
      if (rows > 0) {
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_NAMESPACE_QUERY);
        ...
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_AUID_QUERY);
        ...
        ps = idxDbManager.prepareStatement(conn, DELETE_ORPHANED_URL_QUERY);
        ...
      }
      ...
    } finally {
      DbManager.safeCloseStatement(ps);
    }
```

`ps` is reassigned without closing the previous statement, and the `finally` closes only the last one.

**Failure scenario**

Three statements leak per delete (the DELETE plus two of the three orphan statements). Leaks are bounded by connection lifetime. Note this is now less severe than when the review was written, because the bulk-load path no longer leaks: the accumulation-across-a-batch argument applied to `addUrl`, which `addArtifacts:2605-2694` calls under one long-lived connection. `deleteArtifact` is called per delete, and deletion is comparatively rare.

**Fix direction**

Use try-with-resources per statement rather than a single reassigned `ps` variable.

> Worth fixing together with finding 2, which is in the same block: today the three orphan statements never delete anything, so the leak is the only live consequence of that code.

---

## 12. ANALYZE-cadence path acquires a second connection while holding the first — MEDIUM

**Anchors:** `SQLArtifactIndexManagerSql.java:971` (`noteUrlRowCreated` inside `addUrl`), `:2804` (`estimatedRowCount`), `:2671` (`maybeAnalyzeUrls` inside `addArtifacts`), `:2681` (the `finally` releasing `conn`), `:2733-2765` (`analyzeTableOutOfBand`)

```java
      // Count this new urls-table row toward the geometric ANALYZE cadence.
      noteUrlRowCreated();
```

```java
  long estimatedRowCount(String table) throws DbException {
    Connection conn = null;
    ...
      conn = getConnection();
```

`addUrl` calls `noteUrlRowCreated()` with the caller's transaction still open; that path reaches `estimatedRowCount`, which takes a second connection. `maybeAnalyzeUrls` at `:2671` likewise runs inside `addArtifacts`' loop while `conn` is held until the `finally` at `:2681`, and `analyzeTableOutOfBand` opens a third.

**Failure scenario**

With a bounded connection pool and N concurrent bulk loaders, every loader holds one connection and blocks waiting for a second — pool-exhaustion deadlock, with no progress possible until a timeout fires.

(`analyzeTableOutOfBand(ARTIFACT_TABLE)` at `:2690` is correctly placed *after* the `finally` and is not part of this problem.)

**Fix direction**

Pass the existing `Connection` down, or defer the cadence bookkeeping until after the caller's transaction is committed and its connection released.

**Same feature, smaller issues**

- `urlTableRowEstimate:2592` is incremented before commit (at `:971`), so a rolled-back batch permanently inflates it. It is never re-seeded — `seedUrlEstimateIfNeeded` returns early once the value is `>= 0` (`:2780-2784`) — so it drifts monotonically upward and can cross `URL_ANALYZE_CAP_ROWS:2586`, permanently disabling the cadence.
- `maybeAnalyzeUrls:2838-2839` advances `urlRowsAtLastAnalyze` *before* calling `analyzeTableOutOfBand`, recording an ANALYZE that may never have run if `pg_try_advisory_xact_lock` failed to acquire.
- `estimatedRowCount:2804` queries `pg_class WHERE relname = ?` with no `relnamespace` filter, so if the same table name exists in two schemas on the search path it reads an arbitrary one.

---

## 13. Substitution tokens are SQL line comments — LOW (latent)

**Anchor:** `SQLArtifactIndexManagerSql.java:775-783`

```java
  private static final String KEYSET_WHERE_CLAUSE =
      " AND (--SortUriExpr-- > ? OR (--SortUriExpr-- = ? AND " + ARTIFACT_VERSION_COLUMN + " < ?))";
```

All four substitution tokens — `--KeysetCondition--`, `--SortUriExpr--`, `--CommittedStatusCondition--`, `--MaxVersionAllUrlsWithNamespaceAndAuid--` — begin with `--`, which is SQL's line-comment marker. Queries are assembled as single-line Java string concatenations with no embedded newlines.

**Failure scenario**

A future edit adds a query variant and misses one `String.replace`. The statement does not fail to parse; instead everything after the token on that line — including `ORDER BY` and the appended `LIMIT ?` — is commented out. The result is a silently unordered, unlimited result set, which pagination then walks incorrectly and which may return the whole table.

All current substitution sites are correctly replaced, re-verified on 2026-08-05: **12** query constants now carry a `--` token, down from 16 because the long-URL query variants were deleted. Still four token kinds, still all `--`-prefixed. This remains a convention hazard rather than a live bug.

**The finding-0 fix adopted the recommended form for its new token, but did not convert the existing four.** `URL_PREFIX_CONDITION_TOKEN:264` is spelled `@@UrlPrefixCondition@@` rather than `--UrlPrefixCondition--`, deliberately and with the reasoning recorded in its javadoc: an unreplaced `@@…@@` is a syntax error, which fails loudly, where an unreplaced `--…--` silently comments out the rest of the statement. So the convention is now split — one safe token and four unsafe ones — which is arguably a worse state to leave than either uniform choice, since the next person to add a token has two precedents to copy.

**Fix direction**

Convert the remaining four to the `@@…@@` form already established in the same file, so the safe spelling is the only precedent. (The original suggestion was `${sortUriExpr}`; matching what is already there is better than introducing a third form.)

---

## 14. Miscellaneous — LOW

All re-verified on 2026-08-05 unless noted. The list is one item shorter than at review time: the `long_urls` unique-constraint bullet is **dissolved**, since the table it described no longer exists — see the redesign section above.

- **`upsertArtifactsForReindex(Iterable):2931`** — the correct single-transaction batch method — is still unused. `SQLArtifactIndex.reindexArtifacts:171-183` loops the single-artifact variant, committing one connection per artifact, so reindex is non-atomic and performs N round-trips. The call site even carries a `// TODO: Implement idxdb.upsertArtifactsForReindex(artifacts)` next to the loop that replaces it.
- **`setPageSize` accepts `Integer.MAX_VALUE`.** `PagingArtifactIterator.java:241` then computes `pageSize + 1` → `Integer.MIN_VALUE`, producing a `NegativeArraySizeException` at the `new ArrayList<>(limit)` in each page fetcher (`:1480`, `:1557`, `:1634`, …) and an invalid `LIMIT`.
- **`pageSize:70` is a non-volatile mutable instance field** — a theoretical visibility race; currently set only from tests.
- **Logging volume.** User-supplied URLs and multi-KB generated SQL are dumped at ERROR level on every failure (`:917/918`, `:976/977`, `:981/982`, `:1392/1395`, `:2403/2406`), unguarded, once per artifact during a bulk load. `addUrl` still logs the same URL and SQL twice, now because its `catch (SQLException)` and `catch (DbException)` blocks are identical (`:974-983`). URLs can now be arbitrarily long rather than capped at 2500 characters, so if anything this got worse.
- **Unused import** `java.lang.ref.Cleaner` at `:47`.
- **PostgreSQL-only constructs** (`COLLATE "C"`, `DISTINCT ON`, `ON CONFLICT`, `pg_try_advisory_xact_lock`, `pg_class`, and now `pgcrypto`'s `digest()`) appear in a class reachable from a nominally multi-dialect `DbManagerSql` that still carries Derby and MySQL branches. Worth an explicit assertion at construction time, or at minimum a class-level doc statement. The v5 migration strengthens the case: it now hard-depends on a PostgreSQL contrib extension.

New in this area since the review, both cosmetic:

- **`COLLISION_TABLE` does not exist.** `SQLArtifactIndexDbManagerSql.java:604` writes `{@value #COLLISION_TABLE}` in the `updateDatabaseFrom4To5` javadoc and `:366` names it in a comment, but the constant is `COLLISION_REPORT_FILE` and the artifact is a CSV file, not a table. The `{@value}` reference will fail javadoc generation, and the surrounding prose ("recorded in …", "The table is created only when…") describes the earlier design rather than what ships.
- **Stale `sortUri` comment.** `SQLArtifactIndexManagerSql.java:770` still documents the computed column as `replace(concat(url, long_url), '/', '\t')`. The `concat` went with `long_urls`; the expression 20 lines below it at `:789-790` is the current one.

---

## 15. The SQL and Volatile ArtifactIndex implementations disagree on result ordering — MEDIUM

**Anchors:** `ArtifactIndex.java:218`; `lockss-util-rest/.../util/ArtifactComparators.java:42-43, 57-66`; `VolatileArtifactIndex:372, 393`; `SQLArtifactIndexManagerSql.java:146-152, 668-677, 775-783`

This is one contract divergence with three parts, not three unrelated bugs. `ArtifactIndex.java:218` documents the ordering contract as `PreOrderComparator`. `VolatileArtifactIndex` implements it literally:

```java
  public static final Comparator<Artifact> BY_URI =
      Comparator.comparing(Artifact::getUri, PreOrderComparator.INSTANCE);      
```

The SQL implementation only approximates it. The three divergences below are ranked by reachability.

### 15a — `getNamespaces()` and `getAuIds()` are not ordered at all on the SQL side

Most reachable of the three; it happens on every call.

`VolatileArtifactIndex:372` and `:393` return both through `.sorted()`, i.e. natural `String` order. `GET_NAMESPACES_QUERY:146-152` and `GET_AUIDS_BY_NAMESPACE_QUERY:668-677` have no `ORDER BY` at all:

```sql
SELECT DISTINCT ns.namespace
  FROM namespaces ns
 WHERE EXISTS ( ... )
```

With `DISTINCT` and no `ORDER BY`, PostgreSQL returns rows in whatever order the chosen plan or hash grouping produces. This is not a collation disagreement — one implementation sorts and the other does not sort at all.

Cross-reference finding 1: on the SQL side `getNamespaces()` also returns the wrong *set*, so both the membership and the ordering of its result are wrong.

**Fix:** add an `ORDER BY` to both queries.

### 15b — the AUID tiebreaker uses two different collations, reachable with plain ASCII (FIXED)

The full orderings are `BY_URI_BY_AUID_BY_DECREASING_VERSION` and `BY_URI_BY_DATE_BY_AUID_BY_DECREASING_VERSION` (`ArtifactComparators.java:57-66`):

```java
  public static final Comparator<Artifact> BY_URI_BY_AUID_BY_DECREASING_VERSION =
      Comparator.comparing(Artifact::getUri, PreOrderComparator.INSTANCE)
                .thenComparing(Artifact::getAuid)
                .thenComparing(Comparator.comparingInt(Artifact::getVersion).reversed());
```

The second key is a bare `.thenComparing(Artifact::getAuid)` — plain natural `String` order, i.e. UTF-16 binary. The SQL side orders by `auid.auid ASC` with **no `COLLATE`**, so it uses the database default: `en_US.UTF-8` in the deployed containers.

This needs no exotic input. AUIDs are punctuation-dense, e.g.

```
org|lockss|plugin|foo|FooPlugin&base_url~http%3A%2F%2Fexample%2Ecom%2F&year~2020
```

glibc's `en_US.UTF-8` deweights punctuation at the primary level, comparing letters first and only consulting punctuation at a later level. Java compares `|` (0x7C) against `b` (0x62) directly. Two AUIDs differing only in where their punctuation falls therefore order differently on the two implementations, using nothing but ASCII.

It is only observable when two artifacts share a URI, since AUID is the secondary key — which is exactly the case the all-AUIDs queries serve. Note also that `auid` is the one remaining column exposed to the glibc-collation-version hazard described in finding 0's design note: a base-image glibc bump can silently reorder it.

**Fix — landed.** Two parts: the v5 migration declares `auids.auid` as `COLLATE "C"` (see finding 0's design note), and a new `SORT_AUID_EXPR` constant in `SQLArtifactIndexManagerSql` is applied at all 8 `ORDER BY ... auid ASC` sites and at both `auid` terms in `KEYSET_WHERE_CLAUSE_ALL_AUIDS`.

It is a shared constant rather than 8 inline edits on purpose: that structurally guarantees the sort and the keyset comparison cannot drift into different collations, which is the failure mode that silently skips or repeats rows at page boundaries. No paging expectations shifted, as expected — the Java side already used natural `String` order, which is exactly what C collation matches.

### 15c — the URI comparison itself diverges in three narrow places (latent)

`PreOrderComparator.preOrderCompareTo` compares UTF-16 `char` values with `/` forced first, then falls back to a length difference:

```java
  public static int preOrderCompareTo(String str1, String str2) {
    int len1 = str1.length();
    int len2 = str2.length();
    int n = Math.min(len1, len2);

    for (int ix = 0 ; ix < n ; ++ix) {
      char c1 = str1.charAt(ix);
      char c2 = str2.charAt(ix);
      if (c1 != c2) {
        if (c1 == '/') {
          return -1;
        }
        if (c2 == '/') {
          return 1;
        }
        return c1 - c2;
      }
    }
    return len1 - len2;
  }
```

The SQL implements it as a substitution plus a byte comparison:

```java
  private static final String SORT_URI_EXPR_LONG_URL =
      "replace(concat(u." + URL_COLUMN + ", " + LONG_URL_COLUMN + "), '/', '\u0009') COLLATE \"C\"";
```

That is: map `/` (0x2F) to TAB (0x09), then compare UTF-8 bytes — which is code point order. The two disagree in exactly three places:

- **Astral characters versus `U+E000`–`U+FFFF` only.** In UTF-16 an astral code point is a surrogate pair beginning in `0xD800`–`0xDBFF`, so Java sorts it *below* `U+E000`–`U+FFFF`; in code point order it is `0x10000`+ and sorts *above*. Astral-vs-astral agrees (surrogate pairs are monotonic in code point) and astral vs. anything below `U+D800` agrees, so the inversion is confined to that window.
- **`0x00`–`0x08` invert against `/`.** SQL places them before the substituted `/` (which became `0x09`); `PreOrderComparator` places `/` first unconditionally.
- **`0x09` (TAB) collides with `/`.** Both map to `0x09`, so SQL calls them equal, where the comparator puts `/` first.

Everything from `0x0A` upward agrees.

Checked and found equivalent, so nobody re-derives it: the length tiebreaker agrees (a shorter string that is a prefix of a longer one sorts first under both); `replace()` replaces all occurrences, matching the comparator's per-position special case; and all BMP non-surrogate characters agree, because UTF-16 code-unit order, code point order and UTF-8 byte order coincide there.

Practical exposure is near zero for well-formed URLs, which are ASCII or percent-encoded. The value of recording it is that it is the same defect class as finding 0: two implementations of one documented contract that quietly disagree.

### Note — what the v5 migration did and did not resolve

Of finding 15's three parts, the v5 migration resolved **15b only** — and that remains true now that v5 carries the `long_urls` fold as well as the `COLLATE "C"` change. Neither half of v5 touches 15a or 15c.

It does **not** resolve 15c. The `sortUri` expression already carried an explicit `COLLATE "C"`, so a column-level declaration changes nothing for it: 15c's three divergences come from the `replace('/' → TAB)` substitution and from UTF-16 versus code point order, both of which are collation-independent.

Current state of this finding: **15a unfixed** (needs `ORDER BY` on the two queries), **15b fixed**, **15c unfixed**. 15c is now confirmed rather than predicted: `SORT_URI_EXPR:789-790` is `replace(u.url, '/', E'\t') COLLATE "C"` — the `long_urls` removal has happened, and every divergence 15c describes survived it, exactly as this section said it would.

---

## Verified clean

Checked during this review and found correct — recorded so nobody re-investigates them.

**Obsoleted by the `long_urls` removal.** These four described code that no longer exists. Kept only so it is clear they were checked and then deleted, rather than checked and still standing:

- ~~**Long-URL reassembly.** No query selects a bare `u.url` alongside a `LEFT JOIN long_urls`.~~
- ~~**Unqualified `long_url` in the sortUri expressions** resolves unambiguously at all sites.~~
- ~~**The 2500-character split boundary** is identical on read and write across all 14 sites, with no off-by-one.~~
- ~~**Placeholder/parameter alignment** across 20 (later 24) assembled query variants.~~ The long/short dimension is gone, so the variant count is halved and the alignment question is correspondingly smaller. **Not re-verified against the current assemblies** — the fetch methods were rewritten by `253429ec`.

Still standing, re-verified on 2026-08-05:

- **Keyset and `ORDER BY` tuples** agree in column set, order and direction. `ALL_AUIDS_CURSOR_EXTRACTOR` is used for exactly the two 3-column-ordered queries.
- **`COLLATE "C"` placement** is identical in the sortUri select expression and the injected keyset expression, and the Java-side cursor `getUri().replace("/", "\t")` matches the SQL `replace(url, '/', E'\t')`. Both sides simplified when `concat()` went away, and they simplified in step. This remains a *structural fragility*, and arguably a sharper one now that the expression looks trivial enough to edit casually: two independently-maintained expressions must stay byte-identical forever, no test asserts it, and divergence breaks pagination silently. Recommend reading `rs.getString("sortUri")` back instead of recomputing it in Java.
- **`GROUP BY` covers the non-aggregate SELECT list** in all six MAX-version subqueries.
- **`addArtifact` is genuinely atomic** (`:2883`), with LRU publication correctly placed after the commit. `addArtifacts` cache coherency, via `pending_*` maps published only after commit, is also correct.
- **Iterator termination and page arithmetic** (fetch `pageSize+1`, `size() <= pageSize` means last page) are correct, with no extra empty fetch at the end.
- **No method ignores `includeUncommitted`.**
- **`analyzeTableOutOfBand` itself is correctly written**: try-with-resources, dedicated connection, best-effort semantics, transaction-scoped advisory lock.
- **No identifier collides with a PostgreSQL reserved word.**
- **No accidental `LEFT JOIN` → `INNER JOIN` degradation** from a `WHERE` predicate on the outer side — and largely moot now, since the `long_urls` outer joins were the ones at risk.

## Test coverage gaps

Closed since the review:

- Finding 5 is now covered from both ends: `TestSQLArtifactIndexDbCollation` asserts that `idx4_urls` is UNIQUE *and* that its expression is a SHA-256 digest — pinning the expression rather than the index name, because a weaker digest would leave an index that exists, passes, and can be collided on demand. `TestSQLArtifactIndexUpsertRace` covers the concurrent-creator race the missing constraint allowed.
- Finding 4 is covered by `TestSQLArtifactIndexUpsertRace`, which pins the `ON CONFLICT` semantics the fix depends on against a real PostgreSQL rather than against the documentation: `DO NOTHING` returns no row only after a *committed* winner, `DO UPDATE` always returns one, and a digest collision degrades to a refused URL rather than a wrong one.
- Finding 6's gap is moot — there is no 2500-character boundary left to test.

Still open:

- `TestSQLArtifactIndexDbManager.java:515-520` cannot catch finding 1 — it only asserts the empty → one-namespace transition, which passes with the broken predicate.
- No test covers the orphan-delete queries (finding 2).
- No test covers duplicate-tuple pagination (finding 7).
- No test covers double iteration of a returned `Iterable` (finding 8).
- Nothing tests that the SQL and Volatile implementations return the **same order** for the same corpus. A shared cross-implementation ordering test would have caught all of 15a, 15b and 15c.
- `TestSQLArtifactIndexManagerSqlPrefixWildcards` and `TestSQLArtifactIndexManagerSqlPrefixUpperBound` cover finding 0 only.
- Nothing exercises the prefix path against the **rebuilt** `idx1_urls`: specifically, prefixes longer than `URL_PREFIX_INDEX_LENGTH` (600), where the index range collapses to equality and `urlPrefixCondition` adds the exact recheck. That branch is the one place the finding-0 fix and the `long_urls` fold interact, and it is the newest code in either.
