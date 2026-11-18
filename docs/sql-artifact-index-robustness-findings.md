# Bulk artifact indexing — failure blast-radius audit

**Date:** 2026-08-04

**Scope:** the bulk index-insert path and everything it can fail through —
`SQLArtifactIndexManagerSql.addArtifacts()` and its helpers,
`SQLArtifactIndexManagerSql.upsertArtifactsForReindex()`,
`SQLArtifactIndex.indexArtifacts()` / `reindexArtifacts()` / `auSize()`,
and the callers that drive them.

**Question asked:** two incidents — a failed out-of-band `ANALYZE`, and a single NPE
on a URL sequence lookup — each destroyed a whole batch of `indexArtifacts()` and
every batch after it. Both root causes are fixed. Where else can one small error
take out far more than one item?

This is a companion to `sql-artifact-index-review-findings.md`, which covers
correctness. This document covers **blast radius only**: not "is it wrong" but
"when it goes wrong, how much do we lose". Nothing here is fixed.

---

## Summary

Ranked by blast radius. "Cost" is what one small error destroys today.

| # | Finding | Cost of one error | Severity | Cheap? |
|---|---|---|---|---|
| **C1** | `finishBulkStore` failure drops the AU's volatile index, unrecoverably | **the whole AU, permanently** | CRITICAL | yes |
| R1 | Best-effort `ANALYZE`/row-estimate calls catch only checked exceptions | one batch + the remainder | HIGH | yes |
| C3 | One malformed WARC record aborts the rest of the file | up to 999 parsed artifacts + rest of WARC | HIGH | yes |
| R7 | Reindex paths have no per-item isolation; `invalidateAuSize` rethrows | whole AU reindex / the remainder | HIGH | partly |
| R2 | Trailing `ANALYZE` fails an AU whose rows are all committed | nothing real — a false failure | HIGH | yes |
| R3 | Null `version` NPEs during bind; catch is `SQLException` only | one batch + the remainder | HIGH | yes |
| R4 | Null URL / per-URL server error poisons the transaction | one batch + the remainder | HIGH | half |
| R5 | Batch flush failure discards the whole 1000-row batch | one batch + the remainder | HIGH | no |
| R6 | No good-prefix commit — but a naive one silently loses data | up to 999 bound artifacts | HIGH | no |
| C2 | Input `Iterable` is single-use; a second pass silently yields nothing | blocks replay designs | HIGH | n/a |
| C4 | The "retried `finishBulkStore`" the comments assume does not exist | no recovery path at all | HIGH | n/a |
| R8 | `RuntimeException` in AU-size recompute never completes the future | a permanently hung thread | MED-HIGH | yes |
| C5 | Partial success leaves AU sizes stale with no invalidation path | stale cache, no recovery | MEDIUM | with R6 |
| C6 | Solr silently swallows lost batches; SQL throws | divergent failure semantics | MEDIUM | n/a |
| R9 | Out-of-band `ANALYZE` takes a 2nd connection while holding one | a stalled load | MEDIUM | no |
| R10 | `clearAllArtifacts` LRU clear races concurrent loads → FK violations | unbounded, process-lifetime | MEDIUM | yes |
| R11 | Latent `Long`→`long` unboxing sites | none today | LOW | yes |
| R12 | `noteUrlRowCreated` counts rows that may roll back | slightly-off `ANALYZE` cadence | LOW | — |

C7 (the import path) is a reference, not a defect: it already does per-item
isolation correctly and is the model to copy.

## The two mechanisms

Almost every finding below is one of these two, so they're worth stating once.

### M1 — one `SQLException` poisons the connection for the rest of the transaction

PostgreSQL aborts the entire transaction on any server-side error. Every
subsequent statement on that connection fails with `current transaction is
aborted, commands ignored until end of transaction block` until someone issues
`ROLLBACK`. This is the "…and every batch after it also failed" half of both
incidents.

The consequence for design: **"catch the exception and continue" is not
implementable by adding a `try`/`catch`.** Any handler that wants to continue
after a server-side error must first roll back, and must then treat everything
that was bound-but-uncommitted as lost.

The corollary is the useful part: an error that *never reaches the server* — a
Java-side NPE while binding parameters, a null field, an unboxing failure —
leaves the transaction perfectly healthy. Those are the cases where
skip-and-continue costs nothing, and they are the majority of the findings below.

### M2 — a failed commit closes the connection out from under the caller

`DbManager.commitOrRollback` → `JdbcBridge.commitOrRollback`
(`lockss-core/src/main/java/org/lockss/db/JdbcBridge.java:765-778`) calls
`safeRollbackAndClose(conn)` before rethrowing. `DbManager.commitOrRollback`
(`DbManager.java:498-512`) does it again on its own catch.

So if the per-batch commit at `SQLArtifactIndexManagerSql.java:2641` fails, `conn`
is **closed**, and the `PreparedStatement ps` hoisted outside the loop at 2603 is
prepared on a dead connection. Any continue-after-failure design must obtain a
fresh connection and re-prepare, not merely roll back. This is a structural
constraint on the fix, not a bug in itself.

---

## Before designing anything: decide where skipped items are reported

"Lose one item rather than the batch" is only safe if we know *which* item was
lost. A skipped artifact is a silent divergence between the data store and the
index — the WARC record exists, nothing points at it, and nothing will ever
notice.

`addArtifacts` currently returns `Set<Pair<String,String>>` (the touched
namespace/AUID pairs). There is no channel for "these N artifacts were skipped".
The options, in increasing order of usefulness:

1. `log.warn` per skipped artifact and nothing else. Weakest acceptable answer.
2. Return the skipped artifacts (or their UUIDs) alongside `nsAuids`, so
   `SQLArtifactIndex.indexArtifacts` and `finishBulkStore` can decide.
3. Count failures per AU and mark the AU as needing a reindex.

**This choice shapes the signature and every call site, so it should be made
before any code changes.** It is the one open question in this document that I
can't answer from the code.

---

## Findings, ranked by blast radius

### R1 — best-effort maintenance calls in the hot path catch only checked exceptions — HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:2736` (`analyzeTableOutOfBand`),
`:2768` (`seedUrlEstimateIfNeeded`), `:2797` (`estimatedRowCount`)

This is the *same bug that was just fixed*, incompletely. Moving `ANALYZE`
out-of-band removed the failure's ability to abort the load transaction, but the
handler that makes it "best effort" is a checked-exception list:

```java
    } catch (SQLException | DbException e) {
      log.warn("Out-of-band ANALYZE {} failed (ignored): {}", table, e.getMessage());
    } finally {
```

A `RuntimeException` walks straight through — a driver-level
`IllegalStateException`, a connection-pool `IllegalStateException` on a shut-down
pool, an NPE, a `SQLTimeoutException` subclass that arrives wrapped. It
propagates out of `maybeAnalyzeUrls()` at `:2825`, out of the flush block at
`:2651`, past the `finally` at 2661 that rolls the batch back, and out of
`addArtifacts` entirely. Everything uncommitted in that batch is gone and no
subsequent artifact in the iterable is attempted.

The same applies one level down. `noteUrlRowCreated()` (`:2750`) is called from
`addUrl()` **for every new URL**, and on the first call it does a real database
round trip via `seedUrlEstimateIfNeeded()` → `estimatedRowCount()` →
`getConnection()`. `estimatedRowCount` catches `SQLException`;
`seedUrlEstimateIfNeeded` catches the resulting `DbException`. Neither catches
`RuntimeException`. A statistics-gathering side effect can therefore kill an
artifact insert.

**Failure scenario.** The connection pool is exhausted or shutting down while a
bulk load runs. `getConnection()` inside `analyzeTableOutOfBand` throws an
unchecked pool exception rather than a `DbException`. A batch of up to 1000
artifacts is discarded and the remaining artifacts for the AU are never
attempted, because a *statistics refresh* failed.

**Fix direction.** Every best-effort call reachable from the insert loop should
catch `Exception` (or `Throwable` minus `Error`), not a checked list. These are
by construction non-essential: `ANALYZE` only affects planning, and the row
estimate only affects `ANALYZE` cadence. Nothing they do can justify losing data.
This is a two-line change and the highest value-per-line item in this document.

### R2 — the trailing `ANALYZE` can fail an AU whose rows are all committed — HIGH (cheap)

**Anchor:** `SQLArtifactIndexManagerSql.java:2666-2671`

```java
    } finally {
      DbManager.safeCloseStatement(ps);
      DbManager.safeRollbackAndClose(conn);
    }

    // Refresh artifact-table stats once, after all rows are committed [...]
    if (count > 0) {
      analyzeTableOutOfBand(ARTIFACT_TABLE);
    }

    return nsAuids;
```

This sits *after* the `finally`, after every artifact is durably committed and
the connection is closed. If it throws — which per R1 it can, via
`RuntimeException` — `addArtifacts` throws. `SQLArtifactIndex.indexArtifacts`
(`SQLArtifactIndex.java:152`) wraps it in `IOException`. `finishBulkStore` fails.
The AU is reported as failed even though **100% of its artifacts are in the
database**.

This is a pure false negative: maximum blast radius, zero actual data loss. The
retry is idempotent (`UPSERT_ARTIFACT_QUERY`, `:728`), so the cost is redundant
work plus an alarming error — but the AU is also no longer in bulk-store mode
(see the caller-side section), so the retry may not be straightforward.

Fixed by R1's catch-widening. Worth calling out separately because it is the one
case where the loss is *entirely* illusory.

### R3 — a null `version` on one artifact kills the batch — HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:2879-2893` (`bindArtifactInsertParams`),
`:2939` (the reindex equivalent)

```java
    ps.setInt(5, artifactId.getVersion());
```

`ArtifactIdentifier.getVersion()` returns `Integer`
(`lockss-util/lockss-util-rest/src/main/java/org/lockss/util/rest/repo/model/ArtifactIdentifier.java:113`),
and `setInt` takes `int`. One artifact with a null version is an **NPE** —
structurally identical to the `url_seq` NPE incident.

The NPE is thrown inside this block:

```java
        try {
          bindArtifactInsertParams(ps, namespaceSeq, auidSeq, urlSeq, artifact);
          ps.addBatch();
          count++;
        } catch (SQLException e) {
          throw new DbException("Error binding artifact INSERT parameters", e);
        }
```

The catch is `SQLException` only, so the NPE escapes uncaught, and the whole
call dies.

**This is the single best skip-and-continue candidate in the file.** By M1, a
bind failure never reaches the server, so the transaction is untouched and
perfectly healthy. Catching `RuntimeException` here, logging the artifact, and
`continue`-ing loses exactly one artifact and nothing else. No rollback, no
reconnect, no replay. It is the cheapest possible fix for the exact failure
shape the incidents took.

Two details for whoever implements it:

- **Call `ps.clearParameters()` on the skip path.** A partial bind leaves the
  previous artifact's values in the positions it didn't reach. That is harmless
  today only because `addBatch()` never ran; clearing keeps it harmless if the
  order ever changes.
- **Populate `pending_*_seqs` before the bind attempt, not after.** They are set
  at `:2631-2633`, *after* the `try`. A skipped artifact's namespace/auid/url rows
  were already created and will commit, but their sequence numbers never reach the
  LRU — so the next artifact with the same URL pays the round trip again. Moving
  the `put`s above the bind is safe (they are published only post-commit, per T2)
  and avoids the waste. Note the skipped artifact does leave an orphan `urls` row,
  which per finding 2 of the correctness review is never collected.

Note also that `Pair.of(artifact.getNamespace(), artifact.getAuid())` at `:2606`
and the three `findOrCreate*Seq` calls at `:2608-2621` are **outside any `try`
at all**. A null namespace/auid/uri reaching this loop fails there instead, with
the same total blast radius but a different remedy (see R4).

### R4 — a null or oversized URL on one artifact kills the batch, *and* poisons the connection — HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:2618-2621`, `:919-968` (`addUrl`)

`findOrCreateUrlSeq` → `addUrl` → `ps.setString(1, url)` with a null `url` binds
SQL NULL, and `urls.url` is `NOT NULL`. That is a **server-side** error, so
unlike R3 it lands squarely in M1: the transaction is aborted, everything bound
so far in the batch is unrecoverable, and the connection cannot be reused
without a rollback.

`Artifact.setUri` rejects null/empty
(`Artifact.java:194-200`), so this requires an artifact constructed by a path
that bypasses the setter. That makes it less likely than R3 — but "less likely"
is what both incidents were, too.

The same shape applies to any per-URL server-side failure: a URL that violates a
constraint, a deadlock on the `urls` unique index against a concurrent loader, a
statement timeout. All of them are one-artifact problems that cost a whole batch.

**Fix direction.** Unlike R3 this genuinely requires the rollback-and-replay
machinery in R5. A cheap partial mitigation available immediately: validate
`getUri()`, `getNamespace()`, `getAuid()`, and `getVersion()` for null at the top
of the loop and skip the artifact **before** touching the database, converting
this class of failure into an R3-style free skip.

### R5 — a batch flush failure discards up to 1000 successfully-bound artifacts — HIGH (structural)

**Anchors:** `SQLArtifactIndexManagerSql.java:2635-2652`, `:2685-2692`, `:2661-2664`

```java
  private void flushArtifactBatch(PreparedStatement ps) throws DbException {
    try {
      ps.executeBatch();
      ps.clearBatch();
    } catch (SQLException e) {
      throw new DbException("Error executing artifact INSERT batch", e);
    }
  }
```

One bad row in `executeBatch()` throws `BatchUpdateException`, the transaction is
aborted (M1), and the `finally` at `:2661` rolls back and closes. Up to 1000
artifacts — 999 of them fine — are discarded, and every remaining artifact in
the iterable is abandoned.

**Fix direction — rollback and replay, not savepoints.** On a flush failure:
roll back, obtain a fresh connection and re-prepare (M2 means the old one may be
closed), then re-drive that batch's artifacts individually in their own
transactions, skipping the ones that actually fail, and continue with subsequent
batches. The cost is paid only on the failure path; the happy path stays a single
1000-row `executeBatch`.

Do **not** reach for a savepoint per item. PostgreSQL degrades sharply past a
few dozen subtransactions per transaction (`pg_subtrans` SLRU pressure); a
1000-item batch would sit well past any reasonable threshold. If savepoints are
wanted, scope them to sub-batches and measure the threshold rather than assuming
one.

Don't over-invest in `BatchUpdateException.getUpdateCounts()`. With autocommit
off the transaction is already aborted, so the counts tell you where it stopped
but give you nothing to salvage in place — you still need the replay.

### R6 — commit the good prefix, but only for Java-side failures — HIGH (not cheap)

**Anchor:** `SQLArtifactIndexManagerSql.java:2661-2664`

```java
    } finally {
      DbManager.safeCloseStatement(ps);
      DbManager.safeRollbackAndClose(conn);
    }
```

Every failure path discards the current partial batch, including the artifacts
bound before the failure. For a failure at item 999 of 1000 that is 999 avoidable
losses.

This matters most for failures we *cannot* skip past — a `RuntimeException`
thrown from the `Iterable` itself, mid-iteration. `PagingArtifactIterator.hasNext`
turns a `DbException` into a bare `RuntimeException`
(`PagingArtifactIterator.java:206-211`); the volatile-index iterable can throw
`ConcurrentModificationException` mid-iteration (see C2); and upstream a
malformed WARC record does the same. There is no "skip this element" for an
iterator that has failed; the only available mitigation is to keep what we
already have.

**Fix direction, and the trap in it.** Before propagating, flush and commit what
is already bound, then rethrow.

**This works only for Java-side failures.** For any M1-class failure it is worse
than useless. The comment at `:2696-2701` states the mechanism precisely: after a
server-side error the transaction is aborted, and *a subsequent commit is
silently turned into a rollback by PostgreSQL — discarding every insert in the
batch with no exception thrown*. A good-prefix commit added naively would appear
to preserve the batch, preserve nothing, and report success. That is a worse
failure than the one it replaces.

So R6 applies to exactly the failures that never reached the server: R3's bind
NPE, C2's `ConcurrentModificationException`, `PagingArtifactIterator`'s wrapped
`DbException` (which is thrown on a *different* connection, inside the page
fetcher, so this connection is healthy). It must be guarded by an explicit
"has a server-side error occurred in this transaction?" flag.

That flag is the same bookkeeping R5 needs, which is why R6 is **not** the cheap
standalone win it first appears to be. Land it with R5, not before.

### R7 — both reindex paths are strictly worse than `addArtifacts` — HIGH

**Anchors:** `SQLArtifactIndexManagerSql.java:2911-2926`, `SQLArtifactIndex.java:170-198`

`upsertArtifactsForReindex` is a **single transaction over the entire iterable**
with no batching and no isolation whatsoever:

```java
      for (Artifact artifact : artifacts) {
        upsertArtifactForReindex(conn, artifact);
      }

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
```

One bad artifact — including the null-version NPE of R3, since `:2939` has the
identical `ps.setInt(5, artifactId.getVersion())` — loses **the entire AU's
reindex**, however large. It has none of the per-batch commits that bound
`addArtifacts`' damage. If reindex is ever driven over a large AU this is the
worst blast radius in the subsystem.

`SQLArtifactIndex.reindexArtifacts` is the path actually in use, and it is
better but still wrong. It loops calling the single-artifact
`upsertArtifactForReindex(artifact)`, each with its own connection and commit, so
committed work survives — but the first failure abandons every remaining
artifact.

It also has an inversion worth fixing on sight:

```java
        try {
          invalidateAuSize(firstArtifact.getNamespace(), firstArtifact.getAuid());
        } catch (DbException e) {
          log.warn("Could not invalidate AU size", e);
          throw e;
        }
```

It logs the failure as a warning and then **rethrows it**, failing the whole
reindex. `indexArtifacts` handles the identical call correctly at
`SQLArtifactIndex.java:144-151` — warn and carry on. A stale AU-size cache entry
is a cosmetic problem; it should never fail a reindex. Delete the `throw e`.

**Fix direction.** Give both reindex paths the same per-item catch-and-continue
as `addArtifacts`. The `SQLArtifactIndex.reindexArtifacts` loop is nearly free to
fix — each iteration is already its own transaction, so a `try`/`catch`/`continue`
around the body is correct as written, with no M1 concern at all.

### R8 — a `RuntimeException` in AU-size recomputation hangs every waiting thread — MEDIUM-HIGH

**Anchor:** `SQLArtifactIndex.java:496-527` (`getAuSizeFuture`)

```java
    try {
      AuSize result = computeAuSize(namespace, auid);
      updateAuSize(namespace, auid, result);
      ausFuture.complete(result);
    } catch (DbException e) {
      ...
      ausFuture.completeExceptionally(...);
    } catch (IOException e) {
      ...
      ausFuture.completeExceptionally(e);
    } finally {
      synchronized (auSizeFutures) {
        auSizeFutures.remove(nsAuid);
      }
    }
```

If `computeAuSize` or `updateAuSize` throws a `RuntimeException`, neither catch
fires, so `ausFuture` is **never completed**. The `finally` removes it from the
map, so nothing will ever complete it. Any thread that already read that future
out of the map and is blocked in `auSize()` on `ausFuture.get()`
(`SQLArtifactIndex.java:481`) waits **forever** — an unbounded, uninterruptible
block, not a failure.

The window is narrow (a concurrent `auSize` call between the `put` at `:509` and
the `remove` in the `finally`) but the consequence is a hung thread rather than a
lost item, which makes it worse than most of what's above it.

Adjacent, same method's caller:

```java
    } catch (ExecutionException e) {
      log.error("Could not recompute AU size", e.getCause());
      throw (IOException) e.getCause();
```

An unchecked cast. If the cause is ever anything but `IOException` this throws
`ClassCastException` and discards the real error.

**Fix direction.** Add `catch (RuntimeException e) { ausFuture.completeExceptionally(e); }`,
or better, complete exceptionally from the `finally` if the future is still
incomplete. Replace the cast with an instanceof check.

### R9 — `analyzeTableOutOfBand` takes a second connection while the caller holds one — MEDIUM

**Anchor:** `SQLArtifactIndexManagerSql.java:2713-2716`, called from `:2651` mid-load

Already recorded as finding 12 in the correctness review. The robustness angle is
different from the correctness one: under pool pressure `getConnection()` may
**block** rather than throw, so a load holding one connection can stall waiting
for a second one it only needs for a statistics refresh. R1's catch-widening
covers the throw case; it does not cover the stall.

**Fix direction.** Bound the wait, or skip the refresh entirely when the pool is
under pressure. A statistics refresh should never be able to stall a load.

### R10 — `clearAllArtifacts` and the LRU caches — MEDIUM

**Anchor:** `SQLArtifactIndexManagerSql.java:3079-3099`

```java
      DbManager.commitOrRollback(conn, log);
      lru_namespace_seqs.clear();
      lru_auids_seqs.clear();
      lru_urls_seqs.clear();
```

The clear is correctly *after* the commit, but it is not atomic with it. A
concurrent `addArtifacts` running on another thread can repopulate the LRUs with
pre-delete sequence numbers in the gap, or can already be holding one in a local
variable.

`artifacts` has real foreign keys to `urls`, `namespaces` and `auids`
(`SQLArtifactIndexDbManagerSql.java:82,92,94,96`), so a stale sequence number is
not a silent wrong answer — it is a **foreign-key violation on every subsequent
insert that uses it**, for as long as the entry survives in the LRU. For
`lru_namespace_seqs`, which is an unbounded `HashMap` with no eviction (`:974`),
that is the life of the process.

Low likelihood — `clearAllArtifacts` concurrent with a bulk load is not a normal
operation — but the failure mode is unbounded, so it is worth a guard.

### R11 — latent unboxing sites — LOW

**Anchors:** `SQLArtifactIndexManagerSql.java:2533-2535`, `:2929-2931`

```java
      long namespaceSeq = findOrCreateNamespaceSeq(conn, artifact.getNamespace());
      long auidSeq = findOrCreateAuidSeq(conn, artifact.getAuid());
      long urlSeq = findOrCreateUrlSeq(conn, artifact.getUri());
```

These assign `Long` to `long`. This is the exact shape of the NPE incident.

**They are currently safe.** `findOrCreateUrlSeq` (`:824-863`) now either returns
non-null or throws `DbException`; `addNamespace` (`:1057-1063`) and `addAuid`
(`:1246-1252`) both throw rather than return null on an empty result set. So
this is latent, not live — recorded so nobody chases it as an open bug, and so
that a future nullable return re-breaks loudly rather than silently. Declaring
these `Long` and null-checking explicitly would make the guarantee local instead
of remote.

### R12 — `noteUrlRowCreated` counts rows that may be rolled back — LOW

**Anchor:** `SQLArtifactIndexManagerSql.java:949-951`

`urlTableRowEstimate` is incremented when the `INSERT ... RETURNING` produces a
row, which is before the batch commits. A rolled-back batch leaves the estimate
permanently high. The only consequence is a slightly early or skipped `ANALYZE`,
which `shouldAnalyzeUrls` is already designed to tolerate. Not worth fixing;
recorded so it isn't rediscovered as a leak.

---

## Three things that only break *after* the robustness work lands

These are not bugs today. They are traps waiting for the first `continue`
statement, and each one is easy to miss.

### T1 — `ps.clearBatch()` is inside the `try`, after `executeBatch()`

`SQLArtifactIndexManagerSql.java:2687-2688`. On failure it is skipped. Harmless
now because the statement is closed immediately afterward. The moment a failure
path continues the loop, the stale batch entries are still queued and get
re-executed on the next flush — silently duplicating or re-failing them. **Move
`clearBatch()` into a `finally`** as part of any change here.

### T2 — the pending seq maps must be cleared on rollback, and never published

`SQLArtifactIndexManagerSql.java:2595-2597, 2642-2645, 2677-2683`

`pending_ns_seqs` / `pending_auid_seqs` / `pending_url_seqs` are published to the
shared LRUs only after a successful commit — correct as written, and the comment
at `:2592-2594` says exactly why. If any new failure path lets a rolled-back
sequence number reach `publishSeqLrus`, those sequences **no longer exist in the
database**, and by R10's foreign keys every subsequent insert that hits the
cached entry takes an FK violation. For `lru_namespace_seqs` that is
process-lifetime.

This is the most dangerous single mistake available in this refactor. Any
rollback path must clear the pending maps without publishing them.

### T3 — `count` stops tracking "rows pending in `ps`"

`SQLArtifactIndexManagerSql.java:2626, 2635, 2656`

`count % ARTIFACT_INSERT_BATCH_SIZE` is correct today precisely because `count`
increments only on a successful `addBatch`, so it and the statement's queue stay
in lockstep. Once a failure path commits or clears outside that rhythm, the two
diverge, and the trailing-partial-batch check at `:2656` starts flushing an empty
batch or skipping a non-empty one. Introduce an explicit `pendingInBatch` counter
rather than deriving it from `count`.

---

## Two things that look like bugs and aren't

- **`nsAuids` is populated at `:2606`, before the artifact is known to insert.**
  A skipped or failed artifact still gets its (namespace, AUID) into the returned
  set, so `invalidateAuSize` runs for an AU we may not have touched. That is the
  safe direction: over-invalidating an AU size only forces a recompute. Leave it.

- **`getNamespaces()` holds an unbounded `HashMap` LRU** (`:974`). Namespace
  cardinality is small and bounded in practice. Called out only because R10's
  consequence is unbounded *in time* for that specific map.

---

## Caller-side findings

Everything above is inside the index. These are in the code that drives it, and
several have a larger blast radius than anything in the index itself. All were
verified directly against the source.

### C1 — a `finishBulkStore` failure destroys the AU's entire volatile index — CRITICAL

**Anchor:** `lockss-core/src/main/java/org/lockss/rs/io/index/DispatchingArtifactIndex.java:345-358`

```java
    ArtifactIndex volInd;
    volInd = tempIndexMap.remove(key(namespace, auid));
    if (volInd == null) {
      throw new IllegalStateException("Attempt to finishBulkStore of AU not in bulk store mode: " + namespace + ", " + auid);
    }
    volInd.stop();
    try {
      Iterable<Artifact> artifacts = volInd.getArtifactsAllVersions(namespace, auid, true);
      masterIndex.indexArtifacts(artifacts);
    } catch (IOException e) {
      log.error("Failed to retrieve and bulk add artifacts", e);
      throw e;
    }
```

The `remove()` happens **before** the risky work. The catch rethrows without
restoring the entry, and there is no `finally`. The only remaining reference to
the AU's entire in-memory index is the local `volInd`, which goes out of scope
when the exception propagates.

So any failure inside `indexArtifacts` — including every finding in this
document — doesn't just lose a batch. **It loses the whole AU's index, with no
way to retry**, because a second `finishBulkStore` hits the
`IllegalStateException` at `:347`. Recovery is a full reindex from WARCs.

This is why R1–R6 matter more than their own blast radius suggests: in this
call path, "lose a batch" and "lose the AU, permanently" are the same event.

**Fix direction, with one thing to get right.** Do not remove from
`tempIndexMap` until the copy has succeeded — and note that `volInd.stop()` at
`:351` also runs before the copy, so a naive "restore the entry in a `catch`"
puts a *stopped* index back into the map.

In practice that is survivable but sloppy: `VolatileArtifactIndex.stop()`
(`VolatileArtifactIndex.java:120-122`) only calls
`setState(ArtifactIndexState.STOPPED)`, and the only code that consults the state
is `isReady()` (`:842`, `return getState() == ArtifactIndexState.RUNNING`). No
read or write path checks it, so a restored-stopped index still holds and serves
all its artifacts — but it reports itself not ready. **Move `stop()` after the
successful copy** rather than restoring a stopped index.

**Retry reachability — verified.** Restoring `tempIndexMap` is sufficient;
nothing else gates a retried FINISH. `AusApiServiceImpl.java:430-431` does
`bulkAuids.remove(auid)` before `finishBulkStore` and does not restore it on the
`catch`, but that set gates nothing on this path: `AusApiServiceImpl`'s
`bulkAuids` (`:45`) is a per-bean instance field that is **written at `:425`/`:431`
and never read**. The only read is in a *different* bean's *different* field,
`ArtifactsApiServiceImpl.bulkAuids` (`:88`, read at `:1302`), which nothing ever
adds to.

That is a separate latent bug worth recording: the cache-invalidate suppression
at `ArtifactsApiServiceImpl.java:1302` — "unless in bulk mode, where it takes
noticeable time and is unnecessary" — **never fires**, because its set is always
empty. Either the field was meant to be `static`/shared or the two beans were
meant to consult one source. Not a robustness issue; a missed optimization and a
misleading pair of call sites.

### C2 — the artifact iterable handed to `indexArtifacts` is single-use and fails silently — HIGH

**Anchor:** `lockss-core/src/main/java/org/lockss/rs/io/index/VolatileArtifactIndex.java:456-458`

```java
        return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
```

`IteratorUtils.asIterable` returns a single-use `IteratorIterable`. A second
`iterator()` call hands back the same, already-exhausted iterator — it returns
**zero artifacts and does not throw**.

This closes off the most natural robustness design: "on failure, re-drive the
same iterable one item at a time." Any replay inside `addArtifacts` must work
from artifacts it has already buffered, never by re-iterating the input. It also
means that if a retry is ever added at this level, it will silently succeed
having indexed nothing.

Separately, the `sorted()` stage materializes on the first `next()`, so a
concurrent `addArtifactData` during the copy surfaces as a
`ConcurrentModificationException` thrown **mid-iteration inside**
`masterIndex.indexArtifacts` — an unskippable failure of exactly the kind R6
exists to mitigate. (Note `waitForCommitTasks` at `DispatchingArtifactIndex.java:336-344`
drains the commit/copy executor, not in-flight `addArtifactData` calls, so this
window is real.)

This is the same defect as finding 8 in the correctness review, on the volatile
side rather than the SQL side.

### C3 — one malformed WARC record discards up to 999 parsed artifacts and skips the rest of the file — HIGH

**Anchors:** `lockss-core/src/main/java/org/lockss/rs/io/storage/warc/WarcArtifactDataStore.java:3002-3003`,
`:3077-3084`, `:2887-2896`

```java
            String dateVal = record.getHeader(WARCConstants.HEADER_KEY_DATE).value;
            Instant created = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(dateVal));
```

A record missing `WARC-Date` is an NPE on `.value`; a malformed one is a
`DateTimeParseException`. Both are unchecked. The per-record catch is
`IOException` **only** — and it isn't isolation anyway, it logs and rethrows:

```java
        } catch (IOException e) {
          log.error("Could not index artifact from WARC record [...]", ...);
          throw e;
        }
```

So the unchecked exception propagates out of `indexArtifactsFromWarc`, taking
with it the partially-filled `List<Artifact> batch` — up to 999 already-parsed,
perfectly good artifacts that were never handed to `reindexArtifacts`. It lands
in the per-WARC handler:

```java
            } catch (Exception e) {
              log.error("Error reindexing artifacts from WARC [warc: {}]", warcPath, e);
            }
```

which is the one place in this surface that *does* continue — to the next WARC
file. So the blast radius of one bad record is **the remainder of an entire WARC
file**, which for a permanent WARC can be ~1 GB of artifacts.

Worse, the file is not added to `indexedWarcs` and no CSV row is written
(`:2892-2894`), so it is correctly marked un-reindexed — but `addDateSuffix`
at `:2903` renames the CSV away on completion regardless, and the next run
starts a fresh file with no memory that anything was skipped. The failure is
recorded only in the log.

Several fields are also read **outside** the per-record try —
`reader.getStartOffset()`, `recordIter.next()`, `getWarcLength(...)`,
`makeWarcRecordStorageUrl(...)` at `:2966-2974` — so a truncated record fails
there with the same consequence.

**Fix direction.** Make the per-record catch `Exception`, log the record ID, and
`continue` — one bad record should cost one artifact. Move the pre-try reads
inside. This is genuinely per-item isolation and is independent of everything in
the index.

### C4 — there is no retry of `finishBulkStore` anywhere, and none is possible — HIGH (context)

The code comments in `SQLArtifactIndexManagerSql.java:723` and `:2601` justify
`UPSERT_ARTIFACT_QUERY` by "a retried `finishBulkStore` that re-presents
artifacts already committed by an earlier partial run." **That retry does not
exist.**

- `RestUtil.callRestService`
  (`lockss-util/lockss-util-rest/src/main/java/org/lockss/util/rest/RestUtil.java:194-229`)
  retries only `ConnectException`/`UnknownHostException`. A 500 from a failed
  `finishBulkStore` is not retried.
- `RestLockssRepository.callBulkOp:1407-1410` logs and rethrows.
- And per C1, a retry couldn't work anyway — the temp index is already gone.

The upsert's idempotency is therefore only exercisable *within* a single
`addArtifacts` call, never across calls. This does not make the upsert wrong —
it is the right primitive and R5's replay design depends on it — but the
comments overstate what exists today, and any recovery design has to build the
retry, not assume it.

The only resume bookkeeping in this whole surface is per-WARC-file, in the
reindex path: `REINDEXED_WARCS_FILE` (`WarcArtifactDataStore.java:2823-2850`,
appended only after a fully successful file) and the reindex token file
(`BaseLockssRepository.java:172-191`, `:307-320`). Resume granularity is a whole
WARC re-read from offset 0. There is no per-artifact success bookkeeping
anywhere.

### C5 — partial success leaves AU sizes stale with no path to invalidation — MEDIUM

**Anchor:** `SQLArtifactIndex.java:142-154`

`addArtifacts` commits every 1000 artifacts, but on failure it throws, so the
returned `Set<Pair<String,String>>` never reaches the `invalidateAuSize` loop.
Every batch that *did* commit leaves its AU's cached size stale, and nothing
will ever invalidate it.

This is a direct argument for R6 (commit the good prefix) plus the reporting
channel: whatever partial-success information survives a failure has to carry
the namespace/AUID pairs, not just the artifact count.

### C6 — Solr and SQL disagree about what `finishBulkStore` failure means — MEDIUM

`SolrArtifactIndex.java:842-845` swallows each failed 1000-doc batch
(`catch (Exception e) { log.error("Failed to perform UpdateRequest", e); }`) and
swallows the final hard commit too (`:856-859`), returning normally. The same
`finishBulkStore` call against Solr silently loses batches; against SQL it
throws and, per C1, destroys the AU.

Neither behaviour is right. Solr's is the failure mode this whole exercise is
meant to avoid (silent loss); SQL's is the one that prompted it (total loss).
Whatever reporting channel is chosen should be implemented in both, so the two
indexes at least fail the same way. Worth noting when deciding the channel.

### C7 — the import path is the model to copy — reference

`BaseLockssRepository.addArtifacts` (`lockss-core/src/main/java/org/lockss/rs/BaseLockssRepository.java:498-621`)
already does this correctly: a per-record
`catch (Exception e) { log.error(...); status.setStatus(ERROR); }` at `:603-606`
that continues the loop and reports the failure per item through
`ImportStatusIterable`. That is exactly the shape R3/R7/C3 want, and it already
exists in-tree — including the reporting channel question from the top of this
document.

Its one gap is the same as C3's: `for (ArchiveRecord record : archiveReader)` at
`:540` iterates outside the per-record try, so one unparseable record still kills
the rest of the upload.

---

## Recommended order of work

**Land now — small, self-contained, no structural change. Together these cover
both incident shapes and the worst of the caller-side loss:**

1. **C1** — stop removing the AU from `tempIndexMap` before the copy succeeds.
   Largest blast-radius reduction available anywhere in this document, and it is
   a few lines. Do this first; it changes what every other failure below costs.
2. **R1** — widen the catches in `analyzeTableOutOfBand`, `seedUrlEstimateIfNeeded`
   and `estimatedRowCount` to `Exception`. Two lines; also fixes R2. This is the
   incompletely-fixed half of the ANALYZE incident.
3. **C3** — per-record `catch (Exception) { log; continue; }` in
   `indexArtifactsFromWarc`, and move the pre-try reads inside. True per-item
   isolation, independent of the index.
4. **R7's `throw e`** — delete the rethrow after the warn in
   `SQLArtifactIndex.reindexArtifacts`. One line.
5. **R8** — complete the future exceptionally on `RuntimeException`; fix the
   unchecked `IOException` cast.

**Land next — per-item isolation inside the index; needs the T1–T3 care:**

6. **R3** — catch `RuntimeException` around `bindArtifactInsertParams`, log, skip.
   Free by M1: no rollback needed. This is the direct answer to "lose one item,
   not the batch", and covers the null-version NPE.
7. **R4's cheap half** — validate uri/namespace/auid/version for null before
   touching the database, turning server-side failures into free skips.
8. **R7's loops** — per-item catch-and-continue in both reindex paths.

**Needs a decision before starting:**

9. **The reporting channel** (see the section near the top; C5, C6 and C7 bear on
   it). Required before 6–8 can be considered complete rather than merely
   quieter. `BaseLockssRepository.addArtifacts`' `ImportStatusIterable` is the
   in-tree precedent.
10. **R5 + R6 together** — rollback-and-replay on flush failure, plus the
    good-prefix commit. These share the poisoned-transaction flag R6 needs to
    avoid PostgreSQL's silent commit-becomes-rollback, so they are one change,
    not two. This is a rewrite of `addArtifacts`' connection and statement
    lifecycle (M2 forces a reconnect and re-prepare) and is materially larger
    than everything above it. Note C2: the replay must work from artifacts
    already buffered, never by re-iterating the input.

**Not worth doing:** R11, R12.
