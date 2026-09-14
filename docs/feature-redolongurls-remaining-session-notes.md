# `feature-redolongurls-remaining` session notes

## Scope

- Working checkout: `lockss-core`, branch `feature-redolongurls-remaining`,
  based on `origin/feature-redolongurls`.
- Objective: follow up the long-URL schema work with an artifact-index schema
  version 6 migration.

## Findings from the version 5 baseline

- Version 5 has already folded `long_urls` into `urls.url`.
- A direct unique btree index on unbounded URL text is not safe in PostgreSQL.
  Version 5 instead enforces URL uniqueness with `idx4_urls`, a unique
  SHA-256-digest expression index. This is the correct replacement for the
  removed v3 unique URL index.
- Artifact UUID uniqueness already exists: `idx4_artifacts` is a unique index
  on `artifacts.uuid`, created by schema version 3.
- `idx7_artifacts` indexes `(namespace_seq, auid_seq, url_seq, version)`, but
  it is **not** unique. The schema therefore permits multiple artifact rows at
  that tuple when their UUIDs differ.
- `artifacts.committed` is nullable through schema version 5.

## Implemented version 6 work (current branch)

- Default target database version changed from 5 to 6.
- Added the `5 -> 6` dispatcher path.
- Added a migration test that creates a version-5 database containing a NULL
  `artifacts.committed` value, then upgrades it to version 6 and verifies the
  resulting constraint.
- Updated manager tests to create version-6 databases and assert that a direct
  SQL attempt to set `committed` to NULL is rejected.

## Decision still pending: legacy NULL `committed` rows

The currently drafted migration normalizes `committed IS NULL` to `FALSE`, then
adds `NOT NULL`. This policy is **not settled** and should be revised before
the branch is finalized.

Why this needs a decision:

- NULL has no defined artifact-state meaning. Treating it as either committed
  or uncommitted invents meaning.
- Keeping NULL rows is incompatible with a real `NOT NULL` constraint.
- NULL does affect ordinary query behavior. Several queries use `IS TRUE` or
  `= FALSE`, which exclude NULL; the limited `COALESCE` uses do not make NULL
  consistently behave as uncommitted.

Suggested non-disruptive policy to discuss:

1. Detect every legacy NULL row.
2. Write a durable report containing UUID, namespace, AUID, URL, version,
   storage URL, digest, and crawl time; log a high-severity warning.
3. Apply a team-approved repair policy to those rows.
4. Add `NOT NULL` and record schema version 6.

The repair value or inference method is the open question.

## Decision still pending: artifact tuple uniqueness

Consider a unique constraint/index on
`(namespace_seq, auid_seq, url_seq, version)` in `artifacts`.

Why:

- `idx7_artifacts` currently indexes this tuple but does not enforce it.
- Enforcing the invariant in PostgreSQL would simplify the query-side conflict
  detection/arbitration logic and prevent multiple UUIDs from claiming the
  same artifact version.
- The recent long-URL work exposed this failure mode when URL rows were
  collapsed: multiple artifacts could land on the surviving tuple. Version 5
  reports those collisions but deliberately leaves them for human resolution.

Migration implications:

- A unique index cannot be added until every existing duplicate tuple is
  resolved by a defined policy.
- The decision needs a durable resolution/reporting approach, because choosing
  an artifact automatically can lose data or select the wrong storage record.
- This should be a separate schema version from the current v6 `committed`
  nullability work unless the team decides the two data-repair policies belong
  together.

## Upgrade/recovery model

- There is intended to be only one Repository instance connected to a database.
- Production does not intentionally run at an arbitrarily configured lower
  schema target; an older binary should reject a newer database version.
- The relevant recovery concern is interrupted migration.
- Database versions are recorded and committed after each successful version
  migration. A failure in v6 therefore does not replay committed v5 work.
- PostgreSQL executes the v6 `UPDATE` and `ALTER TABLE ... SET NOT NULL` in the
  same transaction. If v6 fails or the connection is interrupted, both changes
  roll back and a future startup retries v6 from version 5.
- Runtime query changes can assume the final schema after a successful startup;
  migration SQL itself must be retry-safe from the prior recorded version.

## Existing migration caveat

Version 5 writes a URL-dedup collision CSV before its database transaction
commits. The database changes are transactional, but the CSV is a
non-transactional side effect: a failed/interrupted v5 attempt can leave a
partial or stale report. A retry normally overwrites it from the rolled-back
v4 state, but the report has no explicit post-commit/resumability protocol.

## Questions for the team

1. What is the authoritative repair policy for legacy NULL `committed` rows?
   Is there a reliable source from which state can be inferred, or should a
   documented default be used after reporting?
2. Is a CSV/log warning sufficient for the repair report, or should resolution
   state be durable/queryable in the database as well?
3. Should the v5 collision-report write be made atomic/post-commit or otherwise
   explicitly resumable?
4. Is the current migration-version transaction/recording model the agreed
   contract for future schema migrations?
5. What resolution policy should precede a unique artifact tuple constraint,
   and should that constraint be introduced in its own schema version?
