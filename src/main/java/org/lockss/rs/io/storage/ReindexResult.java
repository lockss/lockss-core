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

package org.lockss.rs.io.storage;

import lombok.Getter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Accumulates the outcome of a reindex pass -- either a whole-data-store
 * reindex or a targeted per-AU one.
 * <p>
 * A reindex used to report nothing at all: per-WARC failures were caught and
 * logged, the ledger was rotated and the {@code index/reindexing} token deleted
 * regardless, so "completed with failures" was indistinguishable from
 * "completed". This carries the information needed to tell them apart:
 * <ul>
 *   <li>the WARCs that could not be read, with their causes;</li>
 *   <li>the AUIDs that had no directory under any base path (targeted pass);</li>
 *   <li>artifacts indexed, and artifacts the index itself skipped.</li>
 * </ul>
 * <p>
 * Deliberately <em>not</em> a retry list: a reindex pass runs once and is done.
 * Re-attempting failed WARCs after fixing them is an explicit operator action.
 * <p>
 * Not thread safe; a reindex pass is single-threaded.
 */
public class ReindexResult {

  /** One WARC file that could not be reindexed, and why. */
  public static class WarcFailure {
    private final Path warcFile;
    private final String error;

    public WarcFailure(Path warcFile, String error) {
      this.warcFile = warcFile;
      this.error = error;
    }

    public Path getWarcFile() {
      return warcFile;
    }

    /** A human-readable description of the failure, for the CSV report. */
    public String getError() {
      return error;
    }

    @Override
    public String toString() {
      return warcFile + ": " + error;
    }
  }

  /** One AU whose reindex failed outright, and why. */
  public static class AuFailure {
    private final String auid;
    private final String error;

    public AuFailure(String auid, String error) {
      this.auid = auid;
      this.error = error;
    }

    public String getAuid() {
      return auid;
    }

    public String getError() {
      return error;
    }

    @Override
    public String toString() {
      return auid + ": " + error;
    }
  }

  private final List<WarcFailure> warcFailures = new ArrayList<>();
  private final List<AuFailure> auFailures = new ArrayList<>();
  private final List<String> missingAus = new ArrayList<>();
  @Getter
  private long artifactsIndexed = 0;
  @Getter
  private long artifactsSkipped = 0;
  @Getter
  private int warcsAttempted = 0;
  @Getter
  private int warcsSucceeded = 0;

  /** Records that a WARC file was reindexed successfully. */
  public void addWarcSucceeded(long numIndexed) {
    warcsAttempted++;
    warcsSucceeded++;
    artifactsIndexed += numIndexed;
  }

  /** Records that a WARC file could not be reindexed. */
  public void addWarcFailure(Path warcFile, Throwable cause) {
    addWarcFailure(warcFile, describe(cause));
  }

  /** Records that a WARC file could not be reindexed. */
  public void addWarcFailure(Path warcFile, String error) {
    warcsAttempted++;
    warcFailures.add(new WarcFailure(warcFile, error));
  }

  /**
   * Records an AUID that has no AU directory under any configured base path,
   * and so cannot be reindexed at all.
   */
  public void addMissingAu(String auid) {
    missingAus.add(auid);
  }

  /**
   * Records an AU whose reindex threw, as distinct from one that simply has no
   * directory: the cause here is a real failure, not a missing AU.
   */
  public void addAuFailure(String auid, Throwable cause) {
    auFailures.add(new AuFailure(auid, describe(cause)));
  }

  /** Records artifacts the index refused, from {@code ArtifactIndex.reindexArtifacts()}. */
  public void addArtifactsSkipped(long n) {
    artifactsSkipped += n;
  }

  /** Records artifacts indexed outside of a completed WARC (salvaged batches). */
  public void addArtifactsIndexed(long n) {
    artifactsIndexed += n;
  }

  /** Folds another result into this one. */
  public ReindexResult add(ReindexResult other) {
    if (other != null) {
      warcFailures.addAll(other.warcFailures);
      auFailures.addAll(other.auFailures);
      missingAus.addAll(other.missingAus);
      artifactsIndexed += other.artifactsIndexed;
      artifactsSkipped += other.artifactsSkipped;
      warcsAttempted += other.warcsAttempted;
      warcsSucceeded += other.warcsSucceeded;
    }
    return this;
  }

  public List<WarcFailure> getWarcFailures() {
    return Collections.unmodifiableList(warcFailures);
  }

  public List<AuFailure> getAuFailures() {
    return Collections.unmodifiableList(auFailures);
  }

  public List<String> getMissingAus() {
    return Collections.unmodifiableList(missingAus);
  }

  public int getWarcsFailedCount() {
    return warcFailures.size();
  }

  /** True iff at least one WARC failed, or at least one AU failed or was unrecoverable. */
  public boolean hasFailures() {
    return !isSuccessful();
  }

  /** True iff nothing failed. Note that skipped artifacts do not make a run unsuccessful. */
  public boolean isSuccessful() {
    return warcFailures.isEmpty() && auFailures.isEmpty() && missingAus.isEmpty();
  }

  private static String describe(Throwable cause) {
    if (cause == null) {
      return "(unknown)";
    }
    return cause.toString();
  }

  @Override
  public String toString() {
    return "[ReindexResult: warcs=" + warcsSucceeded + "/" + warcsAttempted
        + ", artifactsIndexed=" + artifactsIndexed
        + ", artifactsSkipped=" + artifactsSkipped
        + ", auFailures=" + auFailures
        + ", missingAus=" + missingAus + "]";
  }
}
