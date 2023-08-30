/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

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

package org.lockss.rs.io;

import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcFile;
import org.lockss.util.rest.repo.model.Artifact;

/**
 * Abstraction to track and maintain statistics about a set of {@link Artifact} objects. Mainly intended to be
 * subclassed by {@link WarcFile} and support processes within {@link WarcArtifactDataStore}.
 */
public class ArtifactContainerStats {
  private int total = 0;
  private int uncommitted = 0;
  private int committed = 0;
  private int copied = 0;
  private long expiration = 0;

  /**
   * Increments the total number of artifacts.
   */
  public int incArtifactsTotal() {
    return ++total;
  }

  /**
   * Returns the total number of artifacts.
   */
  public int getArtifactsTotal() {
    return total;
  }

  /**
   * Sets the number of uncommitted artifacts to the number provided.
   */
  public ArtifactContainerStats setArtifactsTotal(int artifacts) {
    this.total = artifacts;
    return this;
  }

  /**
   * Increments the number of uncommitted artifacts.
   */
  public int incArtifactsUncommitted() {
    return ++uncommitted;
  }

  /**
   * Decrements the number of uncommitted artifacts.
   */
  public int decArtifactsUncommitted() {
    return --uncommitted;
  }

  /**
   * Returns the number of uncommitted artifacts.
   */
  public int getArtifactsUncommitted() {
    return uncommitted;
  }

  /**
   * Sets the number of uncommitted artifacts to the number provided.
   */
  public ArtifactContainerStats setArtifactsUncommitted(int artifacts) {
    this.uncommitted = artifacts;
    return this;
  }

  /**
   * Returns the number of artifacts in this WARC file that are committed.
   */
  public int getArtifactsCommitted() {
    return committed;
  }

  /**
   * Increments the number of committed (but not copied) artifacts.
   */
  public int incArtifactsCommitted() {
    return ++committed;
  }

  /**
   * Decrements the number of committed (but not copied) artifacts.
   */
  public int decArtifactsCommitted() {
    return --committed;
  }

  /**
   * Sets the number of artifacts in this WARC file that are committed (but not copied).
   */
  public ArtifactContainerStats setArtifactsCommitted(int artifacts) {
    this.committed = artifacts;
    return this;
  }

  /**
   * Returns the number of artifacts that have been copied to permanent storage.
   */
  public int getArtifactsCopied() {
    return copied;
  }

  /**
   * Increments the number of artifacts in this WARC that have been copied to permanent storage.
   */
  public int incArtifactsCopied() {
    return ++copied;
  }

  /**
   * Sets the numbers of artifacts in this WARC file that have been copied to permanent storage.
   */
  public ArtifactContainerStats setArtifactsCopied(int artifacts) {
    this.copied = artifacts;
    return this;
  }

  /**
   * Returns the latest expiration time of all artifacts in this container.
   */
  public long getLatestExpiration() {
    return expiration;
  }

  /**
   * Sets the latest expiration time of the last artifact in this WARC file.
   */
  public ArtifactContainerStats setLatestExpiration(long expiration) {
    this.expiration = expiration;
    return this;
  }

  @Override
  public String toString() {
    return "ArtifactContainerStats{" +
        "total=" + total +
        ", uncommitted=" + uncommitted +
        ", committed=" + committed +
        ", copied=" + copied +
        ", expiration=" + expiration +
        '}';
  }
}
