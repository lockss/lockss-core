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

package org.lockss.rs.io.storage.warc;

import org.lockss.rs.io.ArtifactContainerStats;

import java.nio.file.Path;

/**
 * Class to keep track of a WARC file's length and the number of artifacts contained with it.
 * This class is not thread-safe. The assumption is there is only one user of a WARC file at
 * a time.
 */
public class WarcFile {
  // TODO: Replace with storage URL
  private final Path path;
  private final boolean isCompressed;
  private long length = 0;
  private boolean isMarkedForGC = false;
  private boolean isCheckedOut = false;
  private final ArtifactContainerStats stats = new ArtifactContainerStats();
  private boolean isReleased = false;

  public boolean isCheckedOut() {
    return isCheckedOut;
  }

  public WarcFile setCheckedOut(boolean checkedOut) {
    isCheckedOut = checkedOut;
    return this;
  }

  public ArtifactContainerStats getStats() {
    return stats;
  }

  /**
   * Constructor.
   *
   * @param path A {@link Path} containing the path to the WARC file.
   * @param isCompressed A {@code boolean} indicating whether the WARC file is GZIP compressed.
   */
  public WarcFile(Path path, boolean isCompressed) {
    this.path = path;
    this.isCompressed = isCompressed;
  }

  public Path getPath() {
    return path;
  }

  public long incrementLength(long length) {
    this.length += length;
    return this.length;
  }

  public long getLength() {
    return length;
  }

  public WarcFile setLength(long length) {
    this.length = length;
    return this;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  public boolean isMarkedForGC() {
    return isMarkedForGC;
  }

  public void setMarkedForGC() {
    this.isMarkedForGC = true;
  }

  public void release() {
    this.isReleased = true;
  }

  public boolean isReleased() {
    return isReleased;
  }

  @Override
  public String toString() {
    return "WarcFile{" +
        "path=" + path +
        ", isCompressed=" + isCompressed +
        ", length=" + length +
        ", isMarkedForGC=" + isMarkedForGC +
        ", isCheckedOut=" + isCheckedOut +
        ", stats=" + stats +
        ", isReleased=" + isReleased +
        '}';
  }
}
