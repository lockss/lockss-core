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
package org.lockss.rs.io.index;

import java.util.Objects;

public class ArtifactIndexVersion {
  public static final ArtifactIndexVersion UNKNOWN =
      ArtifactIndexVersion.create("UNKNOWN", Integer.MAX_VALUE);

  private static ArtifactIndexVersion create(String type, int version) {
    return new ArtifactIndexVersion()
        .setIndexType(type)
        .setIndexVersion(version);
  }

  private String indexType;
  private int indexVersion;

  public ArtifactIndexVersion setIndexType(String indexType) {
    this.indexType = indexType;
    return this;
  }

  public String getIndexType() {
    return indexType;
  }

  public ArtifactIndexVersion setIndexVersion(int indexVersion) {
    this.indexVersion = indexVersion;
    return this;
  }

  public int getIndexVersion() {
    return indexVersion;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ArtifactIndexVersion that)) return false;

    return indexVersion == that.indexVersion && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(indexType);
    result = 31 * result + indexVersion;
    return result;
  }
}
