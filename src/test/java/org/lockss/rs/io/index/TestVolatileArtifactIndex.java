/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.lockss.rs.io.index;

import org.apache.commons.collections4.IterableUtils;
import org.junit.jupiter.api.Test;
import org.lockss.rs.io.storage.warc.WarcArtifactDataUtil;
import org.lockss.util.ListUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;

import java.net.URI;
import java.util.UUID;

/**
 * Test class for {@code org.lockss.laaws.rs.io.index.VolatileArtifactIndex}
 */
public class TestVolatileArtifactIndex extends AbstractArtifactIndexTest<VolatileArtifactIndex> {

  // *******************************************************************************************************************
  // * JUNIT LIFECYCLE
  // *******************************************************************************************************************

  @Override
  protected VolatileArtifactIndex makeArtifactIndex() {
    return new VolatileArtifactIndex();
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION SPECIFIC TESTS
  // *******************************************************************************************************************

  @Test
  @Override
  public void testInitIndex() throws Exception {
    VolatileArtifactIndex index = makeArtifactIndex();
    index.init();
    assertTrue(index.getState() == AbstractArtifactIndex.ArtifactIndexState.INITIALIZED);
  }

  @Test
  @Override
  public void testShutdownIndex() throws Exception {
    VolatileArtifactIndex index = makeArtifactIndex();
    index.stop();
    assertTrue(index.getState() == AbstractArtifactIndex.ArtifactIndexState.STOPPED);
  }

  /**
   * DRAINING refuses new work but still lets an already-accepted storage URL
   * update land. {@link DispatchingArtifactIndex#finishBulkStore} sets this
   * before waiting for an AU's queued copy tasks, which exist to make exactly
   * that call.
   */
  @Test
  public void testWriteModeDraining() throws Exception {
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("ns1", "auid1", "http://example.com/1");
    spec.setArtifactUuid(UUID.randomUUID().toString());
    indexArtifactSpec(spec);

    String uuid = spec.getArtifactUuid();
    Artifact other = makeUnindexedArtifact("http://example.com/2");

    assertFalse(index.isReadOnly());

    index.setWriteMode(AbstractArtifactIndex.WriteMode.DRAINING, "testing [ns: ns1, auid: auid1]");
    assertTrue(index.isReadOnly());
    assertEquals(AbstractArtifactIndex.WriteMode.DRAINING, index.getWriteMode());

    assertNewWorkRefused(uuid, other);

    // But a copy task queued before the drain can still finish.
    assertNotNull(index.updateStorageUrl(uuid, "file:///permanent"));
    assertEquals("file:///permanent", index.getArtifact(uuid).getStorageUrl());

    assertReadsUnaffected(uuid, other);

    // Writes resume once the restriction is lifted.
    index.setWritable();
    assertFalse(index.isReadOnly());
    assertEquals(AbstractArtifactIndex.WriteMode.WRITABLE, index.getWriteMode());
    assertNotNull(index.commitArtifact(uuid));
    assertTrue(index.getArtifact(uuid).isCommitted());
  }

  /**
   * FROZEN refuses every modification, including the storage URL updates
   * DRAINING allows, and serves every read. Set for the duration of the
   * bulk-store copy itself.
   */
  @Test
  public void testWriteModeFrozen() throws Exception {
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("ns1", "auid1", "http://example.com/1");
    spec.setArtifactUuid(UUID.randomUUID().toString());
    indexArtifactSpec(spec);

    String uuid = spec.getArtifactUuid();
    String storageUrl = index.getArtifact(uuid).getStorageUrl();
    Artifact other = makeUnindexedArtifact("http://example.com/2");

    index.setWriteMode(AbstractArtifactIndex.WriteMode.FROZEN, "testing [ns: ns1, auid: auid1]");
    assertTrue(index.isReadOnly());
    assertEquals(AbstractArtifactIndex.WriteMode.FROZEN, index.getWriteMode());

    assertNewWorkRefused(uuid, other);

    // Unlike DRAINING, completions are refused too.
    assertThrowsMatch(IllegalStateException.class, "read-only: testing",
                      () -> index.updateStorageUrl(uuid, "file:///elsewhere"));
    assertEquals(storageUrl, index.getArtifact(uuid).getStorageUrl());

    assertReadsUnaffected(uuid, other);

    index.setWritable();
    assertNotNull(index.commitArtifact(uuid));
    assertTrue(index.getArtifact(uuid).isCommitted());
  }

  /** setWriteMode() rejects arguments that would silently do nothing. */
  @Test
  public void testWriteModeArgs() throws Exception {
    assertThrowsMatch(IllegalArgumentException.class, "Null write mode",
                      () -> index.setWriteMode(null, "reason"));
    assertThrowsMatch(IllegalArgumentException.class, "Use setWritable()",
                      () -> index.setWriteMode(AbstractArtifactIndex.WriteMode.WRITABLE, "reason"));
    assertThrowsMatch(IllegalArgumentException.class, "Null reason",
                      () -> index.setWriteMode(AbstractArtifactIndex.WriteMode.FROZEN, null));
    assertFalse(index.isReadOnly());
  }

  /** Asserts that every operation introducing new work is refused. */
  private void assertNewWorkRefused(String uuid, Artifact other) throws Exception {
    // The reason is in the message so an operator can tell which AU froze it.
    String msg = "read-only: testing";

    assertThrowsMatch(IllegalStateException.class, msg, () -> index.indexArtifact(other));
    assertThrowsMatch(IllegalStateException.class, msg,
                      () -> index.indexArtifacts(ListUtil.list(other)));
    assertThrowsMatch(IllegalStateException.class, msg, () -> index.reindexArtifact(other));
    assertThrowsMatch(IllegalStateException.class, msg,
                      () -> index.reindexArtifacts(ListUtil.list(other)));
    assertThrowsMatch(IllegalStateException.class, msg, () -> index.commitArtifact(uuid));
    assertThrowsMatch(IllegalStateException.class, msg,
                      () -> index.commitArtifact(UUID.fromString(uuid)));
    assertThrowsMatch(IllegalStateException.class, msg, () -> index.deleteArtifact(uuid));
    assertThrowsMatch(IllegalStateException.class, msg,
                      () -> index.deleteArtifact(UUID.fromString(uuid)));
    assertThrowsMatch(IllegalStateException.class, msg, () -> index.clearIndex());

    // Nothing above changed the index.
    assertTrue(index.artifactExists(uuid));
    assertFalse(index.getArtifact(uuid).isCommitted());
    assertFalse(index.artifactExists(other.getUuid()));
  }

  /** Asserts that reads work in any write mode. */
  private void assertReadsUnaffected(String uuid, Artifact other) throws Exception {
    assertTrue(index.artifactExists(uuid));
    assertNotNull(index.getArtifact(uuid));
    assertFalse(index.artifactExists(other.getUuid()));
    assertEquals(1, IterableUtils.size(index.getArtifactsAllVersions("ns1", "auid1", true)));
    assertNotNull(index.auSize("ns1", "auid1"));
  }

  /** An {@link Artifact} that has not been added to the index. */
  private Artifact makeUnindexedArtifact(String url) throws Exception {
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("ns1", "auid1", url);
    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create(spec.getArtifactUuid()));
    spec.setVersion(1);
    return WarcArtifactDataUtil.getArtifact(spec.getArtifactData());
  }
}
