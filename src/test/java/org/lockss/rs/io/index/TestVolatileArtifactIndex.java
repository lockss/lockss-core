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

import org.junit.jupiter.api.Test;

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
}
