/*
 * Copyright (c) 2018-2019, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class TestLocalArtifactIndex extends AbstractArtifactIndexTest<LocalArtifactIndex> {
    private final static L4JLogger log = L4JLogger.getLogger();

    private static final String PERSISTED_INDEX_NAME = "repository-index.ser";
    private File testBaseDir;

    // *****************************************************************************************************************
    // * JUNIT LIFECYCLE
    // *****************************************************************************************************************

    public LocalArtifactIndex makeArtifactIndex() throws IOException {
      testBaseDir = getTempDir();
      testBaseDir.deleteOnExit();

      return new LocalArtifactIndex(testBaseDir, PERSISTED_INDEX_NAME);
    }

    @AfterEach
    void tearDown() {
        quietlyDeleteDir(testBaseDir);
    }

    // *****************************************************************************************************************
    // * UTILITY METHODS
    // *****************************************************************************************************************

    protected static void quietlyDeleteDir(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        }
        catch (IOException ioe) {
            // oh well.
        }
    }

    private void compareArtifactIndexes(ArtifactIndex index1, ArtifactIndex index2) throws IOException {
        // Compare namespaces
        List<String> nss1 = IteratorUtils.toList(index1.getNamespaces().iterator());
        List<String> nss2 = IteratorUtils.toList(index2.getNamespaces().iterator());
        if (!(nss1.containsAll(nss2) && nss2.containsAll(nss1))) {
            fail("Expected both the original and rebuilt artifact indexes to contain the same set of namespaces");
        }

        // Iterate over the namespaces
        for (String ns : nss1) {
            // Compare the set of AUIDs
            List<String> auids1 = IteratorUtils.toList(index1.getAuIds(ns).iterator());
            List<String> auids2 = IteratorUtils.toList(index2.getAuIds(ns).iterator());
            if (!(auids1.containsAll(auids2) && auids2.containsAll(auids1))) {
                fail("Expected both the original and rebuilt artifact indexes to contain the same set of AUIDs");
            }

            // Iterate over AUIDs
            for (String auid : auids1) {
                List<Artifact> artifacts1 = IteratorUtils.toList(index1.getArtifacts(ns, auid, true).iterator());
                List<Artifact> artifacts2 = IteratorUtils.toList(index2.getArtifacts(ns, auid, true).iterator());

                // Debugging
                artifacts1.forEach(artifact -> log.debug("Artifact from artifact1: {}", artifact));
                artifacts2.forEach(artifact -> log.debug("Artifact from artifact2: {}", artifact));

                if (!(artifacts1.containsAll(artifacts2) && artifacts2.containsAll(artifacts1))) {
                    fail("Expected both the original and rebuilt artifact indexes to contain the same set of artifacts");
                }
            }
        }
    }

    // *****************************************************************************************************************
    // * IMPLEMENTATION SPECIFIC TESTS
    // *****************************************************************************************************************

    @Test
    void addToIndexTest() throws IOException {
        // Create an Artifact to add
        String artifactUuid = UUID.randomUUID().toString();
        ArtifactIdentifier ident = new ArtifactIdentifier(artifactUuid, "ns1", "auid1", "uri1", 1);
        Artifact artifact = new Artifact(ident, true, "volatile://test.warc?offset=0", 1024, "sha1");

        // Add Artifact to index
        index.addToIndex(artifactUuid, artifact);

        // Check that the persisted file exists
        File persistedIndexFile = new File(testBaseDir, PERSISTED_INDEX_NAME);
        assertTrue(persistedIndexFile.exists());
        assertTrue(persistedIndexFile.isFile());

        // Populate a second LocalArtifactIndex from the persisted file
        LocalArtifactIndex index2 = new LocalArtifactIndex(testBaseDir, PERSISTED_INDEX_NAME);

        // Compare the
        compareArtifactIndexes(index, index2);
    }

    @Test
    void removeFromIndexTest() {
        // TODO
    }

    @Test
    void populateFromPersistenceTest() {
        // TODO
    }

    @Test
    void persistTest() {
        // TODO
    }

    @Disabled
    @Test
    @Override
    public void testInitIndex() throws Exception {
        // TODO
    }

    @Disabled
    @Test
    @Override
    public void testShutdownIndex() throws Exception {
        // TODO
    }
}
