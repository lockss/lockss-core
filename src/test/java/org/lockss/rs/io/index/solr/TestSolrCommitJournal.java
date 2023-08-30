/*
 * Copyright (c) 2021, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.rs.io.index.solr;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.test.LockssTestCase5;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.lockss.rs.io.index.solr.SolrCommitJournal.*;
import static org.lockss.rs.io.index.solr.SolrCommitJournal.SolrOperation.ADD;
import static org.lockss.rs.io.index.solr.SolrCommitJournal.SolrOperation.DELETE;
import static org.mockito.Mockito.*;

public class TestSolrCommitJournal extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();
  private final static String EMPTY_STRING = "";

  protected boolean wantTempTmpDir() {
    return true;
  }

  /**
   * Tests for {@link SolrCommitJournal.SolrJournalWriter}.
   */
  @Nested
  class TestSolrJournalWriter {
    /**
     * Test for {@link SolrCommitJournal.SolrJournalWriter#logOperation(SolrOperation, String, String)}.
     * @throws Exception
     */
    @Test
    public void testLogOperation() throws Exception {
      ArtifactIdentifier artifactId = new ArtifactIdentifier(
          "test-artifact",
          "test-namespace",
          "test-auid",
          "test-url",
          1
      );

      // Create an instance of Artifact to represent the artifact
      Artifact artifact = new Artifact(
          artifactId,
          false,
          "test-storage-url",
          1234L,
          "test-digest");

      // Save the artifact collection date.
      artifact.setCollectionDate(1234L);

      // Test for ADD
      try (SolrCommitJournal.SolrJournalWriter writer =
          new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        // Write CSV record with logOperation
        ObjectMapper mapper = new ObjectMapper();
        String artifactJson = mapper.writeValueAsString(artifact);
        writer.logOperation(ADD, artifact.getUuid(), artifactJson);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("ADD", record.get(JOURNAL_HEADER_SOLR_OP));

        String json = "{\"uuid\":\"test-artifact\",\"namespace\":\"test-namespace\",\"auid\":\"test-auid\"," +
            "\"uri\":\"test-url\",\"sortUri\":\"test-url\",\"version\":1,\"committed\":false,\"storageUrl\":\"test-storage-url\",\"contentLength\":1234,\"contentDigest\":\"test-digest\",\"collectionDate\":1234}";

        assertEquals(json, record.get(JOURNAL_HEADER_DATA));
      }

      // Test for UPDATE (committed)
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        SolrInputDocument updateSolrDoc = new SolrInputDocument();
        updateSolrDoc.addField("uuid", artifactId.getUuid());

        Map<String, Object> commitFieldMod = new HashMap<>();
        commitFieldMod.put("set", true);
        updateSolrDoc.addField("committed", commitFieldMod);

        // Write CSV record with logOperation
        writer.logOperation(
            SolrOperation.UPDATE_COMMITTED,
            artifact.getUuid(),
            null);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("UPDATE_COMMITTED", record.get(JOURNAL_HEADER_SOLR_OP));

        assertTrue(StringUtils.isEmpty(record.get(JOURNAL_HEADER_DATA)));
      }

      // Test for UPDATE (storage URL)
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        SolrInputDocument updateSolrDoc = new SolrInputDocument();
        updateSolrDoc.addField("uuid", artifactId.getUuid());

        Map<String, Object> storageUrlFieldMod = new HashMap<>();
        storageUrlFieldMod.put("set", "test-storage-url2");
        updateSolrDoc.addField("storageUrl", storageUrlFieldMod);

        // Write CSV record with logOperation
        writer.logOperation(
            SolrOperation.UPDATE_STORAGEURL,
            artifact.getUuid(),
            "test-storage-url2");

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("UPDATE_STORAGEURL", record.get(JOURNAL_HEADER_SOLR_OP));

        assertEquals("test-storage-url2", record.get(JOURNAL_HEADER_DATA));
      }

      // Test for DELETE
      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(getTempFile("journal-test", null).toPath())) {

        // Write CSV record with logOperation
        writer.logOperation(DELETE, artifactId.getUuid(), null);

        // Read the CSV record we just wrote with logOperation
        CSVRecord record = readFirstCSVRecord(writer.getJournalPath());
        assertNotNull(record);

//        assertEquals("test-artifact", record.get(JOURNAL_HEADER_TIME));
        assertEquals("test-artifact", record.get(JOURNAL_HEADER_ARTIFACT_UUID));
        assertEquals("DELETE", record.get(JOURNAL_HEADER_SOLR_OP));
        assertEquals(EMPTY_STRING, record.get(JOURNAL_HEADER_DATA));
      }
    }

    /**
     * Test for {@link SolrJournalWriter#renameWithSuffix(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testRenameWithSuffix() throws Exception {
      File journalFile = getTempFile("journal-test", null);
      Path journalPath = journalFile.toPath();
      String SUFFIX = "test";

      try (SolrCommitJournal.SolrJournalWriter writer =
               new SolrCommitJournal.SolrJournalWriter(journalFile.toPath())) {

        writer.renameWithSuffix(SUFFIX);
        File renamedJournalFile = writer.getJournalPath().toFile();

        Path expectedPath = journalPath
            .resolveSibling(journalPath.getFileName() + "." + SUFFIX);

        assertFalse(journalFile.exists());
        assertEquals(expectedPath, writer.getJournalPath());
        assertTrue(renamedJournalFile.exists());
        assertTrue(renamedJournalFile.isFile());
      }
    }
  }

  /**
   * Test utility. Reads the journal (a CSV file) and returns the first record in it.
   * @param journalPath A {@link Path} containing the path to the journal.
   * @return A {@link CSVRecord} representing the first CSV record in the file.
   * @throws IOException
   */
  private CSVRecord readFirstCSVRecord(Path journalPath) throws IOException {
    try (FileReader reader = new FileReader(journalPath.toFile())) {
      // Read Solr journal as CSV
      Iterable<CSVRecord> records = CSVFormat.DEFAULT
          .withHeader(SOLR_JOURNAL_HEADERS)
          .withSkipHeaderRecord()
          .parse(reader);

      // Read first CSV record if it exists
      Iterator<CSVRecord> recordIterator = records.iterator();
      return recordIterator.hasNext() ? recordIterator.next() : null;
    }
  }

  interface Assertable<T> {
    void runAssert(T input) throws IOException;
  }

  /**
   * Tests for {@link SolrCommitJournal.SolrJournalReader}.
   */
  @Nested
  class TestSolrJournalReader {
    /**
     * Test for {@link SolrCommitJournal.SolrJournalReader#replaySolrJournal(SolrArtifactIndex)}.
     * @throws Exception
     */
    @Test
    public void testReplaySolrJournal() throws Exception {
      testReplaySolrJournal_ADD();
      testReplaySolrJournal_UPDATE_COMMITTED();
      testReplaySolrJournal_UPDATE_STORAGEURL();
      testReplaySolrJournal_DELETE();
    }

    private final String CSV_HEADERS = "time,op,artifactUuid,data\n";

    private void testReplaySolrJournal_ADD() throws Exception {
      ArtifactIdentifier ADD_ARTIFACTID = new ArtifactIdentifier(
          "test-artifact",
          "test-namespace",
          "test-auid",
          "test-url",
          1);

      // Create an instance of Artifact to represent the artifact
      Artifact ADD_ARTIFACT = new Artifact(
          ADD_ARTIFACTID,
          false,
          "test-storage-url1",
          1234,
          "test-digest");

      // Save the artifact collection date.
      ADD_ARTIFACT.setCollectionDate(1234L);

      Path ADD_FILE = writeTmpFile(CSV_HEADERS + "1636690761743,ADD,test-artifact,\"{\n" +
          "  \"\"uuid\"\":\"\"test-artifact\"\",\n" +
          "  \"\"namespace\"\":\"\"test-namespace\"\",\n" +
          "  \"\"auid\"\":\"\"test-auid\"\",\n" +
          "  \"\"uri\"\":\"\"test-url\"\",\n" +
          "  \"\"sortUri\"\":\"\"test-url\"\",\n" +
          "  \"\"version\"\":1,\n" +
          "  \"\"committed\"\":false,\n" +
          "  \"\"storageUrl\"\":\"\"test-storage-url1\"\",\n" +
          "  \"\"contentLength\"\":1234,\n" +
          "  \"\"contentDigest\"\":\"\"test-digest\"\",\n" +
          "  \"\"collectionDate\"\":1234}\"");

      runTestReplaySolrJournal(ADD_FILE, solrIndex -> {
        ArgumentCaptor<Artifact> artifactCaptor = ArgumentCaptor.forClass(Artifact.class);
        verify(solrIndex, times(1)).indexArtifact(artifactCaptor.capture());
        assertEquals(ADD_ARTIFACT, artifactCaptor.getValue());
      });
    }

    private void testReplaySolrJournal_UPDATE_COMMITTED() throws Exception {
      // Write journal with CSV record to temp file
      Path UPDATE_COMMITTED = writeTmpFile(CSV_HEADERS +
          "1636692345004,UPDATE_COMMITTED,test-artifact,\"\"");

      runTestReplaySolrJournal(UPDATE_COMMITTED, solrIndex -> {
        ArgumentCaptor<String> uuidCaptor = ArgumentCaptor.forClass(String.class);
        verify(solrIndex, times(1)).commitArtifact(uuidCaptor.capture());
        assertEquals("test-artifact", uuidCaptor.getValue());
      });
    }

    private void testReplaySolrJournal_UPDATE_STORAGEURL() throws Exception {
      // Write journal with CSV record to temp file
      Path UPDATE_STORAGEURL = writeTmpFile(CSV_HEADERS +
          "1636692454871,UPDATE_STORAGEURL,test-artifact,test-storage-url2");

      runTestReplaySolrJournal(UPDATE_STORAGEURL, solrIndex -> {
        ArgumentCaptor<String> uuidCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> urlCaptor  = ArgumentCaptor.forClass(String.class);
        verify(solrIndex, times(1)).updateStorageUrl(uuidCaptor.capture(), urlCaptor.capture());
        assertEquals("test-artifact", uuidCaptor.getValue());
        assertEquals("test-storage-url2", urlCaptor.getValue());
      });
    }

    private void testReplaySolrJournal_DELETE() throws Exception {
      Path DELETE = writeTmpFile(CSV_HEADERS +
          "1635893660368,DELETE,test-artifact,\n");

      runTestReplaySolrJournal(DELETE, solrIndex -> {
        ArgumentCaptor<String> uuidCaptor = ArgumentCaptor.forClass(String.class);
        verify(solrIndex, times(1)).deleteArtifact(uuidCaptor.capture());
        assertEquals("test-artifact", uuidCaptor.getValue());
      });
    }

    private void runTestReplaySolrJournal(Path journalPath, Assertable<SolrArtifactIndex> assertable) throws Exception {
      // Create new SolrArtifactIndex using mocked SolrClient
      SolrArtifactIndex index = mock(SolrArtifactIndex.class);

      UpdateResponse success = mock(UpdateResponse.class);
      when(success.getResponse()).thenReturn(mock(NamedList.class));
      when(index.handleSolrCommit(ArgumentMatchers.any(SolrArtifactIndex.SolrCommitStrategy.class)))
          .thenReturn(success);

      // Test replay of UPDATE (committed)
      try (SolrCommitJournal.SolrJournalReader journalReader
               = new SolrCommitJournal.SolrJournalReader(journalPath)) {

        // Replay journal
        journalReader.replaySolrJournal(index);

        // Assert replayed operation
        assertable.runAssert(index);

        // Assert hard commit
        verify(index, Mockito.atMost(1)).handleSolrCommit(SolrArtifactIndex.SolrCommitStrategy.HARD);
      }
    }

    /**
     * Test utility. Writes a {@link String} to a temporary file and returns its path.
     * @param content
     * @return A {@link Path} containing the path of the temporary file.
     * @throws IOException Thrown if there are any I/O errors.
     */
    private Path writeTmpFile(String content) throws IOException {
      File tmpFile = getTempFile("TestSolrArtifactIndex", null);

      try (FileWriter writer = new FileWriter(tmpFile, false)) {
        writer.write(content);
        writer.write("\n");
      }

      return tmpFile.toPath();
    }
  }
}
