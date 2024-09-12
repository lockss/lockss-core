package org.lockss.rs.io.storage.warc;

import org.apache.commons.io.IOUtils;
import org.archive.io.warc.WARCRecord;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.test.LockssTestCase5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestWarcArtifactDataUtil extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final String ARTIFACT_BYTES = "If kittens could talk, they would whisper soft riddles into my ear," +
      " tickling me with their whiskers, making me laugh.";

  private static final String ARTIFACT_HTTP_ENCODED = "HTTP/1.1 200 OK\n" +
      "Server: nginx/1.12.0\n" +
      "Date: Wed, 30 Aug 2017 22:36:15 GMT\n" +
      "Content-Type: text/html\n" +
      "Content-Length: 118\n" +
      "Last-Modified: Fri, 07 Jul 2017 09:43:40 GMT\n" +
      "Connection: keep-alive\n" +
      "ETag: \"595f57cc-76\"\n" +
      "Accept-Ranges: bytes\n" +
      "\n" +
      ARTIFACT_BYTES;

  private static final String ARTIFACT_WARC_ENCODED = "WARC/1.0\n" +
      "WARC-Record-ID: <urn:uuid:74e3b795-c1e6-49ce-8b27-de7e747322b7>\n" +
      "Content-Length: " + ARTIFACT_HTTP_ENCODED.length() + "\n" +
      "WARC-Date: 1\n" +
      "WARC-Type: response\n" +
      "WARC-Target-URI: http://biorisk.pensoft.net/article_preview.php?id=1904\n" +
      "Content-Type: application/http; msgtype=response\n" +
      "X-LockssRepo-Artifact-Id: 74e3b795-c1e6-49ce-8b27-de7e747322b7\n" +
      "X-LockssRepo-Artifact-Namespace: demo\n" +
      "X-LockssRepo-Artifact-AuId: testauid\n" +
      "X-LockssRepo-Artifact-Uri: http://biorisk.pensoft.net/article_preview.php?id=1904\n" +
      "X-LockssRepo-Artifact-Version: 1" +
      "\n" +
      ARTIFACT_HTTP_ENCODED;

  @Test
  public void fromArchiveRecord() {
    // WIP - disabled
    if (false) {
      try {
        log.info("Attempt test fromArchiveRecord()");
        InputStream warcStream = new ByteArrayInputStream(ARTIFACT_WARC_ENCODED.getBytes());

        WARCRecord record = new WARCRecord(warcStream, "TestArtifactDataFactory", 0, false, false);
        assertNotNull(record);

        ArtifactData artifact = WarcArtifactDataUtil.fromArchiveRecord(record);
        assertNotNull(artifact);

        ArtifactIdentifier identifier = artifact.getIdentifier();
        assertNotNull(identifier);
        assertEquals("id1", identifier.getUuid());
        assertEquals("ns1", identifier.getNamespace());
        assertEquals("auid1", identifier.getAuid());
        assertEquals("url1", identifier.getUri());
        assertEquals("v1", identifier.getVersion());

        String expectedMsg = "hello world";
        assertEquals(expectedMsg, IOUtils.toString(artifact.getInputStream()));

      } catch (IOException e) {
        e.printStackTrace();
        fail(String.format("Unexpected IOException was caught: %s", e.getMessage()));
      }
    }
  }
}
