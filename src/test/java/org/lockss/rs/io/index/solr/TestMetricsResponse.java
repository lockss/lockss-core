/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.test.LockssTestCase5;
import org.mockito.ArgumentMatchers;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;

import static org.mockito.Mockito.*;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Test for {@link MetricsResponse}.
 */
public class TestMetricsResponse extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private ClientAndServer mockServer;

  // *******************************************************************************************************************
  // * MOCKSERVER FRAMEWORK
  // *******************************************************************************************************************

  @BeforeEach
  public void startMockServer() {
    mockServer = startClientAndServer();
  }

  @AfterEach
  public void stopMockServer() {
    mockServer.stop();
  }

  // *******************************************************************************************************************
  // * TESTS
  // *******************************************************************************************************************

  @Test
  public void testCoreMetrics() throws Exception {
    // ***************************************
    // * Setup test core metrics response data
    // ***************************************

    SimpleOrderedMap coreMetrics = new SimpleOrderedMap();
    coreMetrics.add("CORE.coreName", "lockss-repo");
    coreMetrics.add("CORE.fs.totalSpace", 1234L);
    coreMetrics.add("CORE.fs.usableSpace", 5678L);
    coreMetrics.add("INDEX.sizeInBytes", 4321L);
    coreMetrics.add("CORE.indexDir", "/lockss/test");

    NamedList coreMetricsMap = new NamedList();
    coreMetricsMap.add("solr.core.lockss-repo", coreMetrics);

    NamedList coreMetricsResponseData = new NamedList();
    coreMetricsResponseData.add("metrics", coreMetricsMap);

    // ******************************
    // * Setup MockServer expectation
    // ******************************

    // Marshal test core metrics response data into javabin binary
    UnsynchronizedByteArrayOutputStream os = new UnsynchronizedByteArrayOutputStream();
    new JavaBinCodec().marshal(coreMetricsResponseData, os);

    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/solr/admin/metrics")
        )
        .respond(
            response()
                .withStatusCode(200)
                .withHeaders(
                    new Header("Content-Type", "application/octet-stream")
                )
                .withBody(os.toByteArray())
        );

    // ****************************************************************
    // * Test directly using CoreMetricsRequest and CoreMetricsResponse
    // ****************************************************************

    // Get SolrClient from Solr REST endpoint
    HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl("http://localhost:" + mockServer.getLocalPort() + "/solr")
        .build();

    // Retrieve core metrics from Solr node
    MetricsRequest.CoreMetricsRequest req = new MetricsRequest.CoreMetricsRequest();
    MetricsResponse.CoreMetricsResponse res = req.process(solrClient);
    MetricsResponse.CoreMetrics metrics = res.getCoreMetrics("lockss-repo");

    // Debugging
    log.trace("metrics = {}", metrics);

    // Assert metrics contains expected values
    assertEquals(1234L, metrics.getTotalSpace());
    assertEquals(5678L, metrics.getUsableSpace());
    assertEquals(4321L, metrics.getIndexSizeInBytes());
    assertEquals("/lockss/test", metrics.getIndexDir());

    // *******************************************
    // * Test indirectly through SolrArtifactIndex
    // *******************************************

    // Q: Should this move to TestSolrArtifactIndex instead?

    SolrArtifactIndex index = new SolrArtifactIndex(solrClient, "lockss-repo");
    StorageInfo info = index.getStorageInfo();

    log.trace("StorageInfo = {}", info);

    assertEquals(1L, info.getSizeKB());
    assertEquals(6L, info.getAvailKB());
    assertEquals(4L, info.getUsedKB());
    assertEquals("/lockss/test", info.getName());
  }

  @Test
  public void testNodeMetrics() throws Exception {
    // ***************************************
    // * Setup test node metrics response data
    // ***************************************

    SimpleOrderedMap nodeMetrics = new SimpleOrderedMap();
    nodeMetrics.add("CONTAINER.fs.totalSpace", 1234L);
    nodeMetrics.add("CONTAINER.fs.usableSpace", 5678L);

    NamedList nodeMetricsMap = new NamedList();
    nodeMetricsMap.add("solr.node", nodeMetrics);

    NamedList nodeMetricsResponseData = new NamedList();
    nodeMetricsResponseData.add("metrics", nodeMetricsMap);

    // ******************************
    // * Setup MockServer expectation
    // ******************************

    // Marshal test core metrics response data into javabin binary
    UnsynchronizedByteArrayOutputStream os = new UnsynchronizedByteArrayOutputStream();
    new JavaBinCodec().marshal(nodeMetricsResponseData, os);

    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/solr/admin/metrics")
        )
        .respond(
            response()
                .withStatusCode(200)
                .withHeaders(
                    new Header("Content-Type", "application/octet-stream")
                )
                .withBody(os.toByteArray())
        );

    // ****************************************************************
    // * Test directly using NodeMetricsRequest and NodeMetricsResponse
    // ****************************************************************

    // Get SolrClient from Solr REST endpoint
    HttpSolrClient solrClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl("http://localhost:" + mockServer.getLocalPort() + "/solr")
        .build();

    // Retrieve node metrics from Solr node
    MetricsRequest.NodeMetricsRequest req = new MetricsRequest.NodeMetricsRequest();
    MetricsResponse.NodeMetricsResponse res = req.process(solrClient);
    MetricsResponse.NodeMetrics metrics = res.getNodeMetrics();

    // Debugging
    log.trace("metrics = {}", metrics);

    // Assert metrics contains expected values
    assertEquals(1234L, metrics.getTotalSpace());
    assertEquals(5678L, metrics.getUsableSpace());
  }

  // *******************************************************************************************************************
  // * TESTS FOR INNER CLASSES
  // *******************************************************************************************************************

  /**
   * Tests for {@link MetricsResponse.CoreMetricsResponse}
   */
  @Nested
  public class TestCoreMetricsResponse {

    /**
     * Test for {@link MetricsResponse.CoreMetricsResponse#getSolrCoreKey(String)}.
     *
     * @throws Exception
     */
    @Test
    public void testSolrCoreKey() throws Exception {
      final String testCoreName = "test";
      assertEquals("solr.core." + testCoreName, MetricsResponse.CoreMetricsResponse.getSolrCoreKey(testCoreName));
    }

    /**
     * Test for {@link MetricsResponse.CoreMetricsResponse#getCoreMetrics(String)}
     *
     * @throws Exception
     */
    @Test
    public void testGetCoreMetrics() throws Exception {
      final String testCoreName = "test";

      // Setup mock
      MetricsResponse.CoreMetricsResponse response = mock(MetricsResponse.CoreMetricsResponse.class);
      doCallRealMethod().when(response).getCoreMetrics(ArgumentMatchers.anyString());

      // Assert IllegalStateException thrown if there are no metrics in the response
      when(response.getMetrics()).thenReturn(null);
      assertThrows(IllegalStateException.class, () -> response.getCoreMetrics(testCoreName));

      // Make test metrics response structure
      SimpleOrderedMap coreMetrics = new SimpleOrderedMap();
      coreMetrics.add("CORE.coreName", "lockss-repo");
      coreMetrics.add("CORE.fs.totalSpace", 1234L);
      coreMetrics.add("CORE.fs.usableSpace", 5678L);
      coreMetrics.add("INDEX.sizeInBytes", 4321L);
      coreMetrics.add("CORE.indexDir", "/lockss/test");

      NamedList coreMetricsMap = new NamedList();
      coreMetricsMap.add("solr.core.lockss-repo", coreMetrics);

//      NamedList coreMetricsResponseData = new NamedList();
//      coreMetricsResponseData.add("metrics", coreMetricsMap);

      // Mock response metrics
      NamedList mockedMetricsResponse = mock(NamedList.class);
      when(response.getMetrics()).thenReturn(mockedMetricsResponse);
      when(mockedMetricsResponse.get(MetricsResponse.CoreMetricsResponse.getSolrCoreKey(testCoreName)))
          .thenReturn(coreMetrics);

      // Get the NodeMetrics object representing this node metrics response
      MetricsResponse.CoreMetrics metrics = response.getCoreMetrics(testCoreName);
      assertNotNull(metrics);

      // Assert metrics contains expected values
      assertEquals(1234L, metrics.getTotalSpace());
      assertEquals(5678L, metrics.getUsableSpace());
      assertEquals(4321L, metrics.getIndexSizeInBytes());
      assertEquals("/lockss/test", metrics.getIndexDir());

      MetricsResponse.CoreMetrics expectedMetrics = new MetricsResponse.CoreMetrics(coreMetrics);
      assertEquals(expectedMetrics, coreMetrics);
    }
  }

  /**
   * Tests for {@link MetricsResponse.CoreMetricsResponse}
   */
  @Nested
  public class TestCoreMetrics {
    /**
     * Test for {@link MetricsResponse.CoreMetrics#getIndexDir()}
     *
     * @throws Exception
     */
    @Test
    public void testGetIndexDir() throws Exception {
      MetricsResponse.CoreMetrics metrics = mock(MetricsResponse.CoreMetrics.class);
      doCallRealMethod().when(metrics).getIndexDir();
      when(metrics.findRecursive("CORE.indexDir")).thenReturn("test");
      assertEquals("test", metrics.getIndexDir());
    }

    /**
     * Test for {@link MetricsResponse.CoreMetrics#getIndexSizeInBytes()}
     *
     * @throws Exception
     */
    @Test
    public void testGetIndexSizeInBytes() throws Exception {
      MetricsResponse.CoreMetrics metrics = mock(MetricsResponse.CoreMetrics.class);
      doCallRealMethod().when(metrics).getIndexSizeInBytes();
      when(metrics.findRecursive("INDEX.sizeInBytes")).thenReturn(1234L);
      assertEquals(1234L, metrics.getIndexSizeInBytes());
    }

    /**
     * Test for {@link MetricsResponse.CoreMetrics#getTotalSpace()}
     *
     * @throws Exception
     */
    @Test
    public void testGetTotalSpace() throws Exception {
      MetricsResponse.CoreMetrics metrics = mock(MetricsResponse.CoreMetrics.class);
      doCallRealMethod().when(metrics).getTotalSpace();
      when(metrics.findRecursive("CORE.fs.totalSpace")).thenReturn(1234L);
      assertEquals(1234L, metrics.getTotalSpace());
    }

    /**
     * Test for {@link MetricsResponse.CoreMetrics#getUsableSpace()}
     *
     * @throws Exception
     */
    @Test
    public void testGetUsableSpace() throws Exception {
      MetricsResponse.CoreMetrics metrics = mock(MetricsResponse.CoreMetrics.class);
      doCallRealMethod().when(metrics).getUsableSpace();
      when(metrics.findRecursive("CORE.fs.usableSpace")).thenReturn(1234L);
      assertEquals(1234L, metrics.getUsableSpace());
    }
  }

  /**
   * Test for {@link MetricsResponse.NodeMetricsResponse}
   */
  @Nested
  public class TestNodeMetricsResponse {

    /**
     * Test for {@link MetricsResponse.NodeMetricsResponse#getNodeMetrics()}
     *
     * @throws Exception
     */
    @Test
    public void testGetNodeMetrics() throws Exception {
      // Setup mock
      MetricsResponse.NodeMetricsResponse response = mock(MetricsResponse.NodeMetricsResponse.class);
      doCallRealMethod().when(response).getNodeMetrics();

      // Assert IllegalStateException thrown if there are no metrics in the response
      when(response.getMetrics()).thenReturn(null);
      assertThrows(IllegalStateException.class, () -> response.getNodeMetrics());

      // Make test metrics response structure
      SimpleOrderedMap nodeMetrics = new SimpleOrderedMap();
      nodeMetrics.add("CONTAINER.fs.totalSpace", 1234L);
      nodeMetrics.add("CONTAINER.fs.usableSpace", 5678L);

      SimpleOrderedMap nodeMetricsMap = new SimpleOrderedMap();
      nodeMetricsMap.add(MetricsResponse.NodeMetricsResponse.SOLR_NODE_KEY, nodeMetrics);

      // Mock response metrics
      NamedList mockedMetricsResponse = mock(NamedList.class);
      when(response.getMetrics()).thenReturn(mockedMetricsResponse);
      when(mockedMetricsResponse.get(MetricsResponse.NodeMetricsResponse.SOLR_NODE_KEY))
          .thenReturn(nodeMetrics);

      // Get the NodeMetrics object representing this node metrics response
      MetricsResponse.NodeMetrics metrics = response.getNodeMetrics();
      assertNotNull(metrics);
      assertEquals(1234L, metrics.getTotalSpace());
      assertEquals(5678L, metrics.getUsableSpace());

      MetricsResponse.NodeMetrics expectedMetrics = new MetricsResponse.NodeMetrics(nodeMetrics);
      assertEquals(expectedMetrics, nodeMetrics);
    }
  }

  /**
   * Test for {@link MetricsResponse.NodeMetrics}
   */
  @Nested
  public class TestNodeMetrics {
    @Test
    public void testNodeMetricsConstructor() {
      // Assert IllegalArgumentException thrown if construction attempted with null node metrics
      assertThrows(IllegalArgumentException.class, () -> new MetricsResponse.NodeMetrics(null));

      // Make test metrics response structure
      SimpleOrderedMap nodeMetrics = new SimpleOrderedMap();
      nodeMetrics.add("CONTAINER.fs.totalSpace", 1234L);
      nodeMetrics.add("CONTAINER.fs.usableSpace", 5678L);

      MetricsResponse.NodeMetrics metrics = new MetricsResponse.NodeMetrics(nodeMetrics);

      assertEquals(1234L, metrics.getTotalSpace());
      assertEquals(5678L, metrics.getUsableSpace());
    }

    /**
     * Test for {@link MetricsResponse.NodeMetrics#getTotalSpace()}
     *
     * @throws Exception
     */
    @Test
    public void testGetTotalSpace() throws Exception {
      MetricsResponse.NodeMetrics metrics = mock(MetricsResponse.NodeMetrics.class);
      doCallRealMethod().when(metrics).getTotalSpace();
      when(metrics.findRecursive("CONTAINER.fs.totalSpace")).thenReturn(1234L);
      assertEquals(1234L, metrics.getTotalSpace());
    }

    /**
     * Test for {@link MetricsResponse.NodeMetrics#getUsableSpace()}
     *
     * @throws Exception
     */
    @Test
    public void testGetUsableSpace() throws Exception {
      MetricsResponse.NodeMetrics metrics = mock(MetricsResponse.NodeMetrics.class);
      doCallRealMethod().when(metrics).getUsableSpace();
      when(metrics.findRecursive("CONTAINER.fs.usableSpace")).thenReturn(5678L);
      assertEquals(5678L, metrics.getUsableSpace());
    }
  }
}
