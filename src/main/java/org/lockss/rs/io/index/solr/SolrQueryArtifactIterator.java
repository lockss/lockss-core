/*

Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.rs.io.index.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CursorMarkParams;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.log.L4JLogger;

/**
 * Artifact iterator that wraps a Solr query response.
 * 
 * @version 1.0
 */
public class SolrQueryArtifactIterator implements Iterator<Artifact> {
  private final static L4JLogger log = L4JLogger.getLogger();

  // The Solr client used to query Solr.
  private final SolrClient solrClient;

  // Solr BasicAuth credentials to use with Solr requests
  private List<String> solrCredentials;

  // The Solr query used to obtain artifacts from Solr.
  private final SolrQuery solrQuery;

  private final String solrCollection;

  // The internal buffer used to store locally the artifacts provides by Solr.
  private List<ArtifactSolrDocument> artifactBuffer = null;

  // An iterator to the internal buffer.
  private Iterator<ArtifactSolrDocument> artifactBufferIterator = null;

  // Position for the Solr query.
  private String cursorMark = CursorMarkParams.CURSOR_MARK_START;

  // Indication of whether Solr has returned all the query results already.
  boolean isLastBatch = false;

  /**
   * Constructor with default batch size.
   * 
   * @param solrClient A SolrClient used to query Solr.
   * @param solrQuery  A SolrQuery used to obtain artifacts from Solr.
   */
  public SolrQueryArtifactIterator(String collection, SolrClient solrClient,
                                   List<String> solrCredentials, SolrQuery solrQuery) {
    this(collection, solrClient, solrCredentials, solrQuery, 10);
  }

  /**
   * Full constructor.
   * 
   * @param solrClient A SolrClient used to query Solr.
   * @param solrQuery  A SolrQuery used to obtain artifacts from Solr.
   * @param batchSize  An int with the number of artifacts to request on each
   *                   Solr query.
   */
  public SolrQueryArtifactIterator(String collection, SolrClient solrClient,
                                   List<String> solrCredentials, SolrQuery solrQuery,
      int batchSize) {
    // Validation.
    if (solrClient == null) {
      throw new IllegalArgumentException("SolrClient cannot be null");
    }

    if (solrQuery == null) {
      throw new IllegalArgumentException("SolrQuery cannot be null");
    }

    if (batchSize < 1) {
      throw new IllegalArgumentException("Batch size must be at least 1");
    }

    // Initialization.
    this.solrCollection = collection;
    this.solrClient = solrClient;
    this.solrCredentials = solrCredentials;
    this.solrQuery = solrQuery;
    artifactBuffer = new ArrayList<>(batchSize);
    artifactBufferIterator = artifactBuffer.iterator();

    // Set paging parameters.
    solrQuery.setRows(batchSize);
    solrQuery.addSort(SolrQuery.SortClause.asc("id"));
  }

  /**
   * Returns {@code true} if the there are still artifacts provided by Solr that
   * have not been returned to the client already.
   *
   * @return a boolean with {@code true} if there are more artifacts to be
   *         returned, {@code false} otherwise.
   */
  @Override
  public boolean hasNext() throws SolrRuntimeException {
    log.debug2("Invoked");
    log.trace("artifactBatchIterator.hasNext() = {}",
	artifactBufferIterator.hasNext());
    log.trace("isLastBatch = {}", isLastBatch);

    boolean hasNext = false;

    // Check whether the internal buffer still has artifacts.
    if (artifactBufferIterator.hasNext()) {
      // Yes: The answer is {@code true}.
      hasNext = true;
      // No: Check whether the current batch is the last one.
    } else if (isLastBatch) {
      // Yes: The answer is {@code false}.
      hasNext = false;
    } else {
      // No: Fill the internal buffer with another batch from Solr.
      try {
	fillArtifactBuffer();
      } catch (SolrResponseErrorException | SolrServerException | IOException e)
      {
	throw new SolrRuntimeException(e);
      }

      // Yes: The answer is determined by the contents of the internal buffer.
      hasNext = artifactBufferIterator.hasNext();
    }

    log.debug2("hasNext = {}", hasNext);
    return hasNext;
  }

  /**
   * Provides the next artifact.
   *
   * @return an Artifact with the next artifact.
   * @throws NoSuchElementException if there are no more artifacts to return.
   */
  @Override
  public Artifact next() {
    if (hasNext()) {
      return artifactBufferIterator.next().toArtifact();
    } else {
      throw new NoSuchElementException();
    }
  }

  /**
   * Fills the internal buffer with the next batch of artifacts from Solr.
   * 
   * @throws SolrResponseErrorException if Solr reports problems.
   * @throws SolrServerException        if Solr reports problems.
   * @throws IOException                if Solr reports problems.
   */
  private void fillArtifactBuffer()
      throws SolrResponseErrorException, SolrServerException, IOException {
    log.debug2("Invoked");
    log.trace("cursorMark = {}", cursorMark);

    // Set the current position for the query.
    solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);

    // Wrap SolrQuery into a QueryRequest
    QueryRequest request = new QueryRequest(solrQuery);

    // Add Solr BasicAuth credentials if present
    if (solrCredentials != null) {
      request.setBasicAuthCredentials(
          /* Username */ solrCredentials.get(0),
          /* Password */ solrCredentials.get(1)
      );
    }

    // Make the query to Solr.
    QueryResponse response = SolrArtifactIndex.handleSolrResponse(
        SolrArtifactIndex.processSolrRequest(
            solrClient, solrCollection,
            request, SolrArtifactIndex.SOLR_MAX_RETRIES, SolrArtifactIndex.SOLR_RETRY_DELAY),
        "Problem performing Solr query");

    // Get the position for the next query.
    String nextCursorMark = response.getNextCursorMark();
    log.trace("nextCursorMark = {}", nextCursorMark);

    // Populate the internal buffer with the batch of artifacts returned by
    // Solr.
    artifactBuffer = response.getBeans(ArtifactSolrDocument.class);
    log.trace("artifactBatch = {}", artifactBuffer);

    // Check whether the new query position is different than the old one.
    if (!cursorMark.equals(nextCursorMark)) {
      // Yes: Save the position for the next query.
      cursorMark = nextCursorMark;
    } else {
      // No: There are no more batches after this one.
      isLastBatch = true;
    }

    log.trace("isLastBatch = {}", isLastBatch);

    artifactBufferIterator =  artifactBuffer.iterator();
    log.debug2("Done");
  }
}
