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

import org.apache.solr.client.solrj.response.SolrResponseBase;

/**
 * Exception wrapper for a Solr response that reports errors.
 * 
 * @version 1.0
 */
@SuppressWarnings("serial")
public class SolrResponseErrorException extends Exception {

  // The status provided in the Solr response.
  private int solrStatus = -1;

  // The message included in the Solr response.
  private String solrMessage = null;

  /**
   * Constructor with the Solr response to be wrapped.
   * 
   * @param solrResponse A SolrResponseBase with the Solr response to be
   *                     wrapped.
   */
  public SolrResponseErrorException(SolrResponseBase solrResponse) {
    super();
    setSolrStatus(solrResponse.getStatus());
    setSolrMessage(solrResponse.getResponse().toString());
  }

  /**
   * Constructor with the Solr response to be wrapped and a custom message.
   * 
   * @param message A String with the custom message.
   * @param solrResponse A SolrResponseBase with the Solr response to be
   *                     wrapped.
   */
  public SolrResponseErrorException(String message,
                                    SolrResponseBase solrResponse) {
    super(message);
    setSolrStatus(solrResponse.getStatus());
    setSolrMessage(solrResponse.getResponse().toString());
  }

  /**
   * Provides the Solr response status code.
   * 
   * @return an int with the Solr response status code.
   */
  public int getSolrStatus() {
    return solrStatus;
  }

  /**
   * Saves the Solr response status code.
   * 
   * @param solrStatus An int with the Solr response status code.
   */
  private void setSolrStatus(int solrStatus) {
    this.solrStatus = solrStatus;
  }

  /**
   * Provides the Solr response message.
   * 
   * @return a String with the Solr response message.
   */
  public String getSolrMessage() {
    return solrMessage;
  }

  /**
   * Saves the Solr response message.
   * 
   * @param solrMessage A String with the Solr response message.
   */
  private void setSolrMessage(String solrMessage) {
    this.solrMessage = solrMessage;
  }

  @Override
  public String toString() {
    return "[SorlNonZeroStatusException: " + getMessage() + ": solrStatus="
	+ solrStatus + ", solrMessage=" + solrMessage + "]";
  }
}
