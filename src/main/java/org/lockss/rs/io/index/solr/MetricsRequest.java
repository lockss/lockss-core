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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.lockss.log.L4JLogger;

/**
 * Solr Metrics API request class.
 *
 * @param <T> extends {@link MetricsResponse}
 */
public abstract class MetricsRequest<T extends MetricsResponse> extends SolrRequest<T> {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Metrics API query parameter keys.
   */
  public static final String METRICS_PARAM_GROUP = "group";
  public static final String METRICS_PARAM_TYPE = "type";
  public static final String METRICS_PARAM_PREFIX = "prefix";
  public static final String METRICS_PARAM_REGEX = "regex";
  public static final String METRICS_PARAM_PROPERTY = "property";
  public static final String METRICS_PARAM_KEY = "key";

  /**
   * Metrics groups available through the Metrics API. Default is all.
   */
  public enum MetricsGroups {
    all,
    jvm,
    jetty,
    node,
    core
  }

  /**
   * Metrics types available through the Metrics API. Default is all.
   */
  public enum MetricsTypes {
    all,
    counter,
    gauge,
    histogram,
    meter,
    timer
  }

  /**
   * Metrics API query parameters.
   */
  protected MetricsGroups group = MetricsGroups.all;
  protected MetricsTypes type = MetricsTypes.all;
  protected String prefix;
  protected String regex;
  protected String property;
  protected String key;

  /**
   * Base constructor pointing to endpoint of Metrics API.
   */
  public MetricsRequest() {
    super(METHOD.GET, "/admin/metrics");
  }

  /**
   * Returns the parameters of this Metrics API request.
   *
   * @return A {@link SolrParams} containing the parameters of this Metrics API request.
   */
  @Override
  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(METRICS_PARAM_GROUP, group.toString());
    params.set(METRICS_PARAM_TYPE, type.toString());
    params.set(METRICS_PARAM_PREFIX, prefix);
    params.set(METRICS_PARAM_REGEX, regex);
    params.set(METRICS_PARAM_PROPERTY, property);
    params.set(METRICS_PARAM_KEY, key);

    return params;
  }

  /**
   * A subclass of {@link MetricsRequest} for "core" group metrics requests.
   */
  public static class CoreMetricsRequest extends MetricsRequest<MetricsResponse.CoreMetricsResponse> {
    /**
     * Constructor.
     */
    public CoreMetricsRequest() {
      this.group = MetricsGroups.core;
    }

    /**
     * Creates a new {@link MetricsResponse.CoreMetricsResponse} to contain the "core" group metrics from this Metrics
     * API request.
     *
     * @param client A {@link SolrClient} to a Solr node.
     * @return A new {@link MetricsResponse.CoreMetricsResponse} instance.
     */
    @Override
    protected MetricsResponse.CoreMetricsResponse createResponse(SolrClient client) {
      return new MetricsResponse.CoreMetricsResponse();
    }
  }

  /**
   * A subclass of {@link MetricsRequest} for "node" group metrics requests.
   */
  public static class NodeMetricsRequest extends MetricsRequest<MetricsResponse.NodeMetricsResponse> {
    /**
     * Constructor.
     */
    public NodeMetricsRequest() {
      this.group = MetricsGroups.node;
    }

    /**
     * Creates a new {@link MetricsResponse.NodeMetricsResponse} to contain the "node" group metrics from this Metrics
     * API request.
     *
     * @param client A {@link SolrClient} to a Solr node.
     * @return A new {@link MetricsResponse.NodeMetricsResponse} instance.
     */
    @Override
    protected MetricsResponse.NodeMetricsResponse createResponse(SolrClient client) {
      return new MetricsResponse.NodeMetricsResponse();
    }
  }
}
