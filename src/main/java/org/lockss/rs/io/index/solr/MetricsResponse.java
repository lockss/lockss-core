package org.lockss.rs.io.index.solr;

import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.util.NamedList;
import org.lockss.log.L4JLogger;

/**
 * Solr Metrics API response class.
 */
public class MetricsResponse extends SolrResponseBase {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final static String SOLR_METRICS_KEY = "metrics";

  /**
   * Returns the metrics contained in this Metrics API response.
   *
   * @return A {@link NamedList} containing the metrics from this Metrics API response.
   */
  public NamedList getMetrics() {
    return (NamedList) getResponse().get(SOLR_METRICS_KEY);
  }

  /**
   * Metrics API response for "core" group metrics returned by {@link MetricsRequest.CoreMetricsRequest}.
   */
  public static class CoreMetricsResponse extends MetricsResponse {
    private final static String SOLR_CORE_KEY_PREFIX = "solr.core";

    /**
     * Generates the key to a Solr core's metrics, provided its name.
     *
     * @param name A {@link String} containing the name of the Solr core.
     * @return A {@link String} containing the Solr core key.
     */
    public static String getSolrCoreKey(String name) {
      return String.format("%s.%s", SOLR_CORE_KEY_PREFIX, name);
    }

    /**
     * Returns the metrics of a Solr core, provided its name.
     *
     * @param coreName A {@link String} containing the name of the Solr core.
     * @return A {@link CoreMetrics} containing the metrics of the Solr core.
     */
    public CoreMetrics getCoreMetrics(String coreName) {
      if (coreName == null) {
        throw new IllegalArgumentException("Null core name");
      }

      NamedList metrics = getMetrics();

      if (metrics == null) {
        throw new IllegalStateException("No metrics in response");
      }

      return new CoreMetrics((NamedList) metrics.get(getSolrCoreKey(coreName)));
    }
  }

  /**
   * A subclass of {@link NamedList} to wrap the metrics of a single Solr core.
   */
  public static class CoreMetrics extends NamedList {
    /**
     * Constructor.
     *
     * @param coreMetrics A {@link NamedList} containing the metrics of a single Solr core.
     */
    public CoreMetrics(NamedList coreMetrics) {
      this.addAll(coreMetrics);
    }

    /**
     * Returns the total space (in bytes) of the filesystem that the Solr core resides on.
     *
     * @return A {@code long} containing the total space.
     */
    public long getTotalSpace() {
      return (long) findRecursive("CORE.fs.totalSpace");
    }

    /**
     * Returns the usable space (in bytes) remaining in the filesystem that the Solr core resides on.
     *
     * @return A {@code long} containing the usable space.
     */
    public long getUsableSpace() {
      return (long) findRecursive("CORE.fs.usableSpace");
    }

    /**
     * Returns the size of the Solr core's index (in bytes).
     *
     * @return A {@code long} containing the size of the index.
     */
    public long getIndexSizeInBytes() {
      return (long) findRecursive("INDEX.sizeInBytes");
    }

    /**
     * Returns the path to the Solr core's index on the system running the Solr node (i.e., server).
     *
     * @return A {@link String} containing the path to the Solr core's index.
     */
    public String getIndexDir() {
      return (String) findRecursive("CORE.indexDir");
    }
  }

  /**
   * Metrics API response for "node" group metrics returned by {@link MetricsRequest.NodeMetricsRequest}.
   */
  public static class NodeMetricsResponse extends MetricsResponse {
    public final static String SOLR_NODE_KEY = "solr.node";

    /**
     * Returns the Solr node metrics.
     *
     * @return A {@link NodeMetrics} containing the Solr node metrics.
     */
    public NodeMetrics getNodeMetrics() {
      NamedList metrics = getMetrics();

      if (metrics == null) {
        throw new IllegalStateException("No metrics in response");
      }

      return new NodeMetrics((NamedList) metrics.get(SOLR_NODE_KEY));
    }
  }

  /**
   * A subclass of {@link NamedList} to wrap Solr node metrics.
   */
  public static class NodeMetrics extends NamedList {
    /**
     * Constructor.
     *
     * @param nodeMetrics A {@link NamedList} containing Solr node metrics.
     */
    public NodeMetrics(NamedList nodeMetrics) {
      if (nodeMetrics == null) {
        throw new IllegalArgumentException("Null node metrics");
      }

      this.addAll(nodeMetrics);
    }

    /**
     * Returns the total space (in bytes) of the filesystem in use by the Solr node's core container.
     *
     * @return A {@code long} containing the total space in bytes.
     */
    public long getTotalSpace() {
      return (long) findRecursive("CONTAINER.fs.totalSpace");
    }

    /**
     * Returns the usable space (in bytes) remaining in the filesystem in use by the Solr node's core container.
     *
     * @return A {@code long} containing the usable space in bytes.
     */
    public long getUsableSpace() {
      return (long) findRecursive("CONTAINER.fs.usableSpace");
    }
  }
}
