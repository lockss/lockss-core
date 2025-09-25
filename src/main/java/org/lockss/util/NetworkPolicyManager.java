/*

    Copyright (c) 2017-2020 Board of Trustees of Leland Stanford Jr. University,
    all rights reserved.

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
    STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
    IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

    Except as contained in this notice, the name of Stanford University shall not
    be used in advertising or otherwise to promote the sale, use or other dealings
    in this Software without prior written authorization from Stanford University.

    */
package org.lockss.util;

import inet.ipaddr.AddressStringException;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.*;
import org.lockss.app.BaseLockssManager;
import org.lockss.app.ConfigurableManager;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.log.L4JLogger;
import org.lockss.proxy.ProxyManager;
import org.lockss.servlet.AdminServletManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;

public class NetworkPolicyManager extends BaseLockssManager implements ConfigurableManager {

  private final L4JLogger log = L4JLogger.getLogger();
  static final String PREFIX = Configuration.PREFIX + "networkPolicy";
  // our network policy parameters
  final static String PARAM_LOCKSS_PROTECTED_ADMIN_PORTS = NetworkPolicyManager.PREFIX + ".protected.adminPorts";
  static final String DEFAULT_LOCKSS_PROTECTED_ADMIN_PORTS = "24602";

  final static String PARAM_LOCKSS_PROTECTED_CONTENT_PORTS = NetworkPolicyManager.PREFIX + ".protected.contentPorts";
  static final String DEFAULT_LOCKSS_PROTECTED_CONTENT_PORTS = "8080;24681";

  static final String PARAM_POLICY_FILE = NetworkPolicyManager.PREFIX + ".policyFile";
  static final String DEFAULT_POLICY_FILE = "lockss-network-policy.yaml";

  private static final String NETWORK_POLICY_NAME_CONTENT_ACCESS = "lockss-network-policy-content-access";
  private static final String NETWORK_POLICY_NAME_ADMIN_ACCESS = "lockss-network-policy-admin-access";
  private static final String K8S_NAMESPACE_LOCKSS = "lockss";
  protected static final String K8S_API_VERSION = "networking.k8s.io/v1";
  private static final String LABEL_SERVICE_KIND = "service-kind";
  private static final String LABEL_VALUE_NON_LOCKSS = "non-lockss";
  private static final java.util.List<String> POLICY_TYPES_INGRESS = Collections.singletonList("Ingress");

  protected String managedAdminPorts = NetworkPolicyManager.DEFAULT_LOCKSS_PROTECTED_ADMIN_PORTS;
  protected String managedContentPorts = NetworkPolicyManager.DEFAULT_LOCKSS_PROTECTED_CONTENT_PORTS;

  // Enable test mode that skips talking to a live Kubernetes cluster
  protected boolean dryRun;
  protected String policyFileName = NetworkPolicyManager.DEFAULT_POLICY_FILE;

  private final ExecutorService ingressUpdateExecutor =
      new ThreadPoolExecutor(
          1, 1,
          0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>());


  public NetworkPolicyManager() {
  }

  @Override
  public void stopService() {
    try {
      this.ingressUpdateExecutor.shutdown();
      if (!this.ingressUpdateExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        this.ingressUpdateExecutor.shutdownNow();
      }
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
      this.ingressUpdateExecutor.shutdownNow();
    }
    super.stopService();
  }

  public void setConfig(final org.lockss.config.Configuration config,
                        final org.lockss.config.Configuration oldConfig,
                        final org.lockss.config.Configuration.Differences diffs) {
    try {
      if (ConfigManager.getPlatformVersion().isKubernetes()) {
        this.queueConfigChanges(config, diffs);
      }
      // Update dry-run mode whenever configuration changes affect this manager
      if (diffs.contains(NetworkPolicyManager.PREFIX)) {
        if (this.dryRun) {
          this.log.info("NetworkPolicy dry-run mode enabled: will not apply changes to Kubernetes");
        } else {
          this.log.debug("NetworkPolicy dry-run mode disabled: will apply changes to Kubernetes");
        }
        policyFileName = config.get(NetworkPolicyManager.PARAM_POLICY_FILE,
            NetworkPolicyManager.DEFAULT_POLICY_FILE);
      }
    } catch (final Exception ex) {
      this.log.error("Error processing configuration update", ex);
    }
  }

  void queueConfigChanges(final Configuration config, final Configuration.Differences diffs) {
    if (diffs.contains(NetworkPolicyManager.PARAM_LOCKSS_PROTECTED_ADMIN_PORTS) ||
        diffs.contains(NetworkPolicyManager.PARAM_LOCKSS_PROTECTED_CONTENT_PORTS) ||
        diffs.contains(AdminServletManager.PARAM_IP_INCLUDE) ||
        diffs.contains(AdminServletManager.PARAM_IP_EXCLUDE)) {

      if (diffs.contains(NetworkPolicyManager.PARAM_LOCKSS_PROTECTED_ADMIN_PORTS)) {
        // we changed a protected admin port
        this.managedAdminPorts = config.get(NetworkPolicyManager.PARAM_LOCKSS_PROTECTED_ADMIN_PORTS,
            NetworkPolicyManager.DEFAULT_LOCKSS_PROTECTED_ADMIN_PORTS);
      }

      if (diffs.contains(NetworkPolicyManager.PARAM_LOCKSS_PROTECTED_CONTENT_PORTS)) {
        // we changed a protected content port
        this.managedContentPorts = config.get(NetworkPolicyManager.PARAM_LOCKSS_PROTECTED_CONTENT_PORTS,
            NetworkPolicyManager.DEFAULT_LOCKSS_PROTECTED_CONTENT_PORTS);
      }

      // enqueue the update to be processed by a single background thread
      this.ingressUpdateExecutor.submit(() -> {
        try {
          // Admin access network policy
          V1NetworkPolicy adminAccessNetworkPolicy =
              this.generateUpdatedNetworkPolicy(
                  K8S_NAMESPACE_LOCKSS,
                  NETWORK_POLICY_NAME_ADMIN_ACCESS,
                  config.getList(AdminServletManager.PARAM_IP_INCLUDE),
                  config.getList(AdminServletManager.PARAM_IP_EXCLUDE),
                  managedAdminPorts);

          // Content access network policy
          V1NetworkPolicy contentAccessNetworkPolicy =
              this.generateUpdatedNetworkPolicy(
                  K8S_NAMESPACE_LOCKSS,
                  NETWORK_POLICY_NAME_CONTENT_ACCESS,
                  config.getList(ProxyManager.PARAM_IP_INCLUDE),
                  config.getList(ProxyManager.PARAM_IP_EXCLUDE),
                  managedContentPorts);

          this.writeAndApplyNetworkPolicy(policyFileName, contentAccessNetworkPolicy, adminAccessNetworkPolicy);
        } catch (final Throwable t) {
          this.log.warn("Error running queued updateNetworkPolicyIngress task", t);
        }
      });
    }
  }

  /**
   * Build and persist a NetworkPolicy ingress section based on the include/exclude IP filters.
   * Allows specifying an alternate output filename; if null/blank, defaults to K8S_OUTPUT_FILENAME.
   */
  void writeAndApplyNetworkPolicy(final String outFilename, V1NetworkPolicy... policies) {
    String outputPath = outFilename;
    if (outFilename == null || outFilename.isBlank()) {
      if (this.dryRun) {
        try {
          final Path tmp = Files.createTempFile("lockss-network-policy-", ".yaml");
          outputPath = tmp.toAbsolutePath().toString();
          this.log.info("Dry-run: writing NetworkPolicy YAML to temp file: " + outputPath);
        } catch (IOException e) {
          this.log.warn("Error creating temp file for dry-run NetworkPolicy YAML", e);
          return;
        }
      }
    }

    if (outputPath == null || outputPath.isBlank()) {
      throw new IllegalArgumentException("Null or blank outputPath");
    }

    try {
      this.writePolicyToFile(outputPath, policies);

      List<String> policyNames = Arrays.stream(policies)
          .map(V1NetworkPolicy::getMetadata)
          .filter(Objects::nonNull)
          .map(V1ObjectMeta::getNamespace)
          .toList();

      this.log.info("Wrote updated NetworkPolicy files for " + policyNames
          + " in namespace '" + K8S_NAMESPACE_LOCKSS + "'"
          + " to " + outputPath);

      if (!this.dryRun) {
        this.applyNetworkPolicyToCluster(policies);
      }
    } catch (final Exception e) {
      this.log.warn("Error while preparing Kubernetes NetworkPolicy for access control", e);
    }
  }

  V1NetworkPolicy generateUpdatedNetworkPolicy(String namespace,
                                                       String policyName,
                                                       List<String> includeFilters,
                                                       List<String> excludeFilters,
                                                       String managedPorts)
      throws IOException, ApiException, AddressStringException {

    final List<String> allowedCidrs = this.toCidrList(includeFilters);
    final List<String> deniedCidrs = this.toCidrList(excludeFilters);

    log.debug2("includeFilters: {}, toCidrList(includeFilters): {}", includeFilters, allowedCidrs);
    log.debug2("excludeFilters: {}, toCidrList(excludeFilters): {}", excludeFilters, deniedCidrs);

    if (allowedCidrs.isEmpty()) {
      // xxx this should never be empty.
      this.log.info(
          "No allowed CIDRs derived from include list; skipping Kubernetes access policy preparation");
      // FIXME: Should this throw instead?
      return null;
    }

    // Find existing policy or create a default one; ensure spec exists
    final V1NetworkPolicy networkPolicy =
        this.findExistingPolicyorCreate(policyName, namespace);

    this.ensureSpecWithDefaults(networkPolicy);

    // Build ingress rules
    final List<V1NetworkPolicyIngressRule> ingressRules = new ArrayList<>();

    // Always allow from any pod (podSelector: {})
    ingressRules.add(new V1NetworkPolicyIngressRule()
        .from(Collections.singletonList(new V1NetworkPolicyPeer().podSelector(
            this.anyPodSelector()))));

    // Build ports from current managedPorts
    final List<V1NetworkPolicyPort> ports = this.buildPorts(managedPorts);

    // One rule per allowed CIDR, with optional except list and the fixed ports
    for (final String cidr : allowedCidrs) {
      final V1IPBlock block = this.buildIpBlock(cidr, deniedCidrs);
      final V1NetworkPolicyPeer peer = new V1NetworkPolicyPeer().ipBlock(block);
      final V1NetworkPolicyIngressRule cidrRule = new V1NetworkPolicyIngressRule()
          .from(Collections.singletonList(peer)).ports(ports);
      ingressRules.add(cidrRule);
    }

    networkPolicy.getSpec().setIngress(ingressRules);
    log.debug2("networkPolicy: {}", networkPolicy);

    return networkPolicy;
  }

  /**
   * Convert a list of IP expressions (single IP, wildcard like 10.*.*.*, or CIDR) into CIDR
   * strings.
   */
  List<String> toCidrList(final List<String> ips) {
    final List<String> result = new ArrayList<>();
    if (null == ips) {
      return result;
    }
    ips.stream().filter(raw -> !StringUtil.isNullString(raw)).map(String::trim)
        .filter(s -> !s.isEmpty() && !s.startsWith("#")).forEach(s -> {
          try {
            final IpFilter.Mask mask = IpFilter.newMask(s);
            result.add(mask.toString());
          } catch (final IpFilter.MalformedException ex) {
            this.log.warn(
                MessageFormat.format("Skipping unparsable IP entry for NetworkPolicy: {0} ({1})", s,
                    ex.getMessage()));
          }
        });
    return result;
  }

  /**
   * Retrieve an existing NetworkPolicy or create a default one.
   */
  private V1NetworkPolicy findExistingPolicyorCreate(final String policyName, final String namespace)
      throws ApiException, IOException {
    V1NetworkPolicy existing = K8sClientUtils.readNetworkPolicyOrNull(policyName, namespace);
    if (null == existing) {
      this.log.info("NetworkPolicy '" + policyName + "' not found in namespace '" + namespace
          + "'; creating default.");
      existing = new V1NetworkPolicy();
      final V1ObjectMeta metadata = new V1ObjectMeta();
      metadata.setName(policyName);
      metadata.setNamespace(namespace);
      existing.setMetadata(metadata);
      existing.setApiVersion(NetworkPolicyManager.K8S_API_VERSION);
      existing.setKind("NetworkPolicy");
    }
    return existing;
  }

  /**
   * Ensure a NetworkPolicy spec exists, with sane defaults.
   */
  private void ensureSpecWithDefaults(final V1NetworkPolicy policy) {
    if (null == policy.getSpec()) {
      this.log.info("NetworkPolicy '" + policy.getMetadata().getName() + "' in namespace '"
          + policy.getMetadata().getNamespace() + "' has null spec; setting defaults.");

      final V1NetworkPolicySpec spec = new V1NetworkPolicySpec();
      spec.setPolicyTypes(NetworkPolicyManager.POLICY_TYPES_INGRESS);

      // Empty selector means "any pod"
      spec.setPodSelector(this.anyPodSelector());
      policy.setSpec(spec);
    }
  }


  /**
   * Create or replace the given NetworkPolicy in the running Kubernetes cluster. If it doesn't
   * exist, it will be created; otherwise, it is replaced.
   */
  void applyNetworkPolicyToCluster(final V1NetworkPolicy... policies) {
    if (policies == null || policies.length == 0) {
      this.log.warn("No NetworkPolicy to apply: null or empty policies array");
      return;
    }

    for (final V1NetworkPolicy policy : policies) {
      V1ObjectMeta policyMetadata = policy.getMetadata();
      assert policyMetadata != null;

      String name = policyMetadata.getName();
      String namespace = policyMetadata.getNamespace();

      if (null == name || null == namespace) {
        this.log.warn("Cannot apply NetworkPolicy: name or namespace is null");
        return;
      }

      try {
        final K8sClientUtils.ApplyResult result =
            K8sClientUtils.createOrReplaceNetworkPolicy(policy);
        if (K8sClientUtils.ApplyResult.REPLACED == result) {
          this.log.info("Replaced NetworkPolicy '" + name + "' in namespace '" + namespace + "'");
        } else if (K8sClientUtils.ApplyResult.CREATED == result) {
          this.log.info("Created NetworkPolicy '" + name + "' in namespace '" + namespace + "'");
        } else {
          this.log.info("No change for NetworkPolicy '" + name + "' in namespace '" + namespace + "'");
        }
      } catch (final Exception e) {
        this.log.warn("Failed to apply NetworkPolicy to cluster", e);
      }
    }
  }

  /**
   * Serialize policy as YAML and write to a file for later application (e.g., kubectl apply -f).
   */
  void writePolicyToFile(final String filename, final V1NetworkPolicy... policies) throws IOException {
    K8sClientUtils.writeNetworkPolicyToFile(filename, policies);
  }

  private V1IPBlock buildIpBlock(final String cidr, final List<String> deniedCidrs) throws AddressStringException {
    List<String> intersectedDeniedCidrs = getCidrIntersection(cidr, deniedCidrs);

    final V1IPBlock block = new V1IPBlock().cidr(cidr);
    if (!intersectedDeniedCidrs.isEmpty()) {
      block.setExcept(intersectedDeniedCidrs);
    }
    return block;
  }

  public static List<String> getCidrIntersection(final String cidr, final List<String> deniedCidrs)
      throws AddressStringException {

    if (deniedCidrs == null || deniedCidrs.isEmpty()) {
      return Collections.emptyList();
    }

    IPAddress cidrAddr = new IPAddressString(cidr).toAddress();
    List<String> intersectedDeniedCidrs = new ArrayList<>();

    for (final String deniedCidr : deniedCidrs) {
      IPAddress deniedCidrAddr = new IPAddressString(deniedCidr).toAddress();
      IPAddress cidrIntersection = cidrAddr.intersect(deniedCidrAddr);
      if (cidrIntersection != null) {
        intersectedDeniedCidrs.add(cidrIntersection.toString());
      }
    }

    return intersectedDeniedCidrs;
  }

  private V1LabelSelector anyPodSelector() {
    // Empty selector means "any pod"
    return new V1LabelSelector();
  }

  List<V1NetworkPolicyPort> buildPorts(final String managedPorts) {
    if (null == managedPorts || managedPorts.isBlank()) {
      throw new IllegalArgumentException("Null or blank managedPorts");
    }

    final java.util.LinkedHashSet<Integer> uniquePorts =
        Arrays.stream(managedPorts.split(";", -1)) // keep empties if any
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> {
              final int port;
              try {
                port = Integer.parseInt(s);
              } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port: '" + s + "'");
              }
              if (1 > port || 65535 < port) {
                throw new IllegalArgumentException("Port out of range (1-65535): " + port);
              }
              return Integer.valueOf(port);
            })
            .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));

    if (uniquePorts.isEmpty()) {
      throw new IllegalArgumentException("No valid ports in managedPorts");
    }

    return uniquePorts.stream()
        .map(p -> new V1NetworkPolicyPort().port(new IntOrString(p)))
        .collect(java.util.stream.Collectors.toList());
  }
}
