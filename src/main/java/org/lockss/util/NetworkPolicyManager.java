package org.lockss.util;

import io.kubernetes.client.custom.*;
import io.kubernetes.client.openapi.*;
import io.kubernetes.client.openapi.models.*;
import java.io.*;
import java.nio.file.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.config.Configuration;
import org.lockss.config.Configuration.*;
import org.lockss.util.IpFilter.*;

public class NetworkPolicyManager extends BaseLockssManager implements ConfigurableManager {

  protected static final String NETWORK_POLICY_TEMPLATE_FILENAME = "lockss-network-policy.yaml";
  private final Logger log = Logger.getLogger();
  static final String PREFIX = Configuration.PREFIX + "networkPolicy";
  // our network policy parameters
  final static String PARAM_LOCKSS_PROTECTED_PORTS = PREFIX +".protected.ports";
  static final String DEFAULT_LOCKSS_PROTECTED_PORTS = "24681;24682;24602";
  static final String PARAM_POLICY_FILE= PREFIX + ".policyFile";
  static final String DEFAULT_POLICY_FILE = "lockss-network-policy.yaml";
  // acceess includes/exclue params
  private static final String PARAM_IP_ACCESS_INCLUDE = "org.lockss.ui.ip.include";
  private static final String PARAM_IP_ACCESS_EXCLUDE = "org.lockss.ui.ip.exclude";

  private static final String EXISTING_POLICY_NAME = "lockss";
  private static final String K8S_NAMESPACE_LOCKSS = "lockss";
  protected static final String K8S_API_VERSION = "networking.k8s.io/v1";
  private static final String LABEL_SERVICE_KIND = "service-kind";
  private static final String LABEL_VALUE_NON_LOCKSS = "non-lockss";
  private static final java.util.List<String> POLICY_TYPES_INGRESS = java.util.Collections.singletonList("Ingress");

  protected String managedPorts = DEFAULT_LOCKSS_PROTECTED_PORTS;
  // Enable test mode that skips talking to a live Kubernetes cluster
  protected boolean dryRun = false;
  protected String policyFileName = DEFAULT_POLICY_FILE;

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
      ingressUpdateExecutor.shutdown();
      if (!ingressUpdateExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
        ingressUpdateExecutor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      ingressUpdateExecutor.shutdownNow();
    }
    super.stopService();
  }

  public void setConfig(org.lockss.config.Configuration config,
      org.lockss.config.Configuration oldConfig,
      org.lockss.config.Configuration.Differences diffs) {
    try {
      if (ConfigManager.getPlatformVersion().isKubernetes()) {
        queueConfigChanges(config, diffs);
      }
      // Update dry-run mode whenever configuration changes affect this manager
      if (diffs.contains(PREFIX)) {
        if (dryRun) {
          log.info("NetworkPolicy dry-run mode enabled: will not apply changes to Kubernetes");
        } else {
          log.debug("NetworkPolicy dry-run mode disabled: will apply changes to Kubernetes");
        }
        this.policyFileName = config.get(PARAM_POLICY_FILE, DEFAULT_POLICY_FILE);
      }
    } catch (Exception ex) {
      log.error("Error processing configuration update", ex);
    }
  }

  void queueConfigChanges(Configuration config, Differences diffs) {
    if (diffs.contains(PARAM_LOCKSS_PROTECTED_PORTS) ||
        diffs.contains(PARAM_IP_ACCESS_INCLUDE) ||
        diffs.contains(PARAM_IP_ACCESS_EXCLUDE)) {
      if(diffs.contains(PARAM_LOCKSS_PROTECTED_PORTS)) {
        // we changed a protected port
        managedPorts = config.get(PARAM_LOCKSS_PROTECTED_PORTS,
            DEFAULT_LOCKSS_PROTECTED_PORTS);
      }
      // enqueue the update to be processed by a single background thread
      final List<String> includes = config.getList(PARAM_IP_ACCESS_INCLUDE);
      final List<String> excludes = config.getList(PARAM_IP_ACCESS_EXCLUDE);
      ingressUpdateExecutor.submit(() -> {
        try {
          updateNetworkPolicyIngress(includes, excludes);
        } catch (Throwable t) {
          log.warning("Error running queued updateNetworkPolicyIngress task", t);
        }
      });
    }
  }
  /**
   * Build and persist a NetworkPolicy based on the include/exclude IP filters.
   */
  void updateNetworkPolicyIngress(List<String> includeFilters,
      List<String> excludeFilters) {
    // Delegate to the new overload preserving existing default filename behavior
    updateNetworkPolicyIngress(includeFilters, excludeFilters, null);
  }

  /**
   * Build and persist a NetworkPolicy ingress section based on the include/exclude IP filters.
   * Allows specifying an alternate output filename; if null/blank, defaults to K8S_OUTPUT_FILENAME.
   */
  void updateNetworkPolicyIngress(List<String> includeFilters,
      List<String> excludeFilters, String outFilename) {
    List<String> allowedCidrs = toCidrList(includeFilters);
    List<String> deniedCidrs = toCidrList(excludeFilters);
    if (allowedCidrs.isEmpty()) {
      // xxx this should never be empty.
      log.info(
          "No allowed CIDRs derived from include list; skipping Kubernetes access policy preparation");
      return;
    }
    String namespace = K8S_NAMESPACE_LOCKSS;
    String outputPath = policyFileName;
    try {
      if (outFilename == null || outFilename.isBlank()) {
        if (dryRun) {
          Path tmp = Files.createTempFile("lockss-network-policy-", ".yaml");
          outputPath = tmp.toAbsolutePath().toString();
          log.info("Dry-run: writing NetworkPolicy YAML to temp file: " + outputPath);
        }
      } else {
        outputPath = outFilename;
      }
    } catch (IOException ioe) {
      log.warning("Failed to choose output file; defaulting to " + policyFileName , ioe);
    }
    try {
      // Find existing policy or create a default one; ensure spec exists
      V1NetworkPolicy networkPolicy = findExistingPolicyorCreate(EXISTING_POLICY_NAME, namespace);
      ensureSpecWithDefaults(networkPolicy);
      // Build ingress rules
      List<V1NetworkPolicyIngressRule> ingressRules = new ArrayList<>();
      // Always allow from any pod (podSelector: {})
      ingressRules.add( new V1NetworkPolicyIngressRule()
              .from(java.util.Collections.singletonList(new V1NetworkPolicyPeer().podSelector(anyPodSelector())))
      );
      // Build ports from current managedPorts
      List<V1NetworkPolicyPort> ports = buildPorts(managedPorts);
      // One rule per allowed CIDR, with optional except list and the fixed ports
      for (String cidr : allowedCidrs) {
        V1IPBlock block = buildIpBlock(cidr, deniedCidrs);
        V1NetworkPolicyPeer peer = new V1NetworkPolicyPeer().ipBlock(block);
        V1NetworkPolicyIngressRule cidrRule = new V1NetworkPolicyIngressRule()
            .from(java.util.Collections.singletonList(peer)).ports(ports);
        ingressRules.add(cidrRule);
      }
      networkPolicy.getSpec().setIngress(ingressRules);
      writePolicyToFile(networkPolicy, outputPath);
      log.info("Wrote updated ingress for NetworkPolicy '" + EXISTING_POLICY_NAME + "' in namespace '"
              + namespace + "' to " + outputPath);
      if (!dryRun) {
        applyNetworkPolicyToCluster(networkPolicy);
      }
    } catch (Exception e) {
      log.warning("Error while preparing Kubernetes NetworkPolicy for access control", e);
    }
  }


  /**
   * Convert a list of IP expressions (single IP, wildcard like 10.*.*.*, or CIDR) into CIDR
   * strings.
   */
  List<String> toCidrList(List<String> ips) {
    List<String> result = new ArrayList<>();
    if (ips == null) {
      return result;
    }
    ips.stream().filter(raw -> !StringUtil.isNullString(raw)).map(String::trim)
        .filter(s -> !s.isEmpty() && !s.startsWith("#")).forEach(s -> {
          try {
            Mask mask = IpFilter.newMask(s);
            result.add(mask.toString());
          } catch (MalformedException ex) {
            log.warning(
                MessageFormat.format("Skipping unparsable IP entry for NetworkPolicy: {0} ({1})", s,
                    ex.getMessage()));
          }
        });
    return result;
  }

  /**
   * Retrieve an existing NetworkPolicy or create a default one.
   */
  private V1NetworkPolicy findExistingPolicyorCreate(String policyName, String namespace)
      throws ApiException, IOException {
    V1NetworkPolicy existing = K8sClientUtils.readNetworkPolicyOrNull(policyName, namespace);
    if (existing == null) {
      log.info("NetworkPolicy '" + policyName + "' not found in namespace '" + namespace
          + "'; creating default.");
      existing = new V1NetworkPolicy();
      V1ObjectMeta metadata = new V1ObjectMeta();
      metadata.setName(policyName);
      metadata.setNamespace(namespace);
      existing.setMetadata(metadata);
      existing.setApiVersion(K8S_API_VERSION);
      existing.setKind("NetworkPolicy");
    }
    return existing;
  }

  /**
   * Ensure a NetworkPolicy spec exists, with sane defaults.
   */
  private void ensureSpecWithDefaults(V1NetworkPolicy policy) {
    if (policy.getSpec() == null) {
      log.info("NetworkPolicy '" + policy.getMetadata().getName() + "' in namespace '"
          + policy.getMetadata().getNamespace() + "' has null spec; setting defaults.");

      V1NetworkPolicySpec spec = new V1NetworkPolicySpec();
      spec.setPolicyTypes(POLICY_TYPES_INGRESS);

      // Empty selector means "any pod"
      spec.setPodSelector(anyPodSelector());
      policy.setSpec(spec);
    }
  }


  /**
   * Create or replace the given NetworkPolicy in the running Kubernetes cluster. If it doesn't
   * exist, it will be created; otherwise, it is replaced.
   */
  void applyNetworkPolicyToCluster(V1NetworkPolicy policy) {
    if (policy == null || policy.getMetadata() == null) {
      log.warning("Cannot apply NetworkPolicy: policy or metadata is null");
      return;
    }
    final String name = policy.getMetadata().getName();
    final String namespace = policy.getMetadata().getNamespace();

    if (name == null || namespace == null) {
      log.warning("Cannot apply NetworkPolicy: name or namespace is null");
      return;
    }

    try {
      K8sClientUtils.ApplyResult result =
          K8sClientUtils.createOrReplaceNetworkPolicy(policy);
      if (result == K8sClientUtils.ApplyResult.REPLACED) {
        log.info("Replaced NetworkPolicy '" + name + "' in namespace '" + namespace + "'");
      } else if (result == K8sClientUtils.ApplyResult.CREATED) {
        log.info("Created NetworkPolicy '" + name + "' in namespace '" + namespace + "'");
      } else {
        log.info("No change for NetworkPolicy '" + name + "' in namespace '" + namespace + "'");
      }
    } catch (Exception e) {
      log.warning("Failed to apply NetworkPolicy to cluster", e);
    }
  }

  /**
   * Serialize policy as YAML and write to a file for later application (e.g., kubectl apply -f).
   */
  void writePolicyToFile(V1NetworkPolicy policy, String filename) throws IOException {
    K8sClientUtils.writeNetworkPolicyToFile(policy, filename);
  }
  
  private V1IPBlock buildIpBlock(String cidr, List<String> deniedCidrs) {
    V1IPBlock block = new V1IPBlock().cidr(cidr);
    if (deniedCidrs != null && !deniedCidrs.isEmpty()) {
      block.setExcept(new ArrayList<>(deniedCidrs));
    }
    return block;
  }

  private V1LabelSelector anyPodSelector() {
    // Empty selector means "any pod"
    return new V1LabelSelector();
  }

  List<V1NetworkPolicyPort> buildPorts(String managedPorts) {
    if (managedPorts == null || managedPorts.isBlank()) {
      throw new IllegalArgumentException("Null or blank managedPorts");
    }

    java.util.LinkedHashSet<Integer> uniquePorts =
        Arrays.stream(managedPorts.split(";", -1)) // keep empties if any
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> {
              int port;
              try {
                port = Integer.parseInt(s);
              } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port: '" + s + "'");
              }
              if (port < 1 || port > 65535) {
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
