package org.lockss.util;

import io.kubernetes.client.custom.*;
import io.kubernetes.client.openapi.*;
import io.kubernetes.client.openapi.models.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.config.Configuration;
import org.lockss.util.IpFilter.*;

public class NetworkPolicyManager extends BaseLockssManager implements ConfigurableManager {

  protected static final String NETWORK_POLICY_TEMPLATE_FILENAME = "lockss-network-policy.yaml";
  private final Logger log = Logger.getLogger();
  static final String PREFIX = Configuration.PREFIX + "networkPolicy";
  // our network policy parameters
  final static String PARAM_LOCKSS_PROTECTED_PORTS = PREFIX +".protected.ports";
  static final String DEFAULT_LOCKSS_PROTECTED_PORTS = "24681;24682;24602";
  // acceess includes/exclue params
  private static final String PARAM_IP_ACCESS_INCLUDE = "org.lockss.ui.ip.include";
  private static final String PARAM_IP_ACCESS_EXCLUDE = "org.lockss.ui.ip.exclude";

  private static final String EXISTING_POLICY_NAME = "lockss";
  private static final String K8S_NAMESPACE_LOCKSS = "lockss";
  private static final String K8S_OUTPUT_FILENAME = "lockss-network-policy.yaml";
  protected static final String LOCKSS_NETWORK_POLICY_NAME = "lockss-network-policy";
  protected static final String K8S_API_VERSION = "networking.k8s.io/v1";

  protected String managedPorts = DEFAULT_LOCKSS_PROTECTED_PORTS;

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
        if (diffs.contains(PREFIX) ||
            diffs.contains(PARAM_IP_ACCESS_INCLUDE) ||
            diffs.contains(PARAM_IP_ACCESS_EXCLUDE)) {
          if(diffs.contains(PREFIX)) {
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
    } catch (Exception ex) {
      log.error("Error processing configuration update", ex);
    }
  }

  /**
   * Build and persist a NetworkPolicy ingress section based on the include/exclude IP filters.
   */
  void updateNetworkPolicyIngress(List<String> includeFilters,
      List<String> excludeFilters) {
    List<String> allowedCidrs = toCidrList(includeFilters);
    List<String> deniedCidrs = toCidrList(excludeFilters);
    if (allowedCidrs.isEmpty()) {
      // xxx this should never be empty.
      log.info(
          "No allowed CIDRs derived from include list; skipping Kubernetes access policy preparation");
      return;
    }
    final String namespace = K8S_NAMESPACE_LOCKSS;
    try {
      V1NetworkPolicy existing;
      try {
        existing = K8sClientUtils.readNetworkPolicyOrNull(EXISTING_POLICY_NAME, namespace);
      } catch (ApiException ae) {
        if (ae.getCode() == 404) {
          log.info(
              "NetworkPolicy '" + EXISTING_POLICY_NAME + "' not found in namespace '" + namespace
                  + "'. No file written.");
          return;
        }
        throw ae;
      }

      if (existing == null) {
        log.info(
            "NetworkPolicy '" + EXISTING_POLICY_NAME + "' not found in namespace '" + namespace
                + "'. No file written.");
        return;
      }

      if (existing.getSpec() == null) {
        log.warning(
            "Existing NetworkPolicy has null spec; cannot update ingress. No file written.");
        return;
      }

      List<V1NetworkPolicyPeer> fromPeers = new ArrayList<>();
      for (String cidr : allowedCidrs) {
        V1IPBlock block = buildIpBlock(cidr, deniedCidrs);
        fromPeers.add(new V1NetworkPolicyPeer().ipBlock(block));
      }
      V1NetworkPolicyIngressRule ingressRule = new V1NetworkPolicyIngressRule().from(fromPeers);
      existing.getSpec().setIngress(Collections.singletonList(ingressRule));

      writePolicyToFile(existing, K8S_OUTPUT_FILENAME);
      log.info(
          "Wrote updated ingress for NetworkPolicy '" + EXISTING_POLICY_NAME + "' in namespace '"
              + namespace + "' to " + K8S_OUTPUT_FILENAME);
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
   * Generate a NetworkPolicy file resembling "lockss-network-policy" using include/exclude inputs.
   * Produces: - metadata: name: lockss-network-policy namespace: lockss - spec:
   * podSelector.matchLabels: { service-kind: non-lockss } policyTypes: [ "Ingress" ] ingress: -
   * from: [ { podSelector: {} } ] - for each allowed CIDR: from: [ { ipBlock: { cidr, except } } ],
   * ports: 24681, 24682, 8080
   */
  void generateLockssStyleNetworkPolicyFile(List<String> includeFilters,
      List<String> excludeFilters,
      String outputFilename) {
    List<String> allowedCidrs = toCidrList(includeFilters);
    List<String> deniedCidrs = toCidrList(excludeFilters);
    //what should we do - if we do not have at least one allow we essentialy have
    // a collection of pods which not externally reachable
    if (allowedCidrs.isEmpty()) {
      log.info("No allowed CIDRs derived from include list; skipping file generation.");
      return;
    }

    // Metadata
    V1ObjectMeta metadata = new V1ObjectMeta()
        .name(LOCKSS_NETWORK_POLICY_NAME)
        .namespace(K8S_NAMESPACE_LOCKSS);

    // Pod selector: matchLabels: { service-kind: non-lockss }
    java.util.Map<String, String> matchLabels = new java.util.HashMap<>();
    matchLabels.put("service-kind", "non-lockss");
    V1LabelSelector podSelector = new V1LabelSelector().matchLabels(matchLabels);

    // Ports to expose on each CIDR rule
    List<V1NetworkPolicyPort> ports = buildPorts(managedPorts);

    // Ingress rules
    List<V1NetworkPolicyIngressRule> ingressRules = new ArrayList<>();

    // Rule allowing from any pod (podSelector: {})
    ingressRules.add(
        new V1NetworkPolicyIngressRule()
            .from(Collections.singletonList(
                new V1NetworkPolicyPeer().podSelector(anyPodSelector())))
    );

    // One rule per allowed CIDR, with (optional) except list and the fixed ports
    for (String cidr : allowedCidrs) {
      V1IPBlock block = buildIpBlock(cidr, deniedCidrs);
      V1NetworkPolicyPeer peer = new V1NetworkPolicyPeer().ipBlock(block);

      V1NetworkPolicyIngressRule cidrRule = new V1NetworkPolicyIngressRule()
          .from(Collections.singletonList(peer))
          .ports(ports);

      ingressRules.add(cidrRule);
    }

    // Spec assembly
    V1NetworkPolicySpec spec = new V1NetworkPolicySpec()
        .podSelector(podSelector)
        .policyTypes(Collections.singletonList("Ingress"))
        .ingress(ingressRules);

    V1NetworkPolicy policy = new V1NetworkPolicy()
        .apiVersion(K8S_API_VERSION)
        .kind("NetworkPolicy")
        .metadata(metadata)
        .spec(spec);

    String outputPath = (outputFilename == null || outputFilename.isBlank())
        ? NETWORK_POLICY_TEMPLATE_FILENAME
        : outputFilename;
    try {
      writePolicyToFile(policy, outputPath);
      log.info("Wrote NetworkPolicy to " + outputPath);
    } catch (IOException ioe) {
      log.warning("Failed to write NetworkPolicy to " + outputPath, ioe);
    }
    // Apply to running cluster
    applyNetworkPolicyToCluster(policy);
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
