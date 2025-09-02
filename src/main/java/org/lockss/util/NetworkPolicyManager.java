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
import org.lockss.util.IpFilter.*;

public class NetworkPolicyManager extends BaseLockssManager implements ConfigurableManager {

  protected static final String NETWORK_POLICY_TEMPLATE_FILENAME = "lockss-network-policy.yaml";
  private final Logger log = Logger.getLogger();

  private final String PARAM_IP_ACCESS_INCLUDE = "org.lockss.ui.ip.include";
  private final String PARAM_IP_ACCESS_EXCLUDE = "org.lockss.ui.ip.exclude";
  private final String EXISTING_POLICY_NAME = "lockss";
  private final String K8S_NAMESPACE_LOCKSS = "lockss";
  private final String K8S_OUTPUT_FILENAME = "lockss-network-policy-update.yaml";
  protected final String LOCKSS_NETWORK_POLICY_NAME = "lockss-network-policy";
  protected final String K8S_API_VERSION = "networking.k8s.io/v1";
  private final ExecutorService ingressUpdateExecutor =
      new ThreadPoolExecutor(
          1, 1,
          0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>());


  public NetworkPolicyManager() {}

  @Override
  public void startService() {
    //String versionName = ConfigManager.getPlatformVersion().getName();
    // todo: merge in tal's changes.
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
    if (diffs.contains(PARAM_IP_ACCESS_INCLUDE) || diffs.contains(PARAM_IP_ACCESS_EXCLUDE)) {
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
        V1IPBlock block = new V1IPBlock().cidr(cidr);
        if (!deniedCidrs.isEmpty()) {
          // use the existing array list rather than creating a new one each time
          block.setExcept(new ArrayList<>(deniedCidrs));
        }
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
    List<V1NetworkPolicyPort> ports = new java.util.ArrayList<>();
    ports.add(new V1NetworkPolicyPort().port(new IntOrString(24681)));
    ports.add(new V1NetworkPolicyPort().port(new IntOrString(24682)));
    ports.add(new V1NetworkPolicyPort().port(new IntOrString(8080)));

    // Ingress rules
    List<V1NetworkPolicyIngressRule> ingressRules = new ArrayList<>();

    // Rule allowing from any pod (podSelector: {})
    ingressRules.add(
        new V1NetworkPolicyIngressRule()
            .from(Collections.singletonList(
                new V1NetworkPolicyPeer().podSelector(new V1LabelSelector())))
    );

    // One rule per allowed CIDR, with (optional) except list and the fixed ports
    for (String cidr : allowedCidrs) {
      V1IPBlock block = new V1IPBlock().cidr(cidr);
      if (!deniedCidrs.isEmpty()) {
        block.setExcept(new ArrayList<>(deniedCidrs));
      }
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

    String out = (outputFilename == null || outputFilename.isBlank())
        ? NETWORK_POLICY_TEMPLATE_FILENAME
        : outputFilename;
    try {
      writePolicyToFile(policy, out);
      log.info("Wrote NetworkPolicy to " + out);
    } catch (IOException ioe) {
      log.warning("Failed to write NetworkPolicy to " + out, ioe);
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
}
