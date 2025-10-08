/*

Copyright (c) 2000-2025, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

 */

package org.lockss.util;

import static org.mockito.Mockito.*;

import inet.ipaddr.AddressStringException;
import io.kubernetes.client.custom.*;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.*;
import java.io.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.lockss.log.L4JLogger;
import org.lockss.test.*;

public class TestNetworkPolicyManager extends LockssCoreTestCase5 {
  private final L4JLogger log = L4JLogger.getLogger();

  private static final List<Integer> EXPECTED_PORTS = List.of(24681, 24682, 24602);
  private static final List<String> INCLUDE_CIDRS = List.of("10.255.0.0/16", "171.67.138.0/24");
  private static final String EXCLUDE_CIDRS = "192.168.0.0/16";
  private static final String REFERENCE_YAML = "lockss-network-policy.yaml";
  private TestableNetworkPolicyManager npMgr;

  @BeforeEach
  public void setUpBeforeEachTest() throws Exception {
    npMgr = new TestableNetworkPolicyManager();
    super.getMockLockssDaemon().setManagerByType(NetworkPolicyManager.class, npMgr);
    npMgr.initService(getMockLockssDaemon());
    npMgr.startService();
  }

  @AfterEach
  public void tearDownAfterEachTest() throws Exception {
    if (npMgr != null) {
      npMgr.stopService();
      npMgr = null;
    }
  }

  @Override
  protected boolean wantTempTmpDir() {
    return true;
  }

  @Test
  public void testGetCidrIntersection() throws Exception {
    assertCidrIntersection(List.of("10.0.0.1"), "10.0.0.0/8", List.of("10.0.0.1"));
    assertCidrIntersection(List.of("10.0.0.1/32"), "10.0.0.0/8", List.of("10.0.0.1/32"));
    assertCidrIntersection(Collections.emptyList(), "171.66.236.0/24", List.of("10.0.0.1"));
    assertCidrIntersection(List.of("171.66.236.16"), "171.66.236.0/24", List.of("10.0.0.1", "171.66.236.16"));
  }

  private void assertCidrIntersection(List<String> expectedIntersection, String cidr, List<String> deniedCidrs)
      throws AddressStringException {
    List<String> actualIntersection = npMgr.getCidrIntersection(cidr, deniedCidrs);
    assertEquals(expectedIntersection, actualIntersection);
  }

  /**
   * Tests for npMgr.toCidrList
   */
  @Test
  public void testNullInputReturnsEmpty() {
    List<String> res = npMgr.toCidrList(null);
    assertNotNull(res);
    assertTrue(res.isEmpty());
  }

  @Test
  public void testIgnoresCommentsAndBlanksAndTrims() {
    List<String> input = Arrays.asList(
        "# a comment",
        "   ",
        "\t",
        "   # another comment",
        "10.*.*.*   ",
        "   192.168.1.1"
    );

    List<String> res = npMgr.toCidrList(input);
    // Expect only valid entries, trimmed, converted to CIDR format
    // 10.*.*.*  -> 10.0.0.0/8
    // 192.168.1.1 -> 192.168.1.1/32
    assertEquals(2, res.size());
    assertEquals("10.0.0.0/8", res.get(0));
    assertEquals("192.168.1.1/32", res.get(1));
  }

  @Test
  public void testConvertsIpv4SinglesWildcardsAndCidr() {
    List<String> input = Arrays.asList(
        "10.*.*.*",
        "172.16.31.*",
        "192.168.1.5",
        "172.16.0.0/12"
    );

    List<String> res = npMgr.toCidrList(input);
    assertEquals(4, res.size());
    assertEquals("10.0.0.0/8", res.get(0));
    assertEquals("172.16.31.0/24", res.get(1));
    assertEquals("192.168.1.5/32", res.get(2));
    assertEquals("172.16.0.0/12", res.get(3));
  }

  @Test
  public void testConvertsIpv6() {
    List<String> input = Arrays.asList(
        "2001:db8::/32",   // already CIDR
        "::1",             // single IPv6 address should become /128
        "2001:db8::"       // address only, should become /128
    );

    List<String> res = npMgr.toCidrList(input);
    assertEquals(3, res.size());
    assertEquals("2001:db8:0:0:0:0:0:0/32", res.get(0));
    assertEquals("0:0:0:0:0:0:0:1", res.get(1));
    assertEquals("2001:db8:0:0:0:0:0:0", res.get(2));
  }

  @Test
  public void testSkipsMalformedEntries() {
    List<String> input = Arrays.asList(
        "10.*.*.*",
        "abc",               // invalid
        "10.*.*.*.*",        // invalid
        "192.0.2.1"          // valid
    );

    List<String> res = npMgr.toCidrList(input);
    // Only the valid ones should remain, in order
    assertEquals(2, res.size());
    assertEquals("10.0.0.0/8", res.get(0));
    assertEquals("192.0.2.1/32", res.get(1));
  }

  @Test
  public void testGenerateUpdatedNetworkPolicy() throws Exception {
    String namespace = "lockss";
    String policyName = "test-policy";
    List<String> allowed = Arrays.asList("10.0.0.0/8", "192.168.0.0/16");
    List<String> denied = Arrays.asList("172.16.0.0/12");
    String managedAdminPorts = "80;443";

    // Assert that if there is not an existing policy, generateUpdatedNetworkPolicy()
    // creates a default one and populates it with the provided ingress settings
    {
      V1NetworkPolicy policy =
          npMgr.generateUpdatedNetworkPolicy(namespace, policyName, allowed, denied, managedAdminPorts);

      assertNotNull(policy);
      V1ObjectMeta metadata = policy.getMetadata();
      assertEquals(namespace, metadata.getNamespace());
      assertEquals(policyName, metadata.getName());
      assertEquals(npMgr.K8S_API_VERSION, policy.getApiVersion());
      assertEquals("NetworkPolicy", policy.getKind());
    }

    // Assert existing policy with null spec is populated with defaults
    {

      V1NetworkPolicy existingPolicy = npMgr.findExistingPolicyorCreate(namespace, policyName);
//      V1NetworkPolicy existingPolicy = new V1NetworkPolicy();
//      V1ObjectMeta metadata = new V1ObjectMeta();
//      existingPolicy.setMetadata(metadata);
//      assertNull(existingPolicy.getSpec());

      npMgr.setExistingPolicy(existingPolicy);

      V1NetworkPolicy policy =
          npMgr.generateUpdatedNetworkPolicy(namespace, policyName, allowed, denied, managedAdminPorts);

      assertNotNull(existingPolicy.getSpec());
      assertIterableEquals(NetworkPolicyManager.POLICY_TYPES_INGRESS, existingPolicy.getSpec().getPolicyTypes());
      assertEquals("non-lockss", existingPolicy.getSpec()
          .getPodSelector()
          .getMatchLabels()
          .get("service-kind"));
      assertSame(existingPolicy.getSpec(), policy.getSpec());

      for (V1NetworkPolicyIngressRule ingressRule : policy.getSpec().getIngress()) {
        for (V1NetworkPolicyPeer peer : ingressRule.getFrom()) {
          log.info("peer: {}", peer);
        }
      }

//      String tmpFile = getTempFile("np-", ".yaml").getAbsolutePath();
//      npMgr.writePolicyToFile(tmpFile, policy);
      log.info("policy: {}", policy);
    }

    // Reset existing policy to null
    npMgr.setExistingPolicy(null);

    // Assert an existing policy is updated with the provided ingress settings
    {
      V1NetworkPolicy existingPolicy = new V1NetworkPolicy();
      npMgr.setExistingPolicy(existingPolicy);
    }

    // Assert that if there is an existing policy that already has ingress settings, that they
    // are not repeated:
  }

  /**
   * Test for {@link NetworkPolicyManager#writeAndApplyNetworkPolicy(String, V1NetworkPolicy...)}.
   */
  @Test
  public void testWriteAndApplyNetworkPolicy() throws Exception {
    V1NetworkPolicy policy = npMgr.createDefaultNetworkPolicy("lockss", "test-policy");

    V1NetworkPolicy[] policies = new V1NetworkPolicy[] {policy};
    NetworkPolicyManager npm = new NetworkPolicyManager();
    npm.writeAndApplyNetworkPolicy("test-namespace", policies);
  }

  @Test
  public void testWritePolicyToFileWritesYaml() throws Exception {
    V1NetworkPolicy policy = new V1NetworkPolicy()
        .apiVersion(npMgr.K8S_API_VERSION)
        .kind("NetworkPolicy")
        .metadata(new V1ObjectMeta().name("test-policy").namespace("lockss"));
    File out = getTempFile("np-", ".yaml");
    try {
      npMgr.writePolicyToFile(out.getAbsolutePath(), policy);
      assertTrue(out.exists());
      assertTrue(out.length() > 0L, "Output file should not be empty");

      // Spot-check contents contain basic YAML fields
      byte[] buf = new byte[(int) Math.min(out.length(), 2048)];
      try (FileInputStream fis = new FileInputStream(out)) {
        int read = fis.read(buf);
        String s = new String(buf, 0, Math.max(0, read));
        assertTrue(s.contains("NetworkPolicy"));
        assertTrue(s.contains("apiVersion:"));
        assertTrue(s.contains("metadata:"));
      }
    } finally {
      // Cleanup
      if (out.exists()) {
        out.delete();
      }
    }
  }

  @Test
  public void testSetConfigIgnoredWhenNotKubernetes() throws Exception {
    // Force non-Kubernetes platform behavior via mock
    npMgr.setTestPlatformVersion(mockPlatformVersion(false));
    org.lockss.test.ConfigurationUtil.resetConfig();
    org.lockss.test.ConfigurationUtil.setFromArgs(
        "org.lockss.ui.access.ip.include", "10.*.*.*;192.168.0.0/16",
        "org.lockss.ui.access.ip.exclude", "172.16.0.0/12");

    // Because platform is not Kubernetes, the async task should NOT run
    Boolean signal = npMgr.calls.poll(300, java.util.concurrent.TimeUnit.MILLISECONDS);
    boolean triggered = signal != null;
    assertFalse(triggered, "updateNetworkPolicyIngress should not be invoked off-Kubernetes");
    assertEquals(0, npMgr.updateCalls);
  }

  @Test
  public void testSetConfigRunsWhenKubernetes() throws Exception {
    npMgr.setTestPlatformVersion(mockPlatformVersion(true));
    org.lockss.test.ConfigurationUtil.setFromArgs(
        "org.lockss.ui.access.ip.include", "10.*.*.*;192.168.0.0/16",
        "org.lockss.ui.access.ip.exclude", "172.16.0.0/12");

    // Because platform is Kubernetes, the async task should  run
    Boolean signal = npMgr.calls.poll(5, java.util.concurrent.TimeUnit.SECONDS);
    assertTrue(signal != null,
        "updateNetworkPolicyIngress should be invoked on Kubernetes");
    assertEquals(1, npMgr.updateCalls, "Expected exactly two calls");
  }

  @Test
  public void testSetConfig_updatesManagedPortsOnPrefixDiff() throws Exception {
    // Ensure a clean start and force Kubernetes platform
    npMgr.setTestPlatformVersion(mockPlatformVersion(true));
    Properties props = new Properties();
    props.put("org.lockss.networkPolicy.protected.adminPorts", "80;443");
    props.put("org.lockss.ui.access.ip.include", "10.*.*.*");
    props.put("org.lockss.ui.access.ip.exclude", "192.168.0.0/16");
    // Change the protected ports under the PREFIX to trigger diffs.contains(PREFIX)
    org.lockss.test.ConfigurationUtil.setCurrentConfigFromProps(props);

    // Wait for the config callback to run
    Boolean signal = npMgr.calls.poll(5, java.util.concurrent.TimeUnit.SECONDS);
    assertNotNull(signal, "Expected updateNetworkPolicyIngress to be invoked");

    // Verify that managedPorts was updated by setConfig based on the new value
    assertEquals("80;443", npMgr.managedAdminPorts, "managedPorts should be updated from config");

    // And that it is usable by buildPorts (sanity check)
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts(npMgr.managedAdminPorts);
    assertEquals(2, ports.size());
    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
  }

  @Test
  public void testSetConfig_doesNotChangeManagedPortsWhenPrefixNotInDiff() throws Exception {
    // Start with a known ports setting
    org.lockss.test.ConfigurationUtil.resetConfig();
    npMgr.setTestPlatformVersion(mockPlatformVersion(true));
    Properties props = new Properties();
    props.put("org.lockss.networkPolicy.protected.adminPorts", "8080;24682");
    props.put("org.lockss.ui.access.ip.include", "10.*.*.*");
    props.put("org.lockss.ui.access.ip.exclude", "192.168.0.0/16");
    org.lockss.test.ConfigurationUtil.setCurrentConfigFromProps(props);
    // Wait for initial config to apply
    Boolean firstSignal = npMgr.calls.poll(5, java.util.concurrent.TimeUnit.SECONDS);
    assertNotNull(firstSignal, "Expected initial update to be invoked");
    assertEquals("8080;24682", npMgr.managedAdminPorts);

    // Now change only the include/exclude (no PREFIX change)
    props = new Properties();
    props.put("org.lockss.networkPolicy.protected.adminPorts", "8080;24682");
    props.put("org.lockss.ui.access.ip.include","172.16.0.0/12");
    props.put("org.lockss.ui.access.ip.exclude", "192.168.1.0/24");
    org.lockss.test.ConfigurationUtil.setCurrentConfigFromProps(props);
    Boolean secondSignal = npMgr.calls.poll(5, java.util.concurrent.TimeUnit.SECONDS);
    assertNotNull(secondSignal, "Expected update to be invoked due to include/exclude change");

    // managedPorts should remain unchanged since PREFIX wasn't in the diff
    assertEquals("8080;24682", npMgr.managedAdminPorts, "managedPorts should not change without PREFIX diff");
  }

  @Test
  void buildPorts_throwsOnNullOrBlank() {
    IllegalArgumentException ex1 =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts(null));
    assertTrue(ex1.getMessage().contains("Null or blank"));

    IllegalArgumentException ex2 =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts("   "));
    assertTrue(ex2.getMessage().contains("Null or blank"));
  }

  @Test
  void buildPorts_throwsOnInvalidToken() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts("80:abc:443"));
    assertTrue(ex.getMessage().contains("Invalid port"));
  }

  @Test
  void buildPorts_throwsOnOutOfRange() {
    IllegalArgumentException exLow =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts("0"));
    assertTrue(exLow.getMessage().contains("out of range"));

    IllegalArgumentException exHigh =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts("70000"));
    assertTrue(exHigh.getMessage().contains("out of range"));
  }

  @Test
  void buildPorts_parsesValidPorts() {
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts("80;443;8080");
    assertEquals(3, ports.size());

    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
    assertPortIntValue(ports.get(2), 8080);
  }

  @Test
  void buildPorts_trimsAndSkipsEmpty() {
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts(" 80 ; ; 443 ;8080 ");
    assertEquals(3, ports.size());

    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
    assertPortIntValue(ports.get(2), 8080);
  }

  @Test
  void buildPorts_removesDuplicatesPreservesFirstOrder() {
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts("80;80;443;80;443");
    assertEquals(2, ports.size(), "Duplicates should be removed");
    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
  }

  @Test
  void buildPorts_throwsWhenNoValidPortsRemain() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts(" ;  ; "));
    assertTrue(ex.getMessage().toLowerCase().contains("no valid ports"));
  }
  
  private void assertIpRuleWithPorts(V1NetworkPolicyIngressRule rule, String expectedCidr,
      List<Integer> expectedPorts) {
    assertNotNull(rule.getFrom(), "CIDR rule should have 'from'");
    assertEquals(1, rule.getFrom().size(), "CIDR rule should have exactly one 'from' entry");
    V1NetworkPolicyPeer peer = rule.getFrom().get(0);
    V1IPBlock block = peer.getIpBlock();
    assertNotNull(block, "CIDR rule should have an ipBlock");
    assertEquals(expectedCidr, block.getCidr());
    assertNotNull(rule.getPorts(), "CIDR rule should specify ports");
    assertEquals(expectedPorts.size(), rule.getPorts().size(),
        "CIDR rule should have expected number of ports");
    for (int i = 0; i < expectedPorts.size(); i++) {
      assertNotNull(rule.getPorts().get(i).getPort(), "Port entry should have a value");
      assertEquals(expectedPorts.get(i).intValue(), rule.getPorts().get(i).getPort().getIntValue());
    }
  }

  @Test
  public void testResourceYamlRoundTrip() throws Exception {
    // Read the reference YAML from the classpath
    try (java.io.InputStream is = org.lockss.util.UrlUtil.getResourceAsStream(REFERENCE_YAML)) {
      assertNotNull(is, "Reference " + REFERENCE_YAML + " should be on the classpath");
      V1NetworkPolicy original = loadNetworkPolicy(is);

      // Write it back out to a temp file
      File out = getTempFile("np-roundtrip-", ".yaml");
      try {
        npMgr.writePolicyToFile(out.getAbsolutePath(), original);
        assertTrue(out.exists(), "Output file should exist");

        // Load the written file
        V1NetworkPolicy reloaded = loadNetworkPolicy(out);

        // Assert the important fields are identical after round-trip
        assertEquals(original.getApiVersion(), reloaded.getApiVersion());
        assertEquals(original.getKind(), reloaded.getKind());
        assertNotNull(reloaded.getMetadata());
        assertEquals(original.getMetadata().getName(), reloaded.getMetadata().getName());
        assertEquals(original.getMetadata().getNamespace(), reloaded.getMetadata().getNamespace());

        assertNotNull(reloaded.getSpec(), "Spec should be present");
        assertEquals(original.getSpec().getPolicyTypes(), reloaded.getSpec().getPolicyTypes(), "Policy types should match");
        // Pod selector and ingress rules should match semantically
        assertEquals(
            original.getSpec().getPodSelector() == null ? null : original.getSpec().getPodSelector().getMatchLabels(),
            reloaded.getSpec().getPodSelector() == null ? null : reloaded.getSpec().getPodSelector().getMatchLabels(),
            "Pod selector matchLabels should match");

        assertEquals(
            original.getSpec().getIngress() == null ? 0 : original.getSpec().getIngress().size(),
            reloaded.getSpec().getIngress() == null ? 0 : reloaded.getSpec().getIngress().size(),
            "Ingress rule count should match");

        if (original.getSpec().getIngress() != null) {
          for (int i = 0; i < original.getSpec().getIngress().size(); i++) {
            V1NetworkPolicyIngressRule oRule = original.getSpec().getIngress().get(i);
            V1NetworkPolicyIngressRule rRule = reloaded.getSpec().getIngress().get(i);

            // Compare 'from' peers
            java.util.List<V1NetworkPolicyPeer> oFrom = oRule.getFrom();
            java.util.List<V1NetworkPolicyPeer> rFrom = rRule.getFrom();
            assertEquals(oFrom == null ? 0 : oFrom.size(), rFrom == null ? 0 : rFrom.size(), "from peer count should match at index " + i);
            if (oFrom != null) {
              for (int j = 0; j < oFrom.size(); j++) {
                V1NetworkPolicyPeer oPeer = oFrom.get(j);
                V1NetworkPolicyPeer rPeer = rFrom.get(j);
                // ipBlock CIDR and except lists
                if (oPeer.getIpBlock() != null || rPeer.getIpBlock() != null) {
                  assertNotNull(oPeer.getIpBlock());
                  assertNotNull(rPeer.getIpBlock());
                  assertEquals(oPeer.getIpBlock().getCidr(), rPeer.getIpBlock().getCidr(), "CIDR should match at rule " + i);
                  assertEquals(
                      oPeer.getIpBlock().getExcept() == null ? java.util.List.of() : oPeer.getIpBlock().getExcept(),
                      rPeer.getIpBlock().getExcept() == null ? java.util.List.of() : rPeer.getIpBlock().getExcept(),
                      "CIDR except list should match at rule " + i);
                }
                // podSelector matchLabels
                if (oPeer.getPodSelector() != null || rPeer.getPodSelector() != null) {
                  assertNotNull(oPeer.getPodSelector());
                  assertNotNull(rPeer.getPodSelector());
                  assertEquals(
                      oPeer.getPodSelector().getMatchLabels(),
                      rPeer.getPodSelector().getMatchLabels(),
                      "podSelector matchLabels should match at rule " + i);
                }
              }
            }

            // Compare ports
            java.util.List<V1NetworkPolicyPort> oPorts = oRule.getPorts();
            java.util.List<V1NetworkPolicyPort> rPorts = rRule.getPorts();
            assertEquals(oPorts == null ? 0 : oPorts.size(), rPorts == null ? 0 : rPorts.size(), "port count should match at rule " + i);
            if (oPorts != null) {
              for (int j = 0; j < oPorts.size(); j++) {
                V1NetworkPolicyPort op = oPorts.get(j);
                V1NetworkPolicyPort rp = rPorts.get(j);
                // Compare IntOrString port values as integers if possible
                if (op.getPort() != null || rp.getPort() != null) {
                  assertNotNull(op.getPort());
                  assertNotNull(rp.getPort());
                  assertEquals(op.getPort().getIntValue(), rp.getPort().getIntValue(), "port intValue should match at rule " + i + " idx " + j);
                }
              }
            }
          }
        }
      } finally {
        // Cleanup temp file
        // ignore failures
        //noinspection ResultOfMethodCallIgnored
        out.delete();
      }
    }
  }

  private V1NetworkPolicy loadNetworkPolicy(File file) throws IOException {
    try (java.io.FileReader reader = new java.io.FileReader(file)) {
      return Yaml.loadAs(reader, V1NetworkPolicy.class);
    }
  }

  private V1NetworkPolicy loadNetworkPolicy(java.io.InputStream is) throws java.io.IOException {
    try (java.io.Reader reader =
             new java.io.InputStreamReader(is, java.nio.charset.StandardCharsets.UTF_8)) {
      return Yaml.loadAs(reader, V1NetworkPolicy.class);
    }
  }

  private  void assertPortIntValue(V1NetworkPolicyPort npPort, int expected) {
    IntOrString ios = npPort.getPort();
    assertNotNull(ios, "Port should be set");
    assertEquals(expected, ios.getIntValue());
  }

  // Add completed mock to allow changing platform version
  private static org.lockss.util.PlatformVersion mockPlatformVersion(boolean isK8s) {
    org.lockss.util.PlatformVersion pv = mock(org.lockss.util.PlatformVersion.class);
    when(pv.isKubernetes()).thenReturn(isK8s);
    when(pv.isRuncluster()).thenReturn(!isK8s);
    when(pv.getName()).thenReturn(isK8s ? "K8s" : "runcluster");
    when(pv.getVersion()).thenReturn("1");
    when(pv.toString()).thenReturn((isK8s ? "K8s" : "runcluster") + "-1");
    when(pv.toString(" ")).thenReturn((isK8s ? "K8s" : "runcluster") + " 1");

    return pv;
  }

  private static class TestableNetworkPolicyManager extends NetworkPolicyManager {
    volatile int updateCalls = 0;
    // Use a resettable signal queue to avoid one-shot CountDownLatch issues across multiple calls
    final java.util.concurrent.BlockingQueue<Boolean> calls = new java.util.concurrent.LinkedBlockingQueue<>();
    private org.lockss.util.PlatformVersion testPv;
    private V1NetworkPolicy existingPolicy;

    void setTestPlatformVersion(org.lockss.util.PlatformVersion pv) {
      this.testPv = pv;
    }

    public void setExistingPolicy(V1NetworkPolicy testNetworkPolicy) {
      this.existingPolicy = testNetworkPolicy;
    }

    public V1NetworkPolicy getExistingPolicy() {
      return this.existingPolicy;
    }

    @Override
    protected V1NetworkPolicy findExistingPolicyorCreate(final String policyName, final String namespace) {
      if (existingPolicy == null) {
        existingPolicy = createDefaultNetworkPolicy(namespace, policyName);
      }
      return existingPolicy;
    }

    @Override
    void writeAndApplyNetworkPolicy(String outFilename, V1NetworkPolicy... policies) {
      updateCalls++;
      calls.offer(Boolean.TRUE);
    }

    @Override
    public void setConfig(org.lockss.config.Configuration config,
        org.lockss.config.Configuration oldConfig,
        org.lockss.config.Configuration.Differences diffs) {
      try {
        org.lockss.util.PlatformVersion pv =
            (testPv != null) ? testPv : org.lockss.config.ConfigManager.getPlatformVersion();
        if (pv != null && pv.isKubernetes()) {
          queueConfigChanges(config, diffs);
        }
      } catch (Exception ex) {
        // swallow in test override
      }
    }
  }
}
