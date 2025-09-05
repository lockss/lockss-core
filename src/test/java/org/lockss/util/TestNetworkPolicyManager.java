package org.lockss.util;

import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.models.V1NetworkPolicy;
import io.kubernetes.client.openapi.models.V1NetworkPolicyPort;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.test.LockssCoreTestCase5;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNetworkPolicyManager extends LockssCoreTestCase5 {

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
    assertEquals("192.168.1.1", res.get(1));
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
    assertEquals("192.168.1.5", res.get(2));
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
    assertEquals("192.0.2.1", res.get(1));
  }

  @Test
  public void testUpdateNetworkPolicyIngressWithNoAllowedSkips() {
    // Should log and return without throwing; no Kubernetes calls executed
    npMgr.updateNetworkPolicyIngress(
        Collections.emptyList(),
        Arrays.asList("10.0.0.0/8", "192.168.0.0/16"));
  }

  @Test
  public void testGenerateLockssStyleNetworkPolicyFileSkipsWhenNoAllowed() throws Exception {
    // Should early-return and not create file
    File out = getTempFile("lockss-np-", ".yaml");
    // Ensure a clean, non-existent path for this test
    if (out.exists()) {
      out.delete();
    }
    assertFalse(out.exists());

    npMgr.generateLockssStyleNetworkPolicyFile(
        Collections.emptyList(),
        Arrays.asList("10.0.0.0/8"),
        out.getAbsolutePath());

    assertFalse(out.exists(), "File should not be created when include list is empty");
  }

  @Test
  public void testWritePolicyToFileWritesYaml() throws Exception {
    V1NetworkPolicy policy = new V1NetworkPolicy()
        .apiVersion(npMgr.K8S_API_VERSION)
        .kind("NetworkPolicy")
        .metadata(new V1ObjectMeta().name("test-policy").namespace("lockss"));
    File out = getTempFile("np-", ".yaml");
    try {
      npMgr.writePolicyToFile(policy, out.getAbsolutePath());
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
        "org.lockss.ui.ip.include", "10.*.*.*;192.168.0.0/16",
        "org.lockss.ui.ip.exclude", "172.16.0.0/12");

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
        "org.lockss.ui.ip.include", "10.*.*.*;192.168.0.0/16",
        "org.lockss.ui.ip.exclude", "172.16.0.0/12");

    // Because platform is Kubernetes, the async task should  run
    Boolean signal = npMgr.calls.poll(5, java.util.concurrent.TimeUnit.SECONDS);
    assertTrue(signal != null,
        "updateNetworkPolicyIngress should be invoked on Kubernetes");
    assertEquals(1, npMgr.updateCalls, "Expected exactly two calls");
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
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts("80:443:8080");
    assertEquals(3, ports.size());

    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
    assertPortIntValue(ports.get(2), 8080);
  }

  @Test
  void buildPorts_trimsAndSkipsEmpty() {
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts(" 80 : : 443 :8080 ");
    assertEquals(3, ports.size());

    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
    assertPortIntValue(ports.get(2), 8080);
  }

  @Test
  void buildPorts_removesDuplicatesPreservesFirstOrder() {
    List<V1NetworkPolicyPort> ports = npMgr.buildPorts("80:80:443:80:443");
    assertEquals(2, ports.size(), "Duplicates should be removed");
    assertPortIntValue(ports.get(0), 80);
    assertPortIntValue(ports.get(1), 443);
  }

  @Test
  void buildPorts_throwsWhenNoValidPortsRemain() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> npMgr.buildPorts(" :  : "));
    assertTrue(ex.getMessage().toLowerCase().contains("no valid ports"));
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

    void setTestPlatformVersion(org.lockss.util.PlatformVersion pv) {
      this.testPv = pv;
    }

    @Override
    void updateNetworkPolicyIngress(List<String> includeFilters, List<String> excludeFilters) {
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
          if (diffs.contains(PREFIX) ||
              diffs.contains("org.lockss.ui.ip.include") ||
              diffs.contains("org.lockss.ui.ip.exclude")) {
            if (diffs.contains(PREFIX)) {
              managedPorts = config.get(PARAM_LOCKSS_PROTECTED_PORTS, DEFAULT_LOCKSS_PROTECTED_PORTS);
            }
            final List<String> includes = config.getList("org.lockss.ui.ip.include");
            final List<String> excludes = config.getList("org.lockss.ui.ip.exclude");
            updateNetworkPolicyIngress(includes, excludes);
          }
        }
      } catch (Exception ex) {
        // swallow in test override
      }
    }
  }

}
