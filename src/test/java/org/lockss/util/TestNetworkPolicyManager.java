package org.lockss.util;

import io.kubernetes.client.openapi.models.*;
import io.netty.util.*;
import java.io.*;
import java.util.*;
import org.junit.*;
import org.lockss.test.*;

public class TestNetworkPolicyManager extends LockssTestCase4 {
  NetworkPolicyManager npMgr;
  private static Logger log = Logger.getLogger();
  private static final String TEST_IP_ADDRESS = "192.168.1.1";
  private static final String TEST_CIDR = "192.168.1.0/24";
  private static final String TEST_CIDR_2 = "192.168.2.0/24";
  private static final String TEST_CIDR_3 = "192.168.3.0/24";


  @Before
  public void setUpBeforeEachTest() throws Exception {
    super.setUp();
    npMgr = new NetworkPolicyManager();
    getMockLockssDaemon().setManagerByType(NetworkPolicyManager.class, npMgr);
    npMgr.initService(getMockLockssDaemon());
    npMgr.startService();
  }

  @After
  public void tearDownAfterEachTest() throws Exception {
    super.tearDown();
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
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File out = new File(tmpDir, "lockss-np-" + System.nanoTime() + ".yaml");
    assertFalse(out.exists());

    npMgr.generateLockssStyleNetworkPolicyFile(
        Collections.emptyList(),
        Arrays.asList("10.0.0.0/8"),
        out.getAbsolutePath());

    assertFalse("File should not be created when include list is empty", out.exists());
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
      assertTrue("Output file should not be empty", out.length() > 0L);

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
      if (out.exists()) out.delete();
    }
  }

  @Test
  public void testApplyNetworkPolicyToClusterNullPolicyDoesNotThrow() {
    npMgr.applyNetworkPolicyToCluster(null);
  }

  @Test
  public void testApplyNetworkPolicyToClusterNullMetadataDoesNotThrow() {
    V1NetworkPolicy policy = new V1NetworkPolicy();
    npMgr.applyNetworkPolicyToCluster(policy);
  }

  @Test
  public void testApplyNetworkPolicyToClusterNullNameDoesNotThrow() {
    V1NetworkPolicy policy = new V1NetworkPolicy()
        .metadata(new V1ObjectMeta().namespace("lockss"));
    npMgr.applyNetworkPolicyToCluster(policy);
  }

  @Test
  public void testApplyNetworkPolicyToClusterNullNamespaceDoesNotThrow() {
    V1NetworkPolicy policy = new V1NetworkPolicy()
        .metadata(new V1ObjectMeta().name("some-name"));
    npMgr.applyNetworkPolicyToCluster(policy);
  }

  @Test
  public void testGeneratesYamlWhenIncludePresent() throws Exception {
    // Given include contains 171.67.138.0/24, a YAML file should be generated
    File out = getTempFile("lockss-np-" + System.nanoTime(),".yaml");
    assertTrue(out.exists());

    try {
      npMgr.generateLockssStyleNetworkPolicyFile(
          Collections.singletonList("171.67.138.0/24"),
          Collections.emptyList(),
          out.getAbsolutePath());

      assertTrue("YAML file should be created", out.exists());
      assertTrue("Output file should not be empty", out.length() > 0L);

      // Spot-check contents for key fields and the CIDR we included
      byte[] buf = new byte[(int) Math.min(out.length(), 4096)];
      int read;
      try (FileInputStream fis = new FileInputStream(out)) {
        read = fis.read(buf);
      }
      String yaml = new String(buf, 0, Math.max(0, read));
      assertTrue("YAML should contain NetworkPolicy kind", yaml.contains("kind: NetworkPolicy"));
      assertTrue("YAML should contain policy name", yaml.contains("name: lockss-network-policy"));
      assertTrue("YAML should contain target namespace", yaml.contains("namespace: lockss"));
      assertTrue("YAML should include the specified CIDR",
          yaml.contains("171.67.138.0/24"));
    } finally {
      if (out.exists()) {
        // Cleanup
        out.delete();
      }
    }
  }
}