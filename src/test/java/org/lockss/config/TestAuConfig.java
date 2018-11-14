/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for org.lockss.config.AuConfig.
 */
public class TestAuConfig extends LockssTestCase5 {

  @Test
  public void testAuConfigFromConfiguration() {
    String auPropKey = null;
    Configuration auConfiguration = null;
    AuConfig auc = null;

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown NullPointerException");
    } catch (NullPointerException npe) {
    }

    auPropKey = "org.lockss.au.foo.auid";

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown NullPointerException");
    } catch (NullPointerException npe) {
    }

    auConfiguration = ConfigManager.fromProperties(new Properties());
    auc = new AuConfig(auPropKey, auConfiguration);
    assertEquals("foo&auid", auc.getAuid());
    assertEquals(0, auc.getConfiguration().size());

    Properties p = new Properties();
    p.put(auPropKey + ".foo", "111");
    p.put(auPropKey + ".bar", "222");
    p.put(auPropKey + ".baz", "333");
    auConfiguration = ConfigManager.fromProperties(p);

    auc = new AuConfig(auPropKey, auConfiguration);
    assertEquals("foo&auid", auc.getAuid());
    assertEquals(3, auc.getConfiguration().size());
    assertEquals("111", auc.getConfiguration().get("foo"));
    assertEquals("222", auc.getConfiguration().get("bar"));
    assertEquals("333", auc.getConfiguration().get("baz"));
  }

  @Test
  public void testFromBackupLine() {
    AuConfig auc = null;

    try {
      auc = AuConfig.fromBackupLine(null);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfig.fromBackupLine("");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    String bul = "abc";
    auc = AuConfig.fromBackupLine(bul);
    assertEquals(bul, auc.getAuid());
    assertEquals(0, auc.getConfiguration().size());
    assertEquals(bul, auc.toBackupLine());

    bul = "abc def";
    auc = AuConfig.fromBackupLine(bul);
    assertEquals(bul, auc.getAuid());
    assertEquals(0, auc.getConfiguration().size());
    assertEquals(bul, auc.toBackupLine());

    try {
      auc = AuConfig.fromBackupLine("abc\tdef");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfig.fromBackupLine("abc\tdef ghi");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    bul = "abc\tdef\tghi";
    auc = AuConfig.fromBackupLine(bul);
    assertEquals("abc", auc.getAuid());
    assertEquals(1, auc.getConfiguration().size());
    assertEquals("ghi", auc.getConfiguration().get("def"));
    assertEquals(bul, auc.toBackupLine());

    try {
      auc = AuConfig.fromBackupLine("abc\tdef\tghi\tjkl");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    bul = "abc\tdef\tghi\tjkl\tmno";
    auc = AuConfig.fromBackupLine(bul);
    assertEquals("abc", auc.getAuid());
    assertEquals(2, auc.getConfiguration().size());
    assertEquals("ghi", auc.getConfiguration().get("def"));
    assertEquals("mno", auc.getConfiguration().get("jkl"));
    assertEquals(bul, auc.toBackupLine());
  }

  @Test
  public void testToBackupLine() {
    String auid = null;
    Map<String, String> configuration = null;
    AuConfig auc = new AuConfig(auid, configuration);
    assertEquals("", auc.toBackupLine());

    auid = "";
    auc = new AuConfig(auid, configuration);
    assertEquals(auid, auc.toBackupLine());

    auid = " ";
    auc = new AuConfig(auid, configuration);
    assertEquals(auid, auc.toBackupLine());

    auid = "auid";
    auc = new AuConfig(auid, configuration);
    assertEquals(auid, auc.toBackupLine());

    configuration = new HashMap<>();
    auc = new AuConfig(auid, configuration);
    assertEquals(auid, auc.toBackupLine());
    assertEquals(auc, AuConfig.fromBackupLine(auc.toBackupLine()));

    configuration.put("abc", "def");
    auc = new AuConfig(auid, configuration);
    assertEquals(auid + "\tabc\tdef", auc.toBackupLine());
    assertEquals(auc, AuConfig.fromBackupLine(auc.toBackupLine()));
  }

  @Test
  public void testToUnprefixedConfiguration() {
    String auid = null;
    Map<String, String> configuration = null;
    AuConfig auc = new AuConfig(auid, configuration);
    Configuration emptyConfiguration = ConfigManager.newConfiguration();
    assertEquals(emptyConfiguration, auc.toUnprefixedConfiguration());

    auid = "";
    auc = new AuConfig(auid, configuration);
    assertEquals(emptyConfiguration, auc.toUnprefixedConfiguration());

    auid = " ";
    auc = new AuConfig(auid, configuration);
    assertEquals(emptyConfiguration, auc.toUnprefixedConfiguration());

    auid = "auid";
    configuration = new HashMap<>();
    auc = new AuConfig(auid, configuration);
    assertEquals(emptyConfiguration, auc.toUnprefixedConfiguration());

    configuration.put("abc", "def");
    auc = new AuConfig(auid, configuration);
    assertEquals(1, auc.toUnprefixedConfiguration().keySet().size());
    assertEquals("def", auc.toUnprefixedConfiguration().get("abc"));

    configuration.put("ghi", "jkl");
    auc = new AuConfig(auid, configuration);
    assertEquals(2, auc.toUnprefixedConfiguration().keySet().size());
    assertEquals("def", auc.toUnprefixedConfiguration().get("abc"));
    assertEquals("jkl", auc.toUnprefixedConfiguration().get("ghi"));
  }

  @Test
  public void toAuidPrefixedConfiguration() {
    String auid = null;
    Map<String, String> configuration = null;
    AuConfig auc = new AuConfig(auid, configuration);
    Configuration emptyConfiguration = ConfigManager.newConfiguration();

    try {
      assertEquals(emptyConfiguration, auc.toAuidPrefixedConfiguration());
      fail("Should have thrown NullPointerException");
    } catch (NullPointerException npe) {
    }

    auid = "auid";
    auc = new AuConfig(auid, configuration);
    assertEquals(emptyConfiguration, auc.toAuidPrefixedConfiguration());

    configuration = new HashMap<>();
    auc = new AuConfig(auid, configuration);
    assertEquals(emptyConfiguration, auc.toAuidPrefixedConfiguration());

    configuration.put("abc", "def");
    auc = new AuConfig(auid, configuration);
    assertEquals(1, auc.toAuidPrefixedConfiguration().keySet().size());
    assertEquals("def", auc.toAuidPrefixedConfiguration().get("auid.abc"));

    configuration.put("ghi", "jkl");
    auc = new AuConfig(auid, configuration);
    assertEquals(2, auc.toAuidPrefixedConfiguration().keySet().size());
    assertEquals("def", auc.toAuidPrefixedConfiguration().get("auid.abc"));
    assertEquals("jkl", auc.toAuidPrefixedConfiguration().get("auid.ghi"));
  }
}