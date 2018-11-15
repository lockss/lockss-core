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
  public void testAuConfig() {
    String auid = null;
    Map<String, String> configuration = null;
    AuConfig auc = null;

    try {
      auc = new AuConfig(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = "";

    try {
      auc = new AuConfig(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = " ";

    try {
      auc = new AuConfig(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = "abc";

    try {
      auc = new AuConfig(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    configuration = new HashMap<>();

    try {
      auc = new AuConfig(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = " ";
    configuration.put("abc", "def");

    try {
      auc = new AuConfig(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = "abc";
    auc = new AuConfig(auid, configuration);
    assertEquals(auid, auc.getAuid());
    assertEquals(configuration, auc.getConfiguration());

    configuration.put("ghi", "jkl");
    auc = new AuConfig(auid, configuration);
    assertEquals(auid, auc.getAuid());
    assertEquals(configuration, auc.getConfiguration());
  }

  @Test
  public void testAuConfigFromConfiguration() {
    String auPropKey = null;
    Configuration auConfiguration = null;
    AuConfig auc = null;

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = "";

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = " ";

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = "org.lockss.au.foo.auid";

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auConfiguration = ConfigManager.fromProperties(new Properties());

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    Properties p = new Properties();
    p.put(auPropKey + ".foo", "111");
    p.put(auPropKey + ".bar", "222");
    p.put(auPropKey + ".baz", "333");
    auConfiguration = ConfigManager.fromProperties(p);

    auPropKey = " ";

    try {
      auc = new AuConfig(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = "org.lockss.au.foo.auid";
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

    try {
      auc = AuConfig.fromBackupLine(" ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfig.fromBackupLine("abc");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfig.fromBackupLine("abc def");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfig.fromBackupLine("abc" + AuConfig.BACKUP_LINE_FIELD_SEPARATOR
	  + "def");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfig.fromBackupLine("abc" + AuConfig.BACKUP_LINE_FIELD_SEPARATOR
	  + "def ghi");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    String bul = "abc" + AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "def"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "ghi";
    auc = AuConfig.fromBackupLine(bul);
    assertEquals("abc", auc.getAuid());
    assertEquals(1, auc.getConfiguration().size());
    assertEquals("ghi", auc.getConfiguration().get("def"));
    assertEquals(bul, auc.toBackupLine());

    try {
      auc = AuConfig.fromBackupLine("abc" + AuConfig.BACKUP_LINE_FIELD_SEPARATOR
	  + "def" + AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "ghi"
	  + AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "jkl");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    bul = "abc" + AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "def"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "ghi"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "jkl"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "mno";
    auc = AuConfig.fromBackupLine(bul);
    assertEquals("abc", auc.getAuid());
    assertEquals(2, auc.getConfiguration().size());
    assertEquals("ghi", auc.getConfiguration().get("def"));
    assertEquals("mno", auc.getConfiguration().get("jkl"));
    assertEquals(bul, auc.toBackupLine());
  }

  @Test
  public void testToBackupLine() {
    String auid = "auid";
    Map<String, String> configuration = new HashMap<>();
    configuration.put("abc", "def");

    AuConfig auc = new AuConfig(auid, configuration);
    assertEquals(auid + AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "abc"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "def", auc.toBackupLine());
    assertEquals(auc, AuConfig.fromBackupLine(auc.toBackupLine()));

    configuration.put("ghi", "jkl");

    auc = new AuConfig(auid, configuration);
    assertEquals(auid + AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "abc"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "def"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "ghi"
	+ AuConfig.BACKUP_LINE_FIELD_SEPARATOR + "jkl", auc.toBackupLine());
    assertEquals(auc, AuConfig.fromBackupLine(auc.toBackupLine()));
  }

  @Test
  public void testToUnprefixedConfiguration() {
    String auid = "auid";
    Map<String, String> configuration = new HashMap<>();
    configuration.put("abc", "def");

    AuConfig auc = new AuConfig(auid, configuration);
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
    String auid = "auid";
    Map<String, String> configuration = new HashMap<>();
    configuration.put("abc", "def");

    AuConfig auc = new AuConfig(auid, configuration);
    assertEquals(1, auc.toAuidPrefixedConfiguration().keySet().size());
    assertEquals("def", auc.toAuidPrefixedConfiguration().get("auid.abc"));

    configuration.put("ghi", "jkl");
    auc = new AuConfig(auid, configuration);
    assertEquals(2, auc.toAuidPrefixedConfiguration().keySet().size());
    assertEquals("def", auc.toAuidPrefixedConfiguration().get("auid.abc"));
    assertEquals("jkl", auc.toAuidPrefixedConfiguration().get("auid.ghi"));
  }
}