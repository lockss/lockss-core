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
 * Test class for org.lockss.config.AuConfigurationUtils.
 */
public class TestAuConfigurationUtils extends LockssTestCase5 {
  @Test
  public void testAuConfigFromConfiguration() {
    String auPropKey = null;
    Configuration auConfiguration = null;
    AuConfiguration auc = null;

    try {
      auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = "";

    try {
      auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = " ";

    try {
      auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = "org.lockss.au.foo.auid";

    try {
      auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auConfiguration = ConfigManager.fromProperties(new Properties());

    try {
      auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
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
      auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auPropKey = "org.lockss.au.foo.auid";
    auc = AuConfigurationUtils.fromConfiguration(auPropKey, auConfiguration);
    assertEquals("foo&auid", auc.getAuId());
    assertEquals(3, auc.getAuConfig().size());
    assertEquals("111", auc.getAuConfig().get("foo"));
    assertEquals("222", auc.getAuConfig().get("bar"));
    assertEquals("333", auc.getAuConfig().get("baz"));
  }

  @Test
  public void testFromBackupLine() {
    AuConfiguration auc = null;

    try {
      auc = AuConfigurationUtils.fromBackupLine(null);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfigurationUtils.fromBackupLine("");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfigurationUtils.fromBackupLine(" ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfigurationUtils.fromBackupLine("abc");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfigurationUtils.fromBackupLine("abc def");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfigurationUtils.fromBackupLine("abc"
	  + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "def");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      auc = AuConfigurationUtils.fromBackupLine("abc"
	  + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "def ghi");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    String bul = "abc" + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR
	+ "def" + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "ghi";
    auc = AuConfigurationUtils.fromBackupLine(bul);
    assertEquals("abc", auc.getAuId());
    assertEquals(1, auc.getAuConfig().size());
    assertEquals("ghi", auc.getAuConfig().get("def"));
    assertEquals(bul, AuConfigurationUtils.toBackupLine(auc));

    try {
      auc = AuConfigurationUtils.fromBackupLine("abc"
	  + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "def"
	  + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "ghi"
	  + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "jkl");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    bul = "abc" + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "def"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "ghi"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "jkl"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "mno";
    auc = AuConfigurationUtils.fromBackupLine(bul);
    assertEquals("abc", auc.getAuId());
    assertEquals(2, auc.getAuConfig().size());
    assertEquals("ghi", auc.getAuConfig().get("def"));
    assertEquals("mno", auc.getAuConfig().get("jkl"));
    assertEquals(bul, AuConfigurationUtils.toBackupLine(auc));
  }

  @Test
  public void testToBackupLine() {
    String auid = "auid";
    Map<String, String> configuration = new HashMap<>();
    configuration.put("abc", "def");

    AuConfiguration auc = new AuConfiguration(auid, configuration);
    assertEquals(auid + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "abc"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "def",
	AuConfigurationUtils.toBackupLine(auc));
    assertEquals(auc, AuConfigurationUtils
	.fromBackupLine(AuConfigurationUtils.toBackupLine(auc)));

    configuration.put("ghi", "jkl");

    auc = new AuConfiguration(auid, configuration);
    assertEquals(auid + AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "abc"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "def"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "ghi"
	+ AuConfigurationUtils.BACKUP_LINE_FIELD_SEPARATOR + "jkl",
	AuConfigurationUtils.toBackupLine(auc));
    assertEquals(auc, AuConfigurationUtils
	.fromBackupLine(AuConfigurationUtils.toBackupLine(auc)));
  }

  @Test
  public void testToUnprefixedConfiguration() {
    String auid = "auid";
    Map<String, String> configuration = new HashMap<>();
    configuration.put("abc", "def");

    AuConfiguration auc = new AuConfiguration(auid, configuration);
    assertEquals(1,
	AuConfigurationUtils.toUnprefixedConfiguration(auc).keySet().size());
    assertEquals("def",
	AuConfigurationUtils.toUnprefixedConfiguration(auc).get("abc"));

    configuration.put("ghi", "jkl");
    auc = new AuConfiguration(auid, configuration);
    assertEquals(2,
	AuConfigurationUtils.toUnprefixedConfiguration(auc).keySet().size());
    assertEquals("def",
	AuConfigurationUtils.toUnprefixedConfiguration(auc).get("abc"));
    assertEquals("jkl",
	AuConfigurationUtils.toUnprefixedConfiguration(auc).get("ghi"));
  }

  @Test
  public void toAuidPrefixedConfiguration() {
    String auid = "auid";
    Map<String, String> configuration = new HashMap<>();
    configuration.put("abc", "def");

    AuConfiguration auc = new AuConfiguration(auid, configuration);
    assertEquals(1,
	AuConfigurationUtils.toAuidPrefixedConfiguration(auc).keySet().size());
    assertEquals("def",
	AuConfigurationUtils.toAuidPrefixedConfiguration(auc).get("auid.abc"));

    configuration.put("ghi", "jkl");
    auc = new AuConfiguration(auid, configuration);
    assertEquals(2,
	AuConfigurationUtils.toAuidPrefixedConfiguration(auc).keySet().size());
    assertEquals("def",
	AuConfigurationUtils.toAuidPrefixedConfiguration(auc).get("auid.abc"));
    assertEquals("jkl",
	AuConfigurationUtils.toAuidPrefixedConfiguration(auc).get("auid.ghi"));
  }
}
