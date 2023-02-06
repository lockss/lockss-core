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

import java.util.*;
import org.junit.Test;
import org.lockss.util.*;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for org.lockss.config.AuConfiguration.
 */
public class TestAuConfiguration extends LockssTestCase5 {
  @Test
  public void testAuConfig() {
    String auid = null;
    Map<String, String> configuration = null;
    AuConfiguration auc = null;

    try {
      auc = new AuConfiguration(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = "";

    try {
      auc = new AuConfiguration(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = " ";

    try {
      auc = new AuConfiguration(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = "abc";

    try {
      auc = new AuConfiguration(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    configuration = new HashMap<>();

    try {
      auc = new AuConfiguration(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = " ";
    configuration.put("abc", "def");

    try {
      auc = new AuConfiguration(auid, configuration);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    auid = "abc";
    auc = new AuConfiguration(auid, configuration);
    assertEquals(auid, auc.getAuId());
    assertEquals(configuration, auc.getAuConfig());

    configuration.put("ghi", "jkl");
    auc = new AuConfiguration(auid, configuration);
    assertEquals(auid, auc.getAuId());
    assertEquals(configuration, auc.getAuConfig());
  }

  String ns(String s) {
    return new String(s);
  }

  @Test
  public void testIntern() {
    String auid = "au|id|1";
    String k1 = "foo";
    String v1 = "oof";
    String k2 = "bar";
    String v2 = "rab";
    Map m1 = MapUtil.map(k1, v1, k2, v2);
    Map m2 = MapUtil.map(ns(k1), ns(v1), ns(k2), ns(v2));
    AuConfiguration auc1 = new AuConfiguration(auid, m1);
    AuConfiguration auc2 = new AuConfiguration(ns(auid), m2);
    assertSame(auc1.getAuId(), auc2.getAuId());
    Map<String,String> om1 = auc1.getAuConfig();
    Map<String,String> om2 = auc2.getAuConfig();
    assertNotSame(om1, om2);
    assertEquals(om1, om2);
    assertSame(om1.get("k1"), om2.get("k1"));
    assertSame(om1.get("k2"), om2.get("k2"));
    List<String> om1ks = new ArrayList(om1.keySet());
    // check that keys are interned
    for (String key : om2.keySet()) {
      assertTrue(key == om1ks.get(0) || key == om1ks.get(1));
    }
  }
}
