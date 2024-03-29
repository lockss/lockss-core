/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.alert;

import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;
import org.apache.commons.collections4.map.MultiValueMap;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;
import org.lockss.daemon.*;
import org.lockss.plugin.*;

/**
 * This is the test class for org.lockss.alert.Alert
 */
public class TestAlert extends LockssTestCase {
  private static final Logger log = Logger.getLogger();

  public void setUp() throws Exception {
    super.setUp();
  }

  public void tearDown() throws Exception {
    TimeBase.setReal();
    super.tearDown();
  }

  public void testNew() {
    Alert a1 = new Alert("foo");
    assertEquals("foo", a1.getName());
  }

  public void testNewMap() {
    Map m = newMap(Alert.ATTR_NAME, "bar", Alert.ATTR_TEXT, "contents");
    Alert a1 = new Alert("xxx", m);
    assertEquals("xxx", a1.getName());
    assertEquals("contents", a1.getAttribute(Alert.ATTR_TEXT));
    assertEquals("bar", m.get(Alert.ATTR_NAME));
  }

  public void testNewClone() {
    Map m = newMap(Alert.ATTR_NAME, "bar", Alert.ATTR_TEXT, "contents");
    Alert a1 = new Alert(m);
    Alert a2 = new Alert("nom", a1);
    assertEquals("nom", a2.getName());
    assertEquals("bar", a1.getName());
    a2.setAttribute(Alert.ATTR_TEXT, "text");
    assertEquals("contents", a1.getAttribute(Alert.ATTR_TEXT));
    assertEquals("text", a2.getAttribute(Alert.ATTR_TEXT));
  }

  public void testAttributes() {
    Alert a1 = new Alert("num");
    a1.setAttribute(Alert.ATTR_TEXT, "contents");
    assertTrue(a1.hasAttribute(Alert.ATTR_NAME));
    assertTrue(a1.hasAttribute(Alert.ATTR_TEXT));
    assertFalse(a1.hasAttribute("NonAtTriBute"));
    assertEquals("num", a1.getName());
    assertEquals("contents", a1.getAttribute(Alert.ATTR_TEXT));
    assertNull("NonAtTriBute", a1.getAttribute("NonAtTriBute"));
  }

  public void testDate() {
    TimeBase.setSimulated(1234);
    Alert a1 = new Alert("foo");
    assertEquals(TimeBase.nowMs(), a1.getLong(Alert.ATTR_DATE));
  }

  public void testTypes() {
    Alert a1 = new Alert("num");
    a1.setAttribute(Alert.ATTR_TEXT, "contents");
    a1.setAttribute(Alert.ATTR_SEVERITY, 7);
    assertEquals("contents", a1.getString(Alert.ATTR_TEXT));
    assertEquals(7, a1.getInt(Alert.ATTR_SEVERITY));
    a1.setAttribute("foo", true);
    assertTrue(a1.getBool("foo"));
    assertFalse(a1.getBool("missing"));
    a1.setAttribute("foo", false);
    assertFalse(a1.getBool("foo"));
  }

  public void testIllTypes() {
    Alert a1 = new Alert("num");
    a1.setAttribute("string", "contents");
    a1.setAttribute("int", 7);
    a1.setAttribute("bool", true);
    try {
      a1.getInt("string");
      fail("Should throw ClassCastException");
    } catch (ClassCastException e) {
    }
    try {
      a1.getLong("string");
      fail("Should throw ClassCastException");
    } catch (ClassCastException e) {
    }
    try {
      a1.getInt("missing");
      fail("Should throw RuntimeException");
    } catch (RuntimeException e) {
    }
    try {
      a1.getLong("missing");
      fail("Should throw RuntimeException");
    } catch (RuntimeException e) {
    }
    assertFalse(a1.getBool("int"));
  }

  public void testGetSeverityString() {
    Alert a1 = new Alert("num");
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_TRACE);
    assertEquals("trace", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_TRACE - 1);
    assertEquals("trace", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_INFO);
    assertEquals("info", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_WARNING);
    assertEquals("warning", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_ERROR);
    assertEquals("error", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_CRITICAL);
    assertEquals("critical", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_CRITICAL - 1);
    assertEquals("critical", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, Alert.SEVERITY_CRITICAL + 1);
    assertEquals("unknown", a1.getSeverityString());
    a1.setAttribute(Alert.ATTR_SEVERITY, "foo");
    assertEquals("unknown", a1.getSeverityString());
  }

  public void testIsSimilarTo() {
    MockPlugin mp1 = new MockPlugin().setPluginId("mp1");
    MockPlugin mp2 = new MockPlugin().setPluginId("mp2");

    MockArchivalUnit mau1 = new MockArchivalUnit(mp1, "auid1");
    MockArchivalUnit mau2 = new MockArchivalUnit(mp2, "auid22");
    MockArchivalUnit mau3 = new MockArchivalUnit(mp1, "auid333");

    Alert fin1 = Alert.auAlert(Alert.CRAWL_FINISHED, mau1);
    Alert fin2 = Alert.auAlert(Alert.CRAWL_FINISHED, mau2);
    Alert fin3 = Alert.auAlert(Alert.CRAWL_FINISHED, mau3);
    Alert fail1 = Alert.auAlert(Alert.CRAWL_FAILED, mau1);
    Alert fail2 = Alert.auAlert(Alert.CRAWL_FAILED, mau2);
    Alert fail3 = Alert.auAlert(Alert.CRAWL_FAILED, mau3);
    Alert other = Alert.auAlert(Alert.CRAWL_EXCLUDED_URL, mau2);

    assertTrue(fin1.isSimilarTo(fin1));
    assertTrue(fin1.isSimilarTo(fin2));
    assertTrue(fin1.isSimilarTo(fail1));
    assertTrue(fin1.isSimilarTo(fail2));

    assertFalse(fin1.isSimilarTo(other));
    assertTrue(other.isSimilarTo(other));

    assertEquals(fin1.getGroupKey(), fin2.getGroupKey());
    assertEquals(fin1.getGroupKey(), fail1.getGroupKey());
    assertEquals(fin1.similarityHash(), fin1.similarityHash());
    assertEquals(fin1.similarityHash(), fin2.similarityHash());
    assertEquals(fin1.similarityHash(), fail1.similarityHash());
    assertEquals(fin1.similarityHash(), fail2.similarityHash());

    ConfigurationUtil.addFromArgs(Alert.PARAM_SPECIAL_GROUPS,
				  "CrawlFinished,au:foo;CrawlFailed,au:foo");

    assertNotEquals(fin1.getGroupKey(), fin2.getGroupKey());
    assertEquals(fin1.getGroupKey(), fail1.getGroupKey());
    assertNotEquals(fin1.getGroupKey(), fail3.getGroupKey());
    assertEquals(fin1.similarityHash(), fin1.similarityHash());
    assertNotEquals(fin1.similarityHash(), fin2.similarityHash());
    assertEquals(fin1.similarityHash(), fail1.similarityHash());
    assertNotEquals(fin1.similarityHash(), fail2.similarityHash());

    ConfigurationUtil.addFromArgs(Alert.PARAM_SPECIAL_GROUPS,
				  "CrawlFinished,plugin:bar;CrawlFailed,plugin:bar");

    assertEquals(fin1.getGroupKey(), fin3.getGroupKey());
    assertEquals(fin1.getGroupKey(), fail3.getGroupKey());
    assertNotEquals(fin1.similarityHash(), fin2.similarityHash());
    assertEquals(fin1.similarityHash(), fail3.similarityHash());
  }

  public void testGroupHash() {
    Map map = new MultiValueMap();
    Alert a1 = new Alert("Name1")
      .setAttribute(Alert.ATTR_IS_CONTENT, true)
      .setAttribute(Alert.ATTR_AUID, "AUID 1")
      .setAttribute(Alert.ATTR_TEXT, "test 1111");
    Alert a2 = new Alert("Name1")
      .setAttribute(Alert.ATTR_IS_CONTENT, true)
      .setAttribute(Alert.ATTR_AUID, "AUID 1")
      .setAttribute(Alert.ATTR_TEXT, "test 2222");
    Alert a3 = new Alert("Name2")
      .setAttribute(Alert.ATTR_IS_CONTENT, true)
      .setAttribute(Alert.ATTR_AUID, "AUID 1")
      .setAttribute(Alert.ATTR_TEXT, "test 2222");
    Alert a4 = new Alert("Name2")
      .setAttribute(Alert.ATTR_IS_CONTENT, true)
      .setAttribute(Alert.ATTR_AUID, "AUID 2")
      .setAttribute(Alert.ATTR_TEXT, "test 1111");
    map.put(a1.getGroupKey(), a1);
    map.put(a2.getGroupKey(), a2);
    map.put(a3.getGroupKey(), a3);
    map.put(a4.getGroupKey(), a4);
    assertEquals(ListUtil.list(a1, a2), map.get(a1.getGroupKey()));
    assertEquals(ListUtil.list(a3), map.get(a3.getGroupKey()));
    assertEquals(ListUtil.list(a4), map.get(a4.getGroupKey()));
    assertEquals(ListUtil.list(a1, a2), map.get(a1.getGroupKey()));
  }


  public void testAuAlert() {
    MockArchivalUnit mau = new MockArchivalUnit();
    mau.setAuId("au_foo");
    Alert a = Alert.auAlert(Alert.REPAIR_COMPLETE, mau);

    assertEquals("RepairComplete", a.getName());
    assertEquals("au_foo", a.getString(Alert.ATTR_AUID));
    assertEquals("MockAU", a.getString(Alert.ATTR_AU_NAME));
    assertTrue(a.getBool(Alert.ATTR_IS_CONTENT));
  }

  public void testCacheAlert() {
    assertFalse(Alert.cacheAlert(Alert.CACHE_DOWN)
		.getBool(Alert.ATTR_IS_CONTENT));
  }

  public void testToString() {
    Alert a1 = new Alert("TestAlert");
    a1.setAttribute(Alert.ATTR_TEXT, "Explanatory text");
    a1.setAttribute(Alert.ATTR_SEVERITY, 7);
    log.debug(a1.toString());
  }

  public void testEquals() {
    // Prevent timestamps from causing alerts not to be equal
    TimeBase.setSimulated(1000);
    MockArchivalUnit mau1 = new MockArchivalUnit("au_foo");
    MockArchivalUnit mau2 = new MockArchivalUnit("au_bar");

    Alert a1 = Alert.auAlert(Alert.REPAIR_COMPLETE, mau1);
    Alert a2 = Alert.auAlert(Alert.REPAIR_COMPLETE, mau1);
    Alert a3 = Alert.auAlert(Alert.REPAIR_COMPLETE, mau2);

    assertEquals(a1, a2);
    assertNotEquals(a1, a3);

    a2.setAttribute(Alert.ATTR_TEXT, "foo");
    assertNotEquals(a1, a2);
  }

  public void testGetMailBody() {
    Alert a1 = new Alert("TestAlert");
    a1.setAttribute(Alert.ATTR_TEXT, "Explanatory text");
    a1.setAttribute(Alert.ATTR_SEVERITY, 7);
    log.debug(a1.getMailBody());

    MockArchivalUnit mau = new MockArchivalUnit();
    mau.setAuId("au_foo");
    Alert a2 = Alert.auAlert(Alert.REPAIR_COMPLETE, mau);
    a2.setAttribute(Alert.ATTR_TEXT, "Explanatory text");
    a2.setAttribute(Alert.ATTR_SEVERITY, 7);
    a2.setAttribute(Alert.ATTR_COMPONENT_NAME, "Poller Service");
    String body = a2.getMailBody();
    log.debug(body);
    String[] lbody =
      (String[])StringUtil.breakAt(body, '\n').toArray(new String[0]);
    int line = 0;
    assertMatchesRE("^LOCKSS box: .*", lbody[line++]);
    assertMatchesRE("^Component: Poller Service", lbody[line++]);
    assertMatchesRE("^Time: ", lbody[line++]);
    assertMatchesRE("^$", lbody[line++]);
    assertMatchesRE("^Name: RepairComplete$", lbody[line++]);
    assertMatchesRE("^Severity: trace$", lbody[line++]);
    assertMatchesRE("^AU: MockAU$", lbody[line++]);
    assertMatchesRE("^AUID: au_foo", lbody[line++]);
    assertMatchesRE("^Explanation: Explanatory text$", lbody[line++]);
  }

  public void testGetMailSubject() {
    Alert a1 = new Alert("TestAlert");
    a1.setAttribute(Alert.ATTR_TEXT, "Explanatory text");
    a1.setAttribute(Alert.ATTR_SEVERITY, 7);
    log.debug(a1.getMailSubject());

    MockArchivalUnit mau = new MockArchivalUnit();
    mau.setAuId("au_foo");
    Alert a2 = Alert.auAlert(Alert.REPAIR_COMPLETE, mau);
    a2.setAttribute(Alert.ATTR_TEXT, "Explanatory text");
    a2.setAttribute(Alert.ATTR_SEVERITY, 7);
    String subject = a2.getMailSubject();
    log.debug(subject);
    assertEquals("LOCKSS alert: RepairComplete", subject);
  }

  Map newMap(String prop, Object val) {
    HashMap map = new HashMap();
    map.put(prop, val);
    return map;
  }

  Map newMap(String prop1, Object val1, String prop2, String val2) {
    HashMap map = new HashMap();
    map.put(prop1, val1);
    map.put(prop2, val2);
    return map;
  }
}
