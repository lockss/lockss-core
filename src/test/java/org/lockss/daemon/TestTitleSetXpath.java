/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

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

package org.lockss.daemon;

import java.io.*;
import java.util.*;
import org.apache.commons.jxpath.*;
import org.lockss.config.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.plugin.*;

/**
 * This is the test class for org.lockss.daemon.TitleSetXpath
 */

public class TestTitleSetXpath extends LockssTestCase {
  private static Logger log = Logger.getLogger();

  PluginManager pluginMgr;
  MockPlugin mp1;
  MockPlugin mp2;
  TitleConfig tc1, tc2, tc3, tc4, tc5, tc6;
  List<TitleConfig> titles;

  public void setUp() throws Exception {
    super.setUp();

    String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    Properties p = new Properties();
    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);
    ConfigurationUtil.setCurrentConfigFromProps(p);

    getMockLockssDaemon().setUpAuConfig();

    useOldRepo();
    pluginMgr = getMockLockssDaemon().getPluginManager();
    setUpDiskSpace();
    makeTitles();
  }

  public void tearDown() throws Exception {
    getMockLockssDaemon().stopDaemon();
    super.tearDown();
  }

  public void makeTitles() {
    titles = new ArrayList<TitleConfig>();
    String pid1 = "o.l.plug1";
    String pid2 = "o.l.plug2";
    tc1 = new TitleConfig("Journal of Title, 2001", pid1);
    tc1.setJournalTitle("Journal of Title");
    tc2 = new TitleConfig("Journal of Title, 2002", pid1);
    tc2.setJournalTitle("Journal of Title");
    tc3 = new TitleConfig("Journal of Title, 2003", pid1);
    tc3.setJournalTitle("Journal of Title");
    tc4 = new TitleConfig("Journal of Title, 2004", pid2);
    tc4.setJournalTitle("Journal of Title");
    tc5 = new TitleConfig("LOCKSS Journal of Dogs", pid2);
    tc5.setJournalTitle("Dog Journal");
    tc6 = new TitleConfig("LOCKSS Journal of Dog Toys 2002", pid2);
    tc6.setJournalTitle("Dog Journal");

    ConfigParamDescr d1 =new ConfigParamDescr("key1");
    ConfigParamDescr d2 =new ConfigParamDescr("key2");

    tc5.setAttributes(MapUtil.map("key1", "val1", "k0", "1"));
    tc6.setAttributes(MapUtil.map("key1", "val2", "k0", "1"));

    titles = ListUtil.list(tc1, tc2, tc3, tc4, tc5, tc6);
  }

  TitleSetXpath newSet0(String pred) {
    return new TitleSetXpath(getMockLockssDaemon(), "Name1", pred);
  }

  TitleSetXpath newSet(String pred) {
    return TitleSetXpath.create(getMockLockssDaemon(), "Name1", pred);
  }

  public void testConstructor() {
    MockLockssDaemon daemon = getMockLockssDaemon();
    TitleSetXpath ts = new TitleSetXpath(daemon, "Name1", "[path1]");
    assertEquals(".[path1]", ts.getPath());
    // Checked in TitleSetXpath constructor
    try {
      new TitleSetXpath(daemon, "Name1", null);
      fail("Null path should be illegal");
    } catch (NullPointerException e) {
    }
    try {
      new TitleSetXpath(daemon, "Name1", "path1]");
      fail("Pattern should be illegal");
    } catch (IllegalArgumentException e) {
    }
    try {
      new TitleSetXpath(daemon, "Name1", "[path1");
      fail("Pattern should be illegal");
    } catch (IllegalArgumentException e) {
    }
    // Illegal predicate syntax, checked in JXPath code
    try {
      new TitleSetXpath(daemon, "Name1", "[journalTitle='Dog Journal']]");
      fail("Illegal xpath, should have thrown");
    } catch (JXPathException e) {
    }
  }

  public void testEquals() {
    MockLockssDaemon daemon = getMockLockssDaemon();
    assertEquals(new TitleSetXpath(daemon, "Name1", "[path1]"),
		 new TitleSetXpath(daemon, "Name1", "[path1]"));
    assertNotEquals(new TitleSetXpath(daemon, "Name1", "[path1]"),
		    new TitleSetXpath(daemon, "Name2", "[path1]"));
    assertNotEquals(new TitleSetXpath(daemon, "Name1", "[path1]"),
		    new TitleSetXpath(daemon, "Name1", "[path2]"));
    assertNotEquals(new TitleSetXpath(daemon, "Name1", "[path1]"),
		    new TitleSetActiveAus(daemon));
    assertNotEquals(new TitleSetXpath(daemon, "Name1", "[path1]"),
		    "foo");
  }

  public void testSimplePat() {
    TitleSetXpath ts = newSet("[journalTitle='Dog Journal']");
    assertEquals(SetUtil.set(tc5, tc6),
		 SetUtil.theSet(ts.filterTitles(titles)));
    assertEquals("Name1", ts.getName());
  }

  public void testComplexPat() {
    TitleSetXpath ts = newSet("[attributes/key1='val1']");
    assertEquals(SetUtil.set(tc5),
		 SetUtil.theSet(ts.filterTitles(titles)));
    assertEquals("Name1", ts.getName());
    ts = newSet("[attributes/k0='1']");
    assertEquals(SetUtil.set(tc5, tc6),
		 SetUtil.theSet(ts.filterTitles(titles)));
    ts = newSet("[journalTitle='Dog Journal' and attributes/key1='val1']");
    assertEquals(SetUtil.set(tc5),
		 SetUtil.theSet(ts.filterTitles(titles)));
  }

  // Ensure that TitleConfig bean accessor names are as we expect.  Because
  // JXPath accesses them at runtime using reflection, renaming an accessor
  // wouldn't otherwise break the build.

  public void testAllFields() {
    assertEquals(SetUtil.set(tc5, tc6),
		 SetUtil.theSet(newSet("[journalTitle='Dog Journal']").filterTitles(titles)));
    assertEquals(SetUtil.set(tc1),
		 SetUtil.theSet(newSet("[displayName='Journal of Title, 2001']").filterTitles(titles)));
    assertEquals(SetUtil.set(tc1, tc2, tc3),
		 SetUtil.theSet(newSet("[pluginName='o.l.plug1']").filterTitles(titles)));
  }

  public void testFunc() {
    TitleSetXpath ts = newSet("[starts-with(journalTitle, \"Dog\")]");
    assertEquals(SetUtil.set(tc5, tc6),
		 SetUtil.theSet(ts.filterTitles(titles)));
  }

  public void testBool() {
    TitleSetXpath ts;
    ts = newSet("[journalTitle=\"Dog Journal\" or pluginName=\"o.l.plug2\"]");
    assertEquals(SetUtil.set(tc4, tc5, tc6),
		 SetUtil.theSet(ts.filterTitles(titles)));
    ts = newSet("[journalTitle='Journal of Title' and pluginName='o.l.plug2']");
    assertEquals(SetUtil.set(tc4),
		 SetUtil.theSet(ts.filterTitles(titles)));
    ts = newSet("[journalTitle='Journal of Title' and (pluginName='o.l.plug2' or RE:isMatchRe(displayName, '2002'))]");
    assertEquals(SetUtil.set(tc2, tc4),
		 SetUtil.theSet(ts.filterTitles(titles)));
  }

  public void testRe() {
    TitleSetXpath ts = newSet("[RE:isMatchRe(displayName, \"D.g[^s]\")]");
    assertEquals(SetUtil.set(tc6),
		 SetUtil.theSet(ts.filterTitles(titles)));
  }

  // forgetting to quote regexp shouldn't cause accidental matches
  public void testNullRe() {
    TitleSetXpath ts = newSet("[RE:isMatchRe(displayName, Dog)]");
    assertEmpty(SetUtil.theSet(ts.filterTitles(titles)));
    ts = newSet("[RE:isMatchRe(displayName, '')]");
    assertEmpty(SetUtil.theSet(ts.filterTitles(titles)));
  }

  // Illegal RE causes failure when TitleSet is used, not when created
  public void testIllRe() {
    TitleSetXpath ts = newSet("[RE:isMatchRe(displayName, 'a[ab')]");
    try {
      assertEmpty(SetUtil.theSet(ts.filterTitles(titles)));
      fail("Illegal regexp, should have thrown");
    } catch (JXPathException e) {
    }
  }


  public void testOptimizedPlugin() {
    TitleSetXpath tsp1 = newSet("[pluginName='o.l.plug2']");
    assertClass(TitleSetXpath.TSPlugin.class, tsp1);
    TitleSetXpath tsp2 = newSet("[pluginName=\"o.l.plug2\"]");
    assertClass(TitleSetXpath.TSPlugin.class, tsp2);
    TitleSetXpath tsp3 = newSet("[pluginName='o.l.plug2' or attributes/X='Y']");
    assertNotClass(TitleSetXpath.TSPlugin.class, tsp3);

    assertSameElements(ListUtil.list(tc4, tc5, tc6), tsp1.filterTitles(titles));
    assertSameElements(ListUtil.list(tc4, tc5, tc6), tsp2.filterTitles(titles));
    assertSameElements(ListUtil.list(tc4, tc5, tc6), tsp3.filterTitles(titles));
  }

  public void testOptimizedAttr() throws Exception {
    TitleSetXpath tsa1 = newSet("[attributes/key1='val1']");
    assertClass(TitleSetXpath.TSAttr.class, tsa1);
    TitleSetXpath tsa2 = newSet("[attributes/key1=\"val1\"]");
    assertClass(TitleSetXpath.TSAttr.class, tsa2);
    TitleSetXpath tsa3 = newSet("[attributes/key1='val1' or false]");
    assertNotClass(TitleSetXpath.TSAttr.class, tsa3);

    assertSameElements(ListUtil.list(tc5), tsa1.filterTitles(titles));
    assertEquals(0, tsa1.countTitles(TitleSet.SET_ADDABLE));
    assertSameElements(ListUtil.list(tc5), tsa2.filterTitles(titles));
    assertSameElements(ListUtil.list(tc5), tsa3.filterTitles(titles));
  }

  public void testFoo() throws Exception {
    TdbTestUtil.makeTestTdb().prettyPrint(System.out);
  }

  String pname = "org.lockss.daemon.TitlesetTestPlugin";

  public void testSample() throws Exception {
    pluginMgr.startService();  // deactivateAu() below requires service runnning
    pluginMgr.ensurePluginLoaded(pluginMgr.pluginKeyFromName(pname));
    final Plugin plug = pluginMgr.getPluginFromId(pname);

    ConfigurationUtil.addFromUrl(getResource("sample1.xml"));
    ConfigManager.getCurrentConfig().getTdb().prettyPrint(System.out);

    TitleSetXpath tsa1 =
      newSet("[attributes/publisher='Springfield Free Press']");
    assertEquals(8, tsa1.countTitles(TitleSet.SET_ADDABLE));
    assertEquals(8, tsa1.getTitles().size());
    final List<String> ptitles = plug.getSupportedTitles();
    List<TitleConfig> lst = new ArrayList<TitleConfig>() {{
	for (String t : ptitles) {
	  TitleConfig tc = plug.getTitleConfig(t);
	  if (tc.getAttributes().get("publisher").startsWith("Springfield")) {
	    add(tc);
	  }
	}
      }};
    assertEquals(8, lst.size());
    assertSameElements(lst, tsa1.getTitles());

    // Create an AU matching this TitleConfig
    TitleConfig tc = lst.get(3);
    ArchivalUnit au =
      PluginTestUtil.createAndStartAu(plug.getPluginId(), tc.getConfig());
    assertEquals(tc.getAuId(pluginMgr, plug), au.getAuId());
    assertEquals(7, tsa1.countTitles(TitleSet.SET_ADDABLE));
    assertEquals(8, tsa1.getTitles().size());

    ArchivalUnit au2 =
      PluginTestUtil.createAndStartAu(plug.getPluginId(),
				      lst.get(1).getConfig());
    assertEquals(6, tsa1.countTitles(TitleSet.SET_ADDABLE));
    assertEquals(8, tsa1.getTitles().size());

    // Deactivate an AU, ensure it still isn't counted as addable
    pluginMgr.deactivateAu(au);
    assertEquals(6, tsa1.countTitles(TitleSet.SET_ADDABLE));
    assertEquals(8, tsa1.getTitles().size());


  }

}
