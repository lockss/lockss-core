/*

Copyright (c) 2000 - 2018, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.plugin;

import java.util.*;

import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.AuEvent.ContentChangeInfo;
import org.lockss.util.*;
import org.lockss.test.*;

/**
 * Test class for org.lockss.plugin.PluginManager
 */
public class TestAuEvent extends LockssTestCase {

  MockArchivalUnit mau1 = new MockArchivalUnit("auid1");
  MockArchivalUnit mau2 = new MockArchivalUnit("auid2");


  Map empty = MapUtil.map("num_urls", 0,
			  "complete", false);
  Map mimeMap = MapUtil.map("text/html", 4, "img/png", 2);

  public void testChInfoEquals() {
    ContentChangeInfo ci1 = new ContentChangeInfo();
    ContentChangeInfo ci2 = new ContentChangeInfo();
    assertEquals(ci1, ci2);
    ci1.setType(ContentChangeInfo.Type.Crawl);
    assertNotEquals(ci1, ci2);
    ci2.setType(ContentChangeInfo.Type.Repair);
    assertNotEquals(ci1, ci2);
    ci2.setType(ContentChangeInfo.Type.Crawl);
    assertEquals(ci1, ci2);
    ci1.setComplete(true);
    assertNotEquals(ci1, ci2);
    ci2.setComplete(true);
    assertEquals(ci1, ci2);

    ci1.setNumUrls(17);
    assertNotEquals(ci1, ci2);
    ci2.setNumUrls(17);
    assertEquals(ci1, ci2);

    ci1.setUrls(ListUtil.list("one", "two"));
    assertNotEquals(ci1, ci2);
    ci2.setUrls(ListUtil.list("two", "one"));
    assertNotEquals(ci1, ci2);
    ci2.setUrls(ListUtil.list("one", "two"));
    assertEquals(ci1, ci2);

    ci1.setMimeCounts(mimeMap);
    assertNotEquals(ci1, ci2);
    ci2.setMimeCounts(MapUtil.map("text/html", 3, "img/png", 2));
    assertNotEquals(ci1, ci2);
    ci2.setMimeCounts(MapUtil.map("text/html", 4, "img/png", 2));
    assertEquals(ci1, ci2);
}

  public void testChInfoToFromMap() {
    ContentChangeInfo ci1, ci2;
    Map map;

    ci1 = new ContentChangeInfo();
    map = ci1.toMap();
    assertEquals(empty, map);

    ci2 = ContentChangeInfo.fromMap(map);
    assertEquals(ci1, ci2);

    ci1.setType(ContentChangeInfo.Type.Crawl);
    map = ci1.toMap();
    assertEquals(MapUtil.map("type", "Crawl",
			     "num_urls", 0,
			     "complete", false),
		 map);

    ci1.setComplete(true);
    ci1.setNumUrls(57);
    Map mimeMap = MapUtil.map("text/html", 4, "img/png", 2);
    ci1.setMimeCounts(mimeMap);
    map = ci1.toMap();
    assertEquals(MapUtil.map("type", "Crawl",
			     "num_urls", 57,
			     "mime_counts", mimeMap,
			     "complete", true),
		 map);

  }

  public void testEquals() {
    AuEvent e1 = AuEvent.forAu(mau1, AuEvent.Type.Create);
    AuEvent e2 = AuEvent.forAu(mau1, AuEvent.Type.Create);
    AuEvent e3 = AuEvent.forAu(mau2, AuEvent.Type.Create);
    AuEvent e4 = AuEvent.forAu(mau1, AuEvent.Type.Delete);
    assertEquals(e1, e2);
    assertNotEquals(e1, e3);
    assertNotEquals(e1, e4);

    e1.setInBatch(true);
    assertNotEquals(e1, e2);
    e2.setInBatch(true);
    assertEquals(e1, e2);

    ContentChangeInfo ci1 = new ContentChangeInfo();
    ContentChangeInfo ci2 = new ContentChangeInfo();
    assertEquals(ci1, ci2);
    ci1.setType(ContentChangeInfo.Type.Crawl);
    ci1.setNumUrls(4);
    e1.setChangeInfo(ci1);
    assertNotEquals(e1, e2);
    e2.setChangeInfo(ci2);
    assertNotEquals(e1, e2);
    ci2.setType(ContentChangeInfo.Type.Crawl);
    assertNotEquals(e1, e2);
    ci2.setNumUrls(4);
    assertEquals(e1, e2);

    e1.setOldConfiguration(ConfigurationUtil.fromArgs("aa", "cc"));
    assertNotEquals(e1, e2);
    e2.setOldConfiguration(ConfigurationUtil.fromArgs("aa", "dd"));
    assertNotEquals(e1, e2);
    e2.setOldConfiguration(ConfigurationUtil.fromArgs("aa", "cc"));
    assertEquals(e1, e2);
  }

  public void testToFromMap() {
    AuEvent e1 = AuEvent.forAu(mau1, AuEvent.Type.Create);
    AuEvent e2 = AuEvent.forAu(mau1, AuEvent.Type.Create);
    Map map;

    map = e1.toMap();
    assertEquals(MapUtil.map("auid", "auid1", "type", "Create"), map);
    e2 = AuEvent.fromMap(map);
    assertEquals(e1, e2);

    e1.setInBatch(true);

    ContentChangeInfo ci1 = new ContentChangeInfo();
    ci1.setType(ContentChangeInfo.Type.Crawl);
    ci1.setUrls(ListUtil.list("u3", "u4"));
    e1.setChangeInfo(ci1);

    Configuration c1 = ConfigurationUtil.fromArgs("p1", "v8");
    e1.setOldConfiguration(c1);
    map = e1.toMap();

    assertEquals(MapUtil.map("auid", "auid1",
			     "type", "Create",
			     "in_batch", true,
			     "change_info", MapUtil.map("type", "Crawl",
						       "complete", false,
						       "urls", ListUtil.list("u3", "u4"),
						       "num_urls", 0),
			     "old_config", MapUtil.map("p1", "v8")),
		 map);
    e2 = AuEvent.fromMap(map);
    assertEquals(e1, e2);


  }
}
