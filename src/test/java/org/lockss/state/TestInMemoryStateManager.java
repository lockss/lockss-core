/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.state;

import java.io.*;
import java.util.*;

import org.lockss.app.*;
import org.lockss.daemon.Crawler;
import org.lockss.plugin.*;
import org.lockss.poller.v3.V3Poller;
import org.lockss.poller.v3.V3Poller.PollVariant;
import org.lockss.repository.AuSuspectUrlVersions;
import org.lockss.test.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;

public class TestInMemoryStateManager extends LockssTestCase {
  L4JLogger log = L4JLogger.getLogger();
  MockLockssDaemon daemon;
  PluginManager pluginMgr;
  MyInMemoryStateManager stateMgr;
  MockPlugin mplug;
  MockArchivalUnit mau1;
  MockArchivalUnit mau2;

  public void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();
    pluginMgr = daemon.getPluginManager();
//     pluginMgr.startService();

    stateMgr = daemon.setUpStateManager(new MyInMemoryStateManager());
    mplug = new MockPlugin(daemon);
    mau1 = new MockArchivalUnit(mplug, "aaa1");
    mau2 = new MockArchivalUnit(mplug, "aaa2");
  }

  public void test1() {
    AuState aus1 = AuUtil.getAuState(mau1);
    AuState aus2 = AuUtil.getAuState(mau2);
    assertNotSame(aus1, aus2);
    assertSame(aus1, AuUtil.getAuState(mau1));
    assertSame(aus2, AuUtil.getAuState(mau2));
    assertEquals(-1, aus1.getLastMetadataIndex());
    aus1.setLastMetadataIndex(123);
    assertEquals(123, aus1.getLastMetadataIndex());
    assertSame(aus1, AuUtil.getAuState(mau1));

    // after deactivate event, getAuState() should return a new instance
    // with the same values as the old instance
    auEvent(mau1, AuEvent.Type.Deactivate);
    AuState aus1b = AuUtil.getAuState(mau1);
    assertNotSame(aus1, aus1b);
    assertEquals(123, aus1b.getLastMetadataIndex());
  }

  void auEvent(ArchivalUnit au, AuEvent.Type type) {
    pluginMgr.signalAuEvent(au, AuEvent.forAu(au, type));
  }


  static class MyInMemoryStateManager extends InMemoryStateManager {
  }

}
