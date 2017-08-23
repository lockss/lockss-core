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

package org.lockss.repository;

import java.io.File;
import java.util.*;

import org.lockss.config.ConfigManager;
import org.lockss.plugin.AuUrl;
import org.lockss.test.*;

/**
 * This is the test class for org.lockss.repostiory.RepositoryNodeImpl
 */
public class TestAuNodeImpl extends LockssTestCase {
  private MockLockssDaemon theDaemon;
  private LockssRepository repo;
  private String tempDirPath;

  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    Properties props = new Properties();
    props.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
		      tempDirPath);
    ConfigurationUtil.setCurrentConfigFromProps(props);

    MockArchivalUnit mau = new MockArchivalUnit();

    theDaemon = getMockLockssDaemon();
    repo = theDaemon.getLockssRepository(mau);
  }

  public void tearDown() throws Exception {
    repo.stopService();
    super.tearDown();
  }

  public void testListEntries() throws Exception {
    TestRepositoryNodeImpl.createLeaf(repo,
                                      "http://www.example.com/testDir/leaf1",
                                      "test stream", null);
    TestRepositoryNodeImpl.createLeaf(repo,
                                      "http://www.example.com/testDir/leaf2",
                                      "test stream", null);
    TestRepositoryNodeImpl.createLeaf(repo,
                                      "http://image.example.com/image1.gif",
                                      "test stream", null);
    TestRepositoryNodeImpl.createLeaf(repo, "ftp://www.example.com/file1",
                                      "test stream", null);

    RepositoryNode auNode = repo.getNode(AuUrl.PROTOCOL_COLON+"//test.com");
    Iterator childIt = auNode.listChildren(null, false);
    ArrayList childL = new ArrayList(3);
    while (childIt.hasNext()) {
      RepositoryNode node = (RepositoryNode)childIt.next();
      childL.add(node.getNodeUrl());
    }
    String[] expectedA = new String[] {
      "ftp://www.example.com",
      "http://image.example.com",
      "http://www.example.com",
      };
    assertSameElements(expectedA, childL);
  }

  public void testIllegalOperations() throws Exception {
    RepositoryNode auNode = new AuNodeImpl("lockssAu:test", "", null);
    assertFalse(auNode.hasContent());
    assertFalse(auNode.isLeaf());
    assertFalse(auNode.isContentInactive());
    try {
      auNode.makeNewVersion();
      fail("Cannot make version for AuNode.");
    } catch (UnsupportedOperationException uoe) { }
    try {
      auNode.deactivateContent();
      fail("Cannot deactivate AuNode.");
    } catch (UnsupportedOperationException uoe) { }
  }

}
