/*

Copyright (c) 2000-2023, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.state;

import org.junit.Before;
import org.junit.Test;
import org.lockss.account.AccountManager;
import org.lockss.account.BasicUserAccount;
import org.lockss.account.UserAccount;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.log.L4JLogger;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.StringUtil;
import org.lockss.util.test.FileTestUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

import static org.lockss.state.PersistentStateManagerStateStore.getAcctDir;

/**
 * Tests for {@link PersistentStateManagerStateStore}.
 */
public class TestPersistentStateManagerStateStore extends LockssTestCase4 {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final String EMPTY_STRING = "";

  private MockLockssDaemon daemon;
  private ConfigDbManager cfgDbMgr;
  private AccountManager acctMgr;
  private StateManager stateMgr;
  private PersistentStateManagerStateStore sstore;

  @Before
  public void setup() throws Exception {
    setUpDiskSpace();
    log.info("acctDir = {}", getAcctDir());

    daemon = getMockLockssDaemon();
    cfgDbMgr = daemon.getConfigDbManager();

    acctMgr = new AccountManager();
    stateMgr = new InMemoryStateManager();
    daemon.setAccountManager(acctMgr);
    daemon.setStateManager(stateMgr);

    acctMgr.initService(getMockLockssDaemon());
    stateMgr.initService(getMockLockssDaemon());
    acctMgr.startService();
    stateMgr.startService();

    sstore = new PersistentStateManagerStateStore(cfgDbMgr);
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#findUserAccountNames()}.
   */
  @Test
  public void testFindUserAccountNames() throws Exception {
    assertEmpty(sstore.findUserAccountNames());

    File acctfile = new File(getAcctDir(), "user1");
    InputStream is = getResourceAsStream("user1.json");
    String orig = StringUtil.fromInputStream(is);
    FileTestUtil.writeFile(acctfile, orig);

    List<String> usernames = new ArrayList<>();
    usernames.add("User1");

    assertIterableEquals(usernames, sstore.findUserAccountNames());
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#findUserAccount(String)}.
   */
  @Test
  public void testFindUserAccount() throws Exception {
    assertNull(sstore.findUserAccount(null));
    assertNull(sstore.findUserAccount(EMPTY_STRING));
    assertNull(sstore.findUserAccount("missing"));

    File acctfile = new File(getAcctDir(), "user1");
    InputStream is = getResourceAsStream("user1.json");
    String orig = StringUtil.fromInputStream(is);
    FileTestUtil.writeFile(acctfile, orig);

    UserAccount acct = sstore.findUserAccount("User1");
    assertNotNull(acct);
    assertEquals("User1", acct.getName());
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#updateUserAccount(String, UserAccount, Set)}}.
   */
  @Test
  public void testUpdateUserAccount() throws Exception {
    UserAccount acct1 = makeUser("User1");
    acct1.setPassword("badPassword");
    acct1.setFilename(PersistentStateManagerStateStore.generateFilename(acct1));

    File f1 = new File(getAcctDir(), acct1.getFilename());
    assertFalse(f1.exists());

    sstore.updateUserAccount(acct1.getName(), acct1, null);
    assertEqualAccts(acct1, sstore.loadUser(f1));

    assertTrue(f1.exists());
    assertEquals("File: " + f1,
        EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE),
        Files.getPosixFilePermissions(f1.toPath()));

    Set<String> fields = new HashSet<>();
    fields.add("email");
    acct1.setEmail("xyzzy@example.org");

    sstore.updateUserAccount(acct1.getName(), acct1, fields);
    UserAccount updatedAcct = sstore.loadUser(f1);
    assertEquals("xyzzy@example.org", updatedAcct.getEmail());

    assertEqualAccts(acct1, updatedAcct);
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#removeUserAccount(UserAccount)}.
   */
  @Test
  public void testRemoveUserAccount() throws Exception {
    UserAccount acct1 = makeUser("User1");
    acct1.setPassword("badPassword");
    acct1.setFilename(PersistentStateManagerStateStore.generateFilename(acct1));

    File f1 = new File(getAcctDir(), acct1.getFilename());
    assertFalse(f1.exists());

    sstore.storeUser(acct1, f1);
    assertTrue(f1.exists());

    sstore.removeUserAccount(acct1);
    assertFalse(f1.exists());
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#storeUser(UserAccount, File)}.
   */
  @Test
  public void testStoreUser() throws Exception {
    UserAccount acct1 = makeUser("User1");
    acct1.setPassword("badPassword");
    acct1.setFilename(PersistentStateManagerStateStore.generateFilename(acct1));

    File f1 = new File(getAcctDir(), acct1.getFilename());
    assertFalse(f1.exists());

    sstore.storeUser(acct1, f1);
    assertTrue(f1.exists());

    assertEqualAccts(acct1, sstore.loadUser(f1));
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#loadUser(File)}.
   */
  @Test
  public void testLoadUser() throws Exception {
    File acctfile = new File(getAcctDir(), "user1");
    assertNull(sstore.loadUser(acctfile));

    // Write user account file from resource
    InputStream is = getResourceAsStream("user1.json");
    String orig = StringUtil.fromInputStream(is);
    FileTestUtil.writeFile(acctfile, orig);

    // Load user account and assert result
    UserAccount acct = sstore.loadUser(acctfile);
    assertNotNull(acct);
    assertEquals("User1", acct.getName());
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#generateFilename(UserAccount)}.
   */
  @Test
  public void testGenerateFilename() {
    assertEquals("john_smith",
        PersistentStateManagerStateStore.generateFilename(makeUser("John_Smith")));
    assertEquals("foo",
        PersistentStateManagerStateStore.generateFilename(makeUser("foo!")));
    assertEquals("foobar_",
        PersistentStateManagerStateStore.generateFilename(makeUser(" +.!|,foo.bar?<>_")));
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#getAcctDir()}.
   */
  @Test
  public void testGetAcctDir() throws IOException {
    File f1 = getAcctDir();
    assertTrue(f1.exists());
    assertEquals("File: " + f1,
        EnumSet.of(PosixFilePermission.OWNER_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.OWNER_EXECUTE),
        Files.getPosixFilePermissions(f1.toPath()));
  }

  /**
   * Test for {@link PersistentStateManagerStateStore#findUserFiles()}.
   */
  @Test
  public void testUserAccountFileFilter() throws Exception {
    File f1 = new File(getAcctDir(), "user1");
    InputStream is = getResourceAsStream("user1.json");
    String orig = StringUtil.fromInputStream(is);
    FileTestUtil.writeFile(f1, orig);

    assertEquals(1, sstore.findUserFiles().length);

    // Rename acct1 file to a name that shouldn't pass the filter
    File illFile = new File(f1.getParent(), "lu.ser");
    assertEquals(f1.getParent(), illFile.getParent());
    f1.renameTo(illFile);

//    // Create a subdir that shouldn't pass the filter.  It doesn't hurt
//    // anything even if AccountManager tries to process the subdir, and the
//    // test won't fail, but the error will appear in the test log.
//    File subdir = new File(f1.getParent(), "adir");
//    subdir.mkdir();

    assertEquals(0, sstore.findUserFiles().length);
  }

  private UserAccount makeUser(String name) {
    return new BasicUserAccount.Factory().newUser(name, acctMgr);
  }

  private void assertEqualAccts(UserAccount a1, UserAccount a2) {
    assertEquals(a1.getName(), a2.getName());
    assertEquals(a1.getPassword(), a2.getPassword());
    assertEquals(a1.getRoles(), a2.getRoles());
    assertEquals(a1.getRoleSet(), a2.getRoleSet());
    assertEquals(a1.getEmail(), a2.getEmail());
    assertEquals(a1.getHashAlgorithm(), a2.getHashAlgorithm());

    assertEquals(a1.getCredentialString(), a2.getCredentialString());
    assertEquals(a1.getLastPasswordChange(), a2.getLastPasswordChange());
    assertEquals(a1.getLastUserPasswordChange(),
        a2.getLastUserPasswordChange());

    assertEquals(a1.isEnabled(), a2.isEnabled());
  }
}
