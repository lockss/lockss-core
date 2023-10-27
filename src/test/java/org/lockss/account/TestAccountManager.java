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

package org.lockss.account;

import org.lockss.config.ConfigManager;
import org.lockss.log.L4JLogger;
import org.lockss.state.InMemoryStateManager;
import org.lockss.state.StateManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase;
import org.lockss.util.PropUtil;
import org.lockss.util.SetUtil;
import org.lockss.util.test.FileTestUtil;

import static org.lockss.servlet.LockssServlet.ROLE_USER_ADMIN;

/**
 * Test class for org.lockss.account.AccountManager
 */
public class TestAccountManager extends LockssTestCase {

  L4JLogger log = L4JLogger.getLogger();

  MyAccountManager acctMgr;
  StateManager stateMgr;

  public void setUp() throws Exception {
    super.setUp();
    ConfigurationUtil.addFromArgs(AccountManager.PARAM_ENABLED, "true",
				  AccountManager.PARAM_ENABLE_DEBUG_USER, "false",
				  ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  getTempDir("accttest").toString());

    acctMgr = new MyAccountManager();
    stateMgr = new InMemoryStateManager();
    getMockLockssDaemon().setAccountManager(acctMgr);
    getMockLockssDaemon().setStateManager(stateMgr);

    acctMgr.initService(getMockLockssDaemon());
    stateMgr.initService(getMockLockssDaemon());
    acctMgr.startService();
    stateMgr.startService();
  }
  
  public void tearDown() throws Exception {
    super.tearDown();
  }

  void assertEqualAccts(UserAccount a1, UserAccount a2) {
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

  public void testGetUserFactory() {
    assertClass(BasicUserAccount.Factory.class, acctMgr.getUserFactory("basic"));
    assertClass(BasicUserAccount.Factory.class, acctMgr.getUserFactory("Basic"));
    assertClass(BasicUserAccount.Factory.class, acctMgr.getUserFactory("Unknown"));

    assertClass(LCUserAccount.Factory.class, acctMgr.getUserFactory("LC"));
    assertClass(LCUserAccount.Factory.class,
		acctMgr.getUserFactory("org.lockss.account.LCUserAccount"));
    assertClass(StaticUserAccount.Factory.class,
		acctMgr.getUserFactory("org.lockss.account.StaticUserAccount"));

    assertClass(BasicUserAccount.Factory.class, acctMgr.getUserFactory("Unknown"));
    assertClass(BasicUserAccount.Factory.class, acctMgr.getUserFactory(""));
    assertClass(BasicUserAccount.Factory.class, acctMgr.getUserFactory(null));
  }

  public void testCreateUser() {
    UserAccount acct = acctMgr.createUser("fred");
    assertClass(BasicUserAccount.class, acct);
    assertEquals("fred", acct.getName());
    ConfigurationUtil.addFromArgs(AccountManager.PARAM_NEW_ACCOUNT_TYPE, "lc");
    acct = acctMgr.createUser("ethel");
    assertClass(LCUserAccount.class, acct);
    assertEquals("ethel", acct.getName());
  }

  public void testGetUser() throws Exception {
    assertFalse(acctMgr.hasUser("nouser"));
    assertNull(acctMgr.getUserOrNull("nouser"));
    assertSame(AccountManager.NOBODY_ACCOUNT, acctMgr.getUser("nouser"));

    acctMgr.addStaticUser("foo", "SHA-1:0102");
    assertTrue(acctMgr.hasUser("foo"));
    assertEquals("foo", acctMgr.getUser("foo").getName());
  }

  public void testAddUser() throws Exception {
    String user = "fred";
    UserAccount acct1 = acctMgr.createUser(user);
    try {
      acctMgr.addUser(acct1);
      fail("Should be illegal to add user without password");
    } catch (AccountManager.NotAddedException e) {
    }
    acct1.setPassword("password");
    assertNull(acctMgr.getUserOrNull(user));
    acctMgr.addUser(acct1);
    UserAccount acct2 = acctMgr.getUser(user);
    assertSame(acct1, acct2);
    // should be ok to add redundantly
    acctMgr.addUser(acct1);
    UserAccount acct3 = acctMgr.createUser(user);
    acct3.setPassword("fleeble");
    try {
      acctMgr.addUser(acct3);
      fail("Should be illegal to add user with existing name");
    } catch (AccountManager.UserExistsException e) {
    }
  }

  public void testAddStaticUser() throws Exception {
    String user = "ferd";
    String cred = "SHA-1:01020304";
    UserAccount acct1 = acctMgr.addStaticUser(user, cred);
    UserAccount acct2 = acctMgr.getUser(user);
    assertSame(acct1, acct2);
    assertTrue(acct1.isStaticUser());
    assertNull(stateMgr.getUserAccount(user));
  }

  public void testInstallDebugUser() throws Exception {
    String user = "lochs";
    assertEquals(0, acctMgr.getUsers().size());
    ConfigurationUtil.addFromArgs(AccountManager.PARAM_ENABLE_DEBUG_USER,
				  "true");
    acctMgr.installDebugUser(AccountManager.DEBUG_USER_PROPERTY_FILE);
    assertEquals(1, acctMgr.getUsers().size());
    UserAccount acct1 = acctMgr.getUser(user);
    assertEquals(user, acct1.getName());
    assertEquals("debugRole,userAdminRole", acct1.getRoles());
    assertTrue(acct1.isStaticUser());
  }

  public void testInstallPlatformUser0() throws Exception {
    String user = "ferd";
    String cred = "SHA-1:01020304";

    assertEquals(0, acctMgr.getUsers().size());
    acctMgr.installPlatformUser(null, cred);
    assertEquals(0, acctMgr.getUsers().size());
    acctMgr.installPlatformUser(user, null);
    assertEquals(0, acctMgr.getUsers().size());
    ConfigurationUtil.addFromArgs(AccountManager.PARAM_CONDITIONAL_PLATFORM_USER,
				  "true");
    acctMgr.installPlatformUser(user, cred);
    assertEquals(1, acctMgr.getUsers().size());
    UserAccount acct1 = acctMgr.getUser(user);
    assertEquals(user, acct1.getName());
    assertEquals(ROLE_USER_ADMIN, acct1.getRoles());
    assertTrue(acct1.isStaticUser());
    assertNull(stateMgr.getUserAccount(user));
  }

  public void testInstallPlatformUser1() throws Exception {
    String user = "ferd";
    String cred = "SHA-1:01020304";

    assertEquals(0, acctMgr.getUsers().size());
    UserAccount admin = acctMgr.createUser("foo");
    admin.setPassword("1234Abcd");
    admin.setRoles(ROLE_USER_ADMIN);
    acctMgr.addUser(admin);
    assertEquals(1, acctMgr.getUsers().size());
    ConfigurationUtil.addFromArgs(AccountManager.PARAM_CONDITIONAL_PLATFORM_USER,
				  "true");
    acctMgr.installPlatformUser(user, cred);
    assertEquals(1, acctMgr.getUsers().size());
    assertNull(acctMgr.getUserOrNull(user));
  }

  public void testLoadFromProps() throws Exception {
    String name = "luser";
    assertEquals(0, acctMgr.getUsers().size());
    acctMgr.loadFromProps(PropUtil.fromArgs(name,
					    "SHA-1:0102,fooRole,barRole\n"));
    UserAccount acct = acctMgr.getUser(name);
    assertEquals(SetUtil.set("fooRole", "barRole"),
		 SetUtil.theSet(acct.getRoleSet()));
  }

  public void xtestLoadFromProps() throws Exception {
    String name = "luser";
    String url =
      FileTestUtil.urlOfString(name + ": SHA-1:01020304,fooRole,barRole\n");
    assertEquals(0, acctMgr.getUsers().size());
    acctMgr.loadFromProps(url);
    UserAccount acct = acctMgr.getUser(name);
    assertEquals(SetUtil.set("FooRole", "BarRole"), acct.getRoleSet());
  }

  UserAccount makeUser(String name) {
    UserAccount acct = new BasicUserAccount.Factory().newUser(name, acctMgr);
    return acct;
  }

  static String PWD1 = "123Sb!@#";
  static String PWD2 = "223Sb!@#";

  // This tests both storeUser() and loadUsers()
  public void testStoreUser() throws Exception {
    UserAccount acct1 = makeUser("lu@ser");
    acct1.setPassword(PWD1, true);

    // Assert storing an account that hasn't been added to the AccountManager
    // will result in a throw:
    try {
      acctMgr.storeUser(acct1);
      fail("Shouldn't be able to store un-added account");
    } catch (IllegalArgumentException e) {
    }

    // Assert the presence of the account in the StateManager prior to and
    // after adding the account to the AccountManager:
    assertNull(stateMgr.getUserAccount(acct1.getName()));
    acctMgr.addUser(acct1);
    assertSame(acct1, stateMgr.getUserAccount(acct1.getName()));

    // Add a second account
    UserAccount acct2 = makeUser("luser!");
    acct2.setPassword(PWD2, true);
    acctMgr.addUser(acct2);

    // Assert a UserAccount with no changes DOES NOT result in a store
    assertFalse(acct1.isChanged());
    acctMgr.storeUser(acct1);
    // TODO

    // Make a change to the first user and assert it results in an update
    acct1.setEmail("her@there");
    assertTrue(acct1.isChanged());
    acctMgr.storeUser(acct1);
    assertFalse(acct1.isChanged());
    assertEqualAccts(acct1, stateMgr.getUserAccount(acct1.getName()));

    assertSame(acct1, stateMgr.getUserAccount(acct1.getName()));
    assertSame(acct2, stateMgr.getUserAccount(acct2.getName()));

    assertSame(acct1, acctMgr.getUser(acct1.getName()));
    assertSame(acct2, acctMgr.getUser(acct2.getName()));

    // Clear accounts from AccountManager
    acctMgr.clearAccounts();

    // Re-load accounts from StateManager
    acctMgr.loadUsers();
    assertEqualAccts(acct1, acctMgr.getUser(acct1.getName()));
    assertEqualAccts(acct2, acctMgr.getUser(acct2.getName()));
    assertEquals(2, acctMgr.getUsers().size());
  }

  public void testStoreWrongUser() throws Exception {
    UserAccount acct1 = makeUser("luser");
    acct1.setPassword(PWD1, true);
    acctMgr.addUser(acct1);

    UserAccount acct2 = makeUser("luser");
    acct2.setPassword(PWD1, true);
    try {
      acctMgr.storeUser(acct2);
      fail("Shouldn't be able to store different instance");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testDeleteUser() throws Exception {
    String name = "lu@ser";
    UserAccount acct1 = makeUser(name);
    acct1.setPassword(PWD1, true);

    acctMgr.addUser(acct1);
    assertSame(acct1, stateMgr.getUserAccount(name));
    assertSame(acct1, acctMgr.getUser(name));
    assertTrue(acct1.isEnabled());

    assertTrue(acctMgr.deleteUser(name));
    assertFalse(acct1.isEnabled());
    assertNull(stateMgr.getUserAccount(name));
    assertNull(acctMgr.getUserOrNull(name));

    assertTrue(acctMgr.deleteUser("notthere"));
  }

  static class MyAccountManager extends AccountManager {
    void clearAccounts() {
      accountMap.clear();
    }
  }
}
