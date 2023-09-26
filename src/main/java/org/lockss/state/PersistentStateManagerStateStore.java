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

import org.lockss.account.AccountManager;
import org.lockss.account.UserAccount;
import org.lockss.app.StoreException;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.db.DbException;
import org.lockss.servlet.LockssServlet;
import org.lockss.util.*;
import org.lockss.util.io.FileUtil;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.lockss.account.AccountManager.DELETED_REASON;

/**
 * Hybrid implementation of {@link StateStore} for {@link PersistentStateManager} which stores AU
 * configuration state into a database but stores and loads {@link UserAccount} serialized files.
 */
public class PersistentStateManagerStateStore extends DbStateManagerSql {
  private static final Logger log = Logger.getLogger();

  /** Config subdir holding account info */
  static final String PREFIX = Configuration.PREFIX + "accounts.";
  static final String PARAM_ACCT_DIR = PREFIX + "acctDir";
  public static final String DEFAULT_ACCT_DIR = "accts";
  private static String acctRelDir = DEFAULT_ACCT_DIR;

  private ConfigManager configMgr;
  private AccountManager acctMgr;

  /**
   * Constructor.
   *
   * @param configDbManager A ConfigDbManager with the database manager.
   * @throws DbException if any problem occurred accessing the database.
   */
  public PersistentStateManagerStateStore(ConfigDbManager configDbManager) throws StoreException {
    super(configDbManager);
    configMgr = configDbManager.getConfigManager();
    acctMgr = configMgr.getApp().getManagerByType(AccountManager.class);
    ensureAcctDir();
  }

  @Override
  public Iterable<String> findUserAccountNames() {
    findUserFiles();
    return new ArrayList<>();
  }

  @Override
  public Iterable<UserAccount> findUserAccounts() throws StoreException, IOException {
    return null;
  }

  @Override
  public UserAccount findUserAccount(String key) {
    return null;
  }

  @Override
  public void updateUserAccount(String key, UserAccount acct, Set<String> fields) throws StoreException {
    // FIXME: Last successful login / failed logins will be overwritten
    // Allow update if the object doens't exist
    String filename = acct.getFilename();
    if (filename == null) {
      filename = generateFilename(acct);
    }
    File file =  new File(getAcctDir(), filename);
    try {
      storeUserInternal(acct, file);
    } catch (SerializationException | IOException e) {
      throw new StoreException("Error storing user in database", e);
    }

    acct.setFilename(file.getName());
  }

  @Override
  public void removeUserAccount(UserAccount acct) {
    deleteUser(acct);
  }

  /** Delete the user */
  public synchronized boolean deleteUser(UserAccount acct) {
    boolean res = true;
    String filename = acct.getFilename();
    if (filename != null) {
      File file = new File(getAcctDir(), filename);
      // done this way so will return true if file is gone, whether we
      // deleted it or not
      file.delete();
      res = !file.exists();
    }
    if (res) {
      acct.disable(DELETED_REASON);
    }
    return res;
  }

  UserAccount loadUser(File file) {
    try {
      UserAccount acct = (UserAccount)makeObjectSerializer().deserialize(file);
      acct.setFilename(file.getName());
      acct.postLoadInit(acctMgr, configMgr.getCurrentConfig());

      postInitLoad(acct);

      log.debug2("Loaded user " + acct.getName() + " from " + file);
      return acct;
    } catch (Exception e) {
      log.error("Unable to load account data from " + file, e);
      return null;
    }
  }

  private void postInitLoad(UserAccount acct) {
    if (acct.getVersion() < 2) {
      Set<String> r = new HashSet<>(acct.getRoleSet());
      if (r.add(LockssServlet.ROLE_CONTENT_ACCESS)) {
        log.debug("Adding accessContentRole to " + acct.getName());
        acct.setRoles(r);
      }
      log.debug("Updating " + acct.getName() + " to version 2");
      acct.setVersion(2);
      acct.storeUser(true);
    }
  }

  private ObjectSerializer makeObjectSerializer() {
    return new XStreamSerializer();
  }

  public static String generateFilename(UserAccount acct) {
    String name = StringUtil.sanitizeToIdentifier(acct.getName()).toLowerCase();
    File dir = getAcctDir();
    if (!new File(dir, name).exists()) {
      return name;
    }
    for (int ix = 1; ix < 10000; ix++) {
      String s = name + "_" + ix;
      if (!new File(dir, s).exists()) {
        return s;
      }
    }
    throw new RuntimeException("Can't generate unique file to store account: "
        + acct.getName());
  }

  /** Return parent dir of all user account files */
  public static File getAcctDir() {
    ConfigManager cfgMgr = ConfigManager.getConfigManager();
    return new File(cfgMgr.getCacheConfigDir(),
        PersistentStateManagerStateStore.DEFAULT_ACCT_DIR);
  }

  File ensureAcctDir() {
    File acctDir = getAcctDir();
    if (!acctDir.exists()) {
      if (!acctDir.mkdir()) {
        throw new IllegalArgumentException("Account data directory " +
            acctDir + " cannot be created.");
      }
      FileUtil.setOwnerRWX(acctDir);
    }
    if (!acctDir.canWrite()) {
      throw new IllegalArgumentException("Account data directory " +
          acctDir + " is not writable.");
    }

    log.debug2("Account dir: " + acctDir);
    return acctDir;
  }

  File[] findUserFiles() {
    File acctDir = getAcctDir();
    File files[] = acctDir.listFiles(new UserAccountFileFilter());
    return files;
  }

  // accept regular files with names that could have been generated by
  // sanitizeName().  This avoids trying to load, e.g., renamed failed
  // deserialization files (*.deser.old)
  private static final class UserAccountFileFilter implements FileFilter {
    public boolean accept(File file) {
      if (!file.isFile()) {
        return false;
      }
      String name = file.getName();
      for (int ix = name.length() - 1; ix >= 0; --ix) {
        char ch = name.charAt(ix);
        if (!Character.isJavaIdentifierPart(ch)) {
          return false;
        }
      }
      return true;
    }
  }

  void storeUserInternal(UserAccount acct, File file)
      throws IOException, SerializationException {
    if (acct.isChanged()) {
      if (log.isDebug2()) log.debug2("Storing account in " + file);
      makeObjectSerializer().serialize(file, acct);
      FileUtil.setOwnerRW(file);
      acct.notChanged();
    }
  }
}
