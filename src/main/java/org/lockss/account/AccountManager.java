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

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;

import org.lockss.app.*;
import org.lockss.daemon.status.*;
import org.lockss.config.*;
import org.lockss.servlet.*;
import org.lockss.state.StateManager;
import org.lockss.util.*;
import org.lockss.alert.*;
import org.lockss.mail.*;

import static org.lockss.servlet.BaseServletManager.SUFFIX_AUTH_TYPE;
import static org.lockss.servlet.BaseServletManager.SUFFIX_USE_SSL;

/** Manage user accounts
 */
public class AccountManager
  extends BaseLockssDaemonManager implements ConfigurableManager  {

  private static final Logger log = Logger.getLogger();

  public static final UserAccount NOBODY_ACCOUNT = new NobodyAccount();

  static final String PREFIX = Configuration.PREFIX + "accounts.";

  /** Enable account management */
  static final String PARAM_ENABLED = PREFIX + "enabled";
  static final boolean DEFAULT_ENABLED = false;

  /** Enable sending password change reminders */
  static final String PARAM_MAIL_ENABLED = PREFIX + "mailEnabled";
  static final boolean DEFAULT_MAIL_ENABLED = false;

  /** Select a preconfigured user account policy, one of LC, SSL, FORM,
   * BASIC  */
  public static final String PARAM_POLICY = PREFIX + "policy";
  public static final String DEFAULT_POLICY = null;

  /** Type of account to create for new users */
  public static final String PARAM_NEW_ACCOUNT_TYPE = PREFIX + "newUserType";
  public static final String DEFAULT_NEW_ACCOUNT_TYPE =
    "org.lockss.account.BasicUserAccount";

  /** Enable the debug user on named server.  Daemon restart required. */
  public static final String PARAM_ENABLE_DEBUG_USER =
    PREFIX + "enableDebugUser";
  public static final boolean DEFAULT_ENABLE_DEBUG_USER = true;

  /** File holding debug user passwd */
  public static final String DEBUG_USER_PROPERTY_FILE =
          "/org/lockss/account/debuguser.props";

  /** Username established during platform config.  Now used for REST auth
   * as well as UI. */
  public static final String PARAM_PLATFORM_USERNAME =
    Configuration.PLATFORM + "ui.username";

  /** Password established during platform config.  Now used for REST auth
   * as well as UI. */
  public static final String PARAM_PLATFORM_PASSWORD =
    Configuration.PLATFORM + "ui.password";

  /** If true, platform user is enabled on startup only if there are no
   * other enabled users with ROLE_USER_ADMIN */
  public static final String PARAM_CONDITIONAL_PLATFORM_USER =
    PREFIX + "conditionalPlatformUser";
  public static final boolean DEFAULT_CONDITIONAL_PLATFORM_USER = false;

  /** Static config user Username prop */
  public static final String USER_PARAM_USER = "user";
  /** Static config user Encrypted password prop */
  public static final String USER_PARAM_PWD = "password";
  /** Static config user List of roles (Debug, Admin) prop */
  public static final String USER_PARAM_ROLES = "roles";

  /** If true, alerts for users who have no email address will be sent to
   * the admin email */
  public static final String PARAM_MAIL_ADMIN_IF_NO_USER_EMAIL =
    PREFIX + "mailAdminIfNoUserEmail";
  public static final boolean DEFAULT_MAIL_ADMIN_IF_NO_USER_EMAIL = false;

  /** Frequency to check for password change reminders to send: daily,
   * weekly or monthly */
  public static final String PARAM_PASSWORD_CHECK_FREQ =
    PREFIX + "passwordCheck.frequency";
  public static final String DEFAULT_PASSWORD_CHECK_FREQ = "daily";

  /** If true, login and logout events will be included in auditable event
   * alerts */
  public static final String PARAM_ALERT_ON_LOGIN_LOGOUT =
    PREFIX + "alertOnLoginLogout";
  public static final boolean DEFAULT_ALERT_ON_LOGIN_LOGOUT = false;

  /** Alertconfig set by AccountManager */
  public static final String PARAM_PASSWORD_REMINDER_ALERT_CONFIG =
    AlertManagerImpl.PARAM_CONFIG + ".acct";

  private static String PASSWORD_REMINDER_ALERT_CONFIG =
    "<org.lockss.alert.AlertConfig>" +
    "  <filters>" +
    "    <org.lockss.alert.AlertFilter>" +
    "      <pattern class=\"org.lockss.alert.AlertPatterns-Predicate\">" +
    "        <attribute>name</attribute>" +
    "        <relation>CONTAINS</relation>" +
    "        <value class=\"list\">" +
    "          <string>PasswordReminder</string>" +
    "          <string>AccountDisabled</string>" +
    "        </value>" +
    "      </pattern>" +
    "      <action class=\"org.lockss.alert.AlertActionMail\"/>" +
    "    </org.lockss.alert.AlertFilter>" +
    "    <org.lockss.alert.AlertFilter>" +
    "      <pattern class=\"org.lockss.alert.AlertPatterns-Predicate\">" +
    "        <attribute>name</attribute>" +
    "        <relation>CONTAINS</relation>" +
    "        <value class=\"list\">" +
    "          <string>AuditableEvent</string>" +
    "        </value>" +
    "      </pattern>" +
    "      <action class=\"org.lockss.alert.AlertActionSyslog\">" +
    "        <fixedLevel>-1</fixedLevel>" +
    "      </action>" +
    "    </org.lockss.alert.AlertFilter>" +
    "  </filters>" +
    "</org.lockss.alert.AlertConfig>";


  // Predefined account policies.  See ConfigManager.setConfigMacros()

  private static String UI_PREFIX = AdminServletManager.PREFIX;

  /** <code>LC</code>: SSL, form auth, Library of Congress password rules */
  public static String[] POLICY_LC = {
    PARAM_ENABLED, "true",
    PARAM_NEW_ACCOUNT_TYPE, "LC",
    PARAM_CONDITIONAL_PLATFORM_USER, "true",
    PARAM_PASSWORD_REMINDER_ALERT_CONFIG, PASSWORD_REMINDER_ALERT_CONFIG,
    PARAM_MAIL_ENABLED, "true",
    PARAM_ENABLE_DEBUG_USER, "false",
    MailService.PARAM_ENABLED, "true",
    AlertManager.PARAM_ALERTS_ENABLED, "true",
    AlertActionMail.PARAM_ENABLED, "true",
    UI_PREFIX + SUFFIX_AUTH_TYPE, "Form",
    UI_PREFIX + SUFFIX_USE_SSL, "true",
  };

  /** <code>SSL</code>: SSL, form auth, configurable password rules */
  public static String[] POLICY_SSL = {
    PARAM_ENABLED, "true",
    PARAM_NEW_ACCOUNT_TYPE, "Basic",
    PARAM_PASSWORD_REMINDER_ALERT_CONFIG, PASSWORD_REMINDER_ALERT_CONFIG,
    UI_PREFIX + SUFFIX_AUTH_TYPE, "Form",
    UI_PREFIX + SUFFIX_USE_SSL, "true",
  };

  /** <code>Form</code>: HTTP, form auth, configurable password rules */
  public static String[] POLICY_FORM = {
    PARAM_ENABLED, "true",
    PARAM_NEW_ACCOUNT_TYPE, "Basic",
    PARAM_PASSWORD_REMINDER_ALERT_CONFIG, PASSWORD_REMINDER_ALERT_CONFIG,
    UI_PREFIX + SUFFIX_AUTH_TYPE, "Form",
    UI_PREFIX + SUFFIX_USE_SSL, "false",
  };

  /** <code>Basic</code>: HTTP, basic auth */
  public static String[] POLICY_BASIC = {
    PARAM_ENABLED, "true",
    PARAM_NEW_ACCOUNT_TYPE, "Basic",
    PARAM_PASSWORD_REMINDER_ALERT_CONFIG, PASSWORD_REMINDER_ALERT_CONFIG,
    UI_PREFIX + SUFFIX_AUTH_TYPE, "Basic",
    UI_PREFIX + SUFFIX_USE_SSL, "false",
  };

  /** <code>Compat</code>: HTTP, basic auth, no account management */
  public static String[] POLICY_COMPAT = {
    PARAM_ENABLED, "false",
    PARAM_NEW_ACCOUNT_TYPE, "Basic",
    PARAM_PASSWORD_REMINDER_ALERT_CONFIG, PASSWORD_REMINDER_ALERT_CONFIG,
    UI_PREFIX + SUFFIX_AUTH_TYPE, "Basic",
    UI_PREFIX + SUFFIX_USE_SSL, "false",
  };

  private boolean isEnabled = DEFAULT_ENABLED;
  private boolean isEnableDebugUser = DEFAULT_ENABLE_DEBUG_USER;
  private boolean mailEnabled = DEFAULT_MAIL_ENABLED;
  private boolean mailAdminIfNoUserEmail = DEFAULT_MAIL_ADMIN_IF_NO_USER_EMAIL;
  private String adminEmail = null;
  private boolean alertOnLoginLogout = DEFAULT_ALERT_ON_LOGIN_LOGOUT;

  private StateManager stateMgr;
  private UserAccount.Factory acctFact;
  private String acctType;

  // Maps account name to UserAccount
  Map<String,UserAccount> accountMap = new HashMap<String,UserAccount>();

  public void startService() {
    super.startService();
    LockssDaemon daemon = getDaemon();
    stateMgr = daemon.getManagerByType(StateManager.class);
    stateMgr.registerUserAccountChangedCallback(userChangedCallback);
    resetConfig();
    installDebugUser(DEBUG_USER_PROPERTY_FILE);
    installPlatformUser();
    if (isEnabled) {
      loadUsers();
      try {
	AdminServletManager adminMgr = 
	  (AdminServletManager)daemon.getManager(LockssDaemon.SERVLET_MANAGER);
	if (adminMgr.hasUserSessions()) {
	  StatusService statusServ = getDaemon().getStatusService();
	  statusServ.registerStatusAccessor(UserStatus.USER_STATUS_TABLE,
					    new UserStatus(adminMgr));
	}
      } catch (IllegalArgumentException e) {
	log.warning("No AdminServletManager, not installing UserStatus table");
      }
    }
  }

  public void stopService() {
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.unregisterStatusAccessor(UserStatus.USER_STATUS_TABLE);
    super.stopService();
  }

  public synchronized void setConfig(Configuration config,
				     Configuration prevConfig,
				     Configuration.Differences changedKeys) {

    if (changedKeys.contains(PREFIX)) {
      isEnabled = config.getBoolean(PARAM_ENABLED, DEFAULT_ENABLED);
      isEnableDebugUser = config.getBoolean(PARAM_ENABLE_DEBUG_USER,
					    DEFAULT_ENABLE_DEBUG_USER);
      acctType = config.get(PARAM_NEW_ACCOUNT_TYPE, DEFAULT_NEW_ACCOUNT_TYPE);
      acctFact = getUserFactory(acctType);

      mailEnabled = config.getBoolean(PARAM_MAIL_ENABLED, DEFAULT_MAIL_ENABLED);
      mailAdminIfNoUserEmail =
	config.getBoolean(PARAM_MAIL_ADMIN_IF_NO_USER_EMAIL,
			  DEFAULT_MAIL_ADMIN_IF_NO_USER_EMAIL);
      adminEmail = config.get(ConfigManager.PARAM_PLATFORM_ADMIN_EMAIL);
      alertOnLoginLogout = config.getBoolean(PARAM_ALERT_ON_LOGIN_LOGOUT,
					     DEFAULT_ALERT_ON_LOGIN_LOGOUT);
    }
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public String getDefaultAccountType() {
    return acctType;
  }

  UserAccount.Factory getUserFactory(String type) {
    if (!StringUtil.isNullString(type)) {
      if (type.equalsIgnoreCase("basic")) {
	return new BasicUserAccount.Factory();
      }
      if (type.equalsIgnoreCase("LC")) {
	return new LCUserAccount.Factory();
      }
      String clsName = type + "$Factory";
      try {
	Class cls = Class.forName(clsName);
	try {
	  return (UserAccount.Factory)cls.newInstance();
	} catch (Exception e) {
	  log.error("Can't instantiate new account factory: " + cls, e);
	}
      } catch (ClassNotFoundException e) {
	log.error("New account factory not found: " + clsName);
      }
    }
    log.warning("No factory of type '" + type + "', using basic accounts");
    return new BasicUserAccount.Factory();
  }    

  /** Create a new UserAccount of the configured type.  The account must be
   * added before becoming active. */
  public UserAccount createUser(String name) {
    return acctFact.newUser(name, this);
  }

  /** Add the user account, if doesn't conflict with an existing user and
   * it has a password. */
  synchronized UserAccount internalAddUser(UserAccount acct)
      throws NotAddedException {
    if (!acct.hasPassword()) {
      throw new NotAddedException("Can't add user without a password");
    }
    UserAccount old = accountMap.get(acct.getName());
    if (old != null && old != acct) {
      throw new UserExistsException("User already exists: " + acct.getName());
    }
    if (log.isDebug2()) {
      log.debug2("Add user " + acct.getName());
    }
    accountMap.put(acct.getName(), acct);
    return acct;
  }

  /** Add the user account, if doesn't conflict with an existing user and
   * it has a password. */
  public UserAccount addUser(UserAccount acct)
      throws NotAddedException, NotStoredException {
    internalAddUser(acct);
    if (acct.isEditable()) {
      storeUser(acct);
    }
    return acct;
  }

  /** Add a static, uneditable user account.  Used for compatibility with
   * platform-generated and old manually configured accounts.. */
  public UserAccount addStaticUser(String name, String credentials)
      throws NotAddedException {
    UserAccount acct = new StaticUserAccount.Factory().newUser(name, this);
    try {
      acct.setCredential(credentials);
    } catch (NoSuchAlgorithmException e) {
      log.error("Static user ( "  + acct.getName() + ") not installed", e);
    }
    return internalAddUser(acct);
  }

  /** Add platform user.  If {@link #PARAM_CONDITIONAL_PLATFORM_USER} is
   * true, the user is installed only if no other admin users exist */
  public void installPlatformUser() {
    // Use platform config in case real config hasn't been loaded yet (when
    // used from TinyUI)
    Configuration platConfig = ConfigManager.getPlatformConfig();
    String platUser = platConfig.get(PARAM_PLATFORM_USERNAME);
    String platPass = platConfig.get(PARAM_PLATFORM_PASSWORD);
    installPlatformUser(platUser, platPass);
  }

  /** Add platform user.  If {@link #PARAM_CONDITIONAL_PLATFORM_USER} is
   * true, the user is installed only if no other admin users exist */
  public void installPlatformUser(String platUser, String platPass) {
    if (!StringUtil.isNullString(platUser) &&
	!StringUtil.isNullString(platPass)) {
      String msg = null;
      if (!CurrentConfig.getBooleanParam(PARAM_CONDITIONAL_PLATFORM_USER,
					 DEFAULT_CONDITIONAL_PLATFORM_USER)) {
	log.info("Installing platform user");
      } else {
	// install only if no existing admin user
	for (UserAccount acct : getUsers()) {
	  if (!acct.isStaticUser()
	      && acct.isUserInRole(LockssServlet.ROLE_USER_ADMIN)) {
	    return;
	  }
	}
	msg = "platform admin account enabled because no other admin user";
      }
      try {
	UserAccount acct = addStaticUser(platUser, platPass);
	acct.setRoles(SetUtil.set(LockssServlet.ROLE_USER_ADMIN));
	if (msg != null) {
	  log.info("User " + acct.getName() + " " + msg);
	  acct.auditableEvent(msg);
	}
      } catch (UserExistsException e) {
	log.debug("Already installed platform user");
      } catch (NotAddedException e) {
	log.error("Can't install platform user", e);
      }
    }
  }

  boolean shouldInstallPlatformUser() {
    if (!CurrentConfig.getBooleanParam(PARAM_CONDITIONAL_PLATFORM_USER,
				       DEFAULT_CONDITIONAL_PLATFORM_USER)) {
      return true;
    }
    for (UserAccount acct : getUsers()) {
      if (!acct.isStaticUser()
	  && acct.isUserInRole(LockssServlet.ROLE_USER_ADMIN)) {
	return false;
      }
    }
    return true;
  }

  public void installDebugUser(String propResource) {
    if (isEnableDebugUser) {
      try {
	log.debug("passwd props file: " + propResource);
	URL propsUrl = this.getClass().getResource(propResource);
	if (propsUrl != null) {
	  log.debug("passwd props file: " + propsUrl);
	  loadFromProps(propResource);
	}
      } catch (IOException e) {
	log.warning("Error loading " + propResource, e);
      }
    }
  }

  public void installStaticConfigUsers(Configuration users) {
    for (Iterator iter = users.nodeIterator(); iter.hasNext(); ) {
      Configuration oneUser = users.getConfigTree((String)iter.next());
      String user = oneUser.get(USER_PARAM_USER);
      String pwd = oneUser.get(USER_PARAM_PWD);
      String roles = oneUser.get(USER_PARAM_ROLES);
      if (!StringUtil.isNullString(user) &&
	  !StringUtil.isNullString(pwd)) {
	try {
	  UserAccount acct = addStaticUser(user, pwd);
	  if (!StringUtil.isNullString(roles)) {
	    acct.setRoles(roles);
	  }
	} catch (UserExistsException e) {
	  log.debug("Already installed static user: " + user);
	} catch (AccountManager.NotAddedException e) {
	  log.error(e.getMessage());
	}
      }
    }
  }

  /** Delete the user */
  public boolean deleteUser(String name) {
    UserAccount acct = getUser(name);
    if (acct.isStaticUser()) {
      throw new IllegalArgumentException("Can't delete static account: "
					 + acct);
    }
    if (acct == null) {
      return true;
    }
    return deleteUser(acct);
  }

  static String DELETED_REASON = "Deleted";

  /** Delete the user */
  public synchronized boolean deleteUser(UserAccount acct) {
    stateMgr.removeUserAccount(acct);
    internalDeleteUser(acct);
    return true;
  }

  /** Store the current state of the user account on disk */
  public void storeUser(UserAccount acct) throws NotStoredException {
    if (acct.isStaticUser()) {
      throw new IllegalArgumentException("Can't store static account: " + acct);
    }
    if (getUser(acct.getName()) != acct) {
      throw new IllegalArgumentException("Can't store uninstalled account: "
					 + acct);
    }
    storeUserInternal(acct);
  }

  /** Store the current state of the user account on disk */
  public void storeUserInternal(UserAccount acct) throws NotStoredException {
    try {
      stateMgr.storeUserAccount(acct);
    } catch (IOException e) {
      throw new NotStoredException("Could not store user");
    }
  }

  /** Load realm users from properties file.
   * The property file maps usernames to password specs followed by
   * an optional comma separated list of role names.
   *
   * @param propsUrl Filename or url of user properties file.
   * @exception IOException
   */
  public void loadFromProps(String propsUrl) throws IOException {
    if (log.isDebug()) log.debug("Load "+this+" from "+propsUrl);
    Properties props = new Properties();
    InputStream ins = getClass().getResourceAsStream(propsUrl);
    props.load(ins);
    loadFromProps(props);
  }

  public void loadFromProps(Properties props) throws IOException {
    for (Map.Entry ent : props.entrySet()) {
      String username = ent.getKey().toString().trim();
      String credentials = ent.getValue().toString().trim();
      String roles = null;
      int c = credentials.indexOf(',');
      if (c > 0) {
	roles = credentials.substring(c+1).trim();
	credentials = credentials.substring(0,c).trim();
      }
      if (!StringUtil.isNullString(username) &&
	  !StringUtil.isNullString(credentials)) {
	try {
	  UserAccount acct = addStaticUser(username, credentials);
	  if (!StringUtil.isNullString(roles)) {
	    acct.setRoles(roles);
	  }
	} catch (UserExistsException e) {
	  log.debug("Already installed user: " + username);
	} catch (NotAddedException e) {
	  log.error("Can't install user: " + e.getMessage());
	}
      }
    }
  }

  /** Return collection of all user accounts */
  public Collection<UserAccount> getUsers() {
    return accountMap.values();
  }

  /** Return true if named user exists */
  public boolean hasUser(String username) {
    return accountMap.containsKey(username);
  }

  /** Return named UserAccount or null */
  public UserAccount getUserOrNull(String username) {
    UserAccount res = accountMap.get(username);
    log.debug2("getUser("+username + "): " + res);
    return res;
  }

  /** Return named UserAccount or Nobody user */
  public UserAccount getUser(String username) {
    UserAccount res = getUserOrNull(username);
    return res != null ? res : NOBODY_ACCOUNT;
  }

  void loadUsers() {
    // Q: Use stateMgr.getUserAccounts()?
    for (String name : stateMgr.getUserAccountNames()) {
      UserAccount acct = stateMgr.getUserAccount(name);
      if (acct != null) {
      try {
        internalAddUser(acct);
      } catch (UserExistsException e) {
        log.debug("Already installed user: " + e.getMessage());
      } catch (NotAddedException e) {
        log.error("Can't install user: " + e.getMessage());
	}
      }
    }
  }

  /** Called by {@link org.lockss.daemon.Cron.SendPasswordReminder} */
  public boolean checkPasswordReminders() {
    for (UserAccount acct : getUsers()) {
      if (!acct.isStaticUser()) {
	acct.checkPasswordReminder();
      }
    }
    return true;
  }

  // Entry points for externally-initiated actions (by UI or other external
  // agent) which should generate audit events.  The first arg is always
  // the UserAccount of the user performing the action.

  /** Add the user account, if doesn't conflict with an existing user and
   * it has a password. */
  public UserAccount userAddUser(UserAccount actor, UserAccount acct)
      throws NotAddedException, NotStoredException {
    UserAccount res = addUser(acct);
    acct.reportCreateEventBy(actor);
    return res;
  }

  /** Store the current state of the user account on disk */
  public void userStoreUser(UserAccount actor, UserAccount acct)
      throws NotStoredException {
    storeUser(acct);
    acct.reportEditEventBy(actor);
  }

  /** Delete the user */
  public  boolean userDeleteUser(UserAccount actor, UserAccount acct) {
    boolean res = deleteUser(acct);
    if (res) {
      acct.reportEventBy(actor, "deleted");
    } else {
      acct.reportEventBy(actor, "failed to delete");
    }
    return res;
  }

  public boolean isAlertOnLoginLogout() {
    return alertOnLoginLogout;
  }

  public void auditableEvent(String msg) {
    AlertManager alertMgr = getDaemon().getAlertManager();
    if (alertMgr != null) {
      Alert alert = Alert.cacheAlert(Alert.AUDITABLE_EVENT);
      alertMgr.raiseAlert(alert, msg);
    } else {
      log.warning(msg);
    }
  }

  /** Send an alert email to the owner of the account */
  void alertUser(UserAccount acct, Alert alert, String text) {
    if (!mailEnabled) {
      return;
    }
    try {
      String to = acct.getEmail();
      if (to == null && mailAdminIfNoUserEmail) {
	to = adminEmail;
      }
      if (StringUtil.isNullString(to)) {
	log.warning("Can't find address to send alert: " + alert);
	return;
      }
      alert.setAttribute(Alert.ATTR_EMAIL_TO, to);
      AlertManager alertMgr = getDaemon().getAlertManager();
      alertMgr.raiseAlert(alert, text);
    } catch (Exception e) {
      // ignored, expected during testing
    }
  }

  private ObjectSerializer makeObjectSerializer() {
    return new XStreamSerializer();
  }

  public class NotAddedException extends Exception {
    public NotAddedException(String msg) {
      super(msg);
    }
    public NotAddedException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  public class NotStoredException extends Exception {
    public NotStoredException(String msg) {
      super(msg);
    }
    public NotStoredException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  public class UserExistsException extends NotAddedException {
    public UserExistsException(String msg) {
      super(msg);
    }
  }
}
