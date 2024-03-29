/*
 * $Id$
 */

/*

Copyright (c) 2000-2010 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.servlet;

import java.io.IOException;
import java.util.*;
import java.util.List;

import javax.servlet.*;
import javax.servlet.http.HttpSession;

import org.lockss.account.*;
import org.lockss.util.*;
import org.mortbay.html.*;

/** Edit account data, add/remove accts (user admin only)
 */

// XXX prevent browser from filling in new password field(s).

public class AdminEditAccounts extends EditAccountBase {

  protected static final String ACTION_ADMIN_ADD = "Add user";
  protected static final String ACTION_ADMIN_UPDATE = "Update user";
  protected static final String ACTION_ADMIN_DELETE = "Delete user";
  protected static final String ACTION_ADMIN_CANCEL = "Cancel";
  protected static final String ACTION_ADMIN_CONFIRM_DELETE = "Confirm delete";

  protected void handleAccountRequest() throws IOException {
    if (!doesUserHaveRole(ROLE_USER_ADMIN)) {
      return;
    }
    if (StringUtil.isNullString(action)) {
      String user = getParameter(KEY_USER);
      if (StringUtil.isNullString(user)) {
	displayAdminSummary();
      } else {
	displayEditUser(user);
      }
    } else if (action.equals(ACTION_ADMIN_ADD)) {
      String form = getParameter(KEY_FORM);
      if (FORM_SUMMARY.equals(form)) {
	displayAddUser(getParameter(KEY_USER));
      } else if (FORM_ADD_USER.equals(form)) {
	doAdminUpdate();
      } else {
	displayAdminSummary();
      }
    } else if (action.equals(ACTION_ADMIN_UPDATE)) {
      doAdminUpdate();
    } else if (action.equals(ACTION_ADMIN_DELETE)) {
      displayEditUser(getParameter(KEY_USER));
    } else if (action.equals(ACTION_ADMIN_CONFIRM_DELETE)) {
      doAdminDelete();
    } else if (action.equals(ACTION_ADMIN_CANCEL)) {
      displayAdminSummary();
    } else {
      errMsg = "Unknown action: " + action;
      displayAdminSummary();
    }
  }

  protected void doAdminDelete() throws IOException {
    HttpSession session = getSession();
    String name = getParameter(KEY_USER);
    if (action == null || name == null
	|| !name.equals(session.getAttribute(SESSION_KEY_USER))) {
      errMsg = FORM_TAMPERED_ERROR;
      displayAdminSummary();
      return;
    }
    UserAccount acct = acctMgr.getUserOrNull(name);
    if (acct == null) {
      errMsg = "User " + name + " disappeared abruptly!";
      displayAdminSummary();
      return;
    }
    if (acctMgr.userDeleteUser(getUserAccount(), acct)) {
      statusMsg = name + " deleted";
      displayAdminSummary();
      return;
    } else {
      errMsg = "Delete failed!";
      displayAdminSummary();
      return;
    }
  }

  protected void doAdminUpdate() throws IOException {
    HttpSession session = getSession();

    String name = getParameter(KEY_USER);
    if (action == null || name == null
	|| !name.equals(session.getAttribute(SESSION_KEY_USER))) {
      errMsg = FORM_TAMPERED_ERROR;
      displayAdminSummary();
      return;
    }
    String pwd1 = getParameter(KEY_NEW_PASSWD);
    String pwd2 = getParameter(KEY_NEW_PASSWD_2);
    String email = getParameter(KEY_EMAIL);

    String roles = getRolesFromForm();

    UserAccount acct;
    if (action.equals(ACTION_ADMIN_ADD)) {
      if (acctMgr.hasUser(name)) {
	errMsg = "Error: " + name + " already exists";
	displayAdminSummary();
	return;
      }
      acct = acctMgr.createUser(name);
    } else if (action.equals(ACTION_ADMIN_UPDATE)) {
      // Get existing UserAccount to update
      acct = acctMgr.getUserOrNull(name);
      if (acct == null) {
	errMsg = "User " + name + " disappeared abruptly!";
	displayAdminSummary();
	return;
      }
    } else {
      errMsg = FORM_TAMPERED_ERROR;
      displayAdminSummary();
      return;
    }

    acct.setRoles(roles, true);
    if (!StringUtil.isNullString(email)) {
      acct.setEmail(email);
    }

    if (!StringUtil.equalStrings(pwd1, pwd2)) {
      errMsg = "Error: passwords don't match";
      displayEditAccount(acct);
      return;
    }

    if (!StringUtil.isNullString(pwd1)) {
      try {
	acct.setPassword(pwd1, true);
      } catch (UserAccount.IllegalPasswordChange e) {
	errMsg = e.getMessage();
	displayEditAccount(acct);
	return;
      }
    }

    if (action.equals(ACTION_ADMIN_ADD)) {
      try {
	acctMgr.userAddUser(getUserAccount(), acct);
      } catch (AccountManager.UserExistsException e) {
	errMsg = "Error: " + e.getMessage();
	displayAdminSummary();
	return;
      } catch (AccountManager.NotAddedException e) {
	errMsg = "Error: " + e.getMessage();
	displayEditAccount(acct);
	return;
      } catch (AccountManager.NotStoredException e) {
	errMsg = "Error: " + e.getMessage();
	displayEditAccount(acct);
	return;
      }
    } else if (action.equals(ACTION_ADMIN_UPDATE)) {
      // Perform existing user update
      try {
	acctMgr.userStoreUser(getUserAccount(), acct);
      } catch (AccountManager.NotStoredException e) {
	errMsg = "Update failed: " + e.getMessage();
	displayAdminSummary();
	return;
      }
    }

    statusMsg = "Update successful";
    displayAdminSummary();
  }

  void addRole(UserAccount acct, Table tbl, String role) {
    tbl.newCell("align=center");

    String val;
    if (!role.equals(ROLE_USER_ADMIN) && acct.isUserInRole(ROLE_USER_ADMIN)) {
      val = ServletUtil.gray("Yes");
    } else {
      val = acct.isUserInRole(role) ? "Yes" : "No";
    }
    tbl.add(val);
  }

  void addHeading(Table tbl, String head) {
    tbl.newCell("class=\"colhead\" valign=\"bottom\" align=\"center\"");
    tbl.add(head);
  }

  Comparator USER_COMPARATOR = new UserComparator();

  static class UserComparator implements Comparator {
    public int compare(Object o1, Object o2) {
      UserAccount a1 = (UserAccount)o1;
      UserAccount a2 = (UserAccount)o2;
      if (a1.isStaticUser() && !a2.isStaticUser()) {
	return 1;
      }
      if (!a1.isStaticUser() && a2.isStaticUser()) {
	return -1;
      }
      return a1.getName().compareTo(a2.getName());
    }
  }

  Table buildAdminUserTable() {
    if (!doesUserHaveRole(ROLE_USER_ADMIN)) {
      throw new RuntimeException("Shouldn't happen");
    }

    Table tbl = new Table(0, "align=center cellspacing=4 cellpadding=0");
    addHeading(tbl, "User");
    for (RoleDesc rd : roleDescs) {
      String role = rd.name;
      addHeading(tbl, rd.shortDesc + addFootnote(rd.longDesc));
    }
    addHeading(tbl, "Type");
    addHeading(tbl, "Email address");
    addHeading(tbl, "Last Login");
    List<UserAccount> users = new ArrayList(acctMgr.getUsers());
    Collections.sort(users, USER_COMPARATOR);
    DisplayConverter dispConverter = new DisplayConverter();
    for (UserAccount acct : users) {
      tbl.newRow();
      tbl.newCell();
      String name = acct.getName();
      String label = encodeText(name);
      if (name.equals(req.getUserPrincipal().toString())) {
	label = "<b>" + label + "<b>";
      }
      tbl.add(linkIfEditable(acct, label));
      for (RoleDesc rd : roleDescs) {
	String role = rd.name;
	addRole(acct, tbl, rd.name);
      }

      tbl.newCell();
      tbl.add(acct.getType());

      tbl.newCell();
      tbl.add(acct.getEmail());

      tbl.newCell();
      tbl.add(dispConverter.dateString(new Date(acct.getLastLogin())));

      if (!acct.isEnabled()) {
	tbl.newCell();
	tbl.add(linkIfEditable(acct, "Disabled"));
      }
    }
    return tbl;

  }

  String linkIfEditable(UserAccount acct, String label) {
    String name = acct.getName();
    if (acct.isEditable()) {
      Properties p = PropUtil.fromArgs(KEY_USER, name);
      return srvLink(myServletDescr(), label, concatParams(p));
    } else {
      return label;
    }
  }

  /** Display list of existing users and attributes and "Add User"
   * button */
  private void displayAdminSummary() throws IOException {

    Page page = newPage();
    layoutErrorBlock(page);

    Form frm = ServletUtil.newForm(srvURL(myServletDescr()));
    frm.attribute("autocomplete", "OFF");
    Table tbl = new Table(0, "align=center cellspacing=4 border=1 cellpadding=2");
//     tbl.newRow();
//     tbl.newCell("align=center");
//     tbl.add("Account policy: ");
//     tbl.add(acctMgr.getDefaultAccountType());

    tbl.newRow();
    tbl.newCell("align=center");
    tbl.add("Click user name to edit");

    Table userTbl = buildAdminUserTable();
    tbl.newRow();
    tbl.newCell();
    tbl.add(userTbl);

    tbl.newRow();
    tbl.newCell("align=center");
    tbl.add("or Add a user");
    tbl.newRow();
    tbl.newCell("align=center");
    Input in = new Input(Input.Text, KEY_USER);
    in.setSize(20);
    setTabOrder(in);
    tbl.add("Username");
    tbl.add(in);
    tbl.add(new Input(Input.Hidden, KEY_FORM, FORM_SUMMARY));
    List btns = ListUtil.list(ACTION_ADMIN_ADD);
    ServletUtil.layoutAuPropsButtons(this,
                                     tbl,
                                     btns.iterator(),
                                     ACTION_TAG);

    frm.add(tbl);
    page.add(frm);

    endPage(page);
  }

  private void displayAddUser(String name) throws IOException {
    if (StringUtil.isNullString(name)) {
      errMsg = "Error: You must specify a user name to add";
      displayAdminSummary();
      return;
    }
    if (acctMgr.hasUser(name)) {
      errMsg = "Error: " + name + " already exists";
      displayAdminSummary();
      return;
    }
    // Create an initialized user of the appropriate type to supply
    // defaults for the form.  This UserAccount isn't otherwise used.
    UserAccount acct = acctMgr.createUser(name);
    displayEditAccount(acct);
  }

  private void displayEditUser(String name) throws IOException {
    if (StringUtil.isNullString(name)) {
      displayAdminSummary();
    }
    UserAccount acct = acctMgr.getUserOrNull(name);
    if (acct == null) {
      errMsg = "No such user: " + name;
      displayAdminSummary();
      return;
    }
    displayEditAccount(acct);
  }

  static String ROLE_PREFIX = "Role_";

  void addEditRole(Table tbl, UserAccount acct, RoleDesc rd) {
    String role = rd.name;
    tbl.newRow();
    tbl.newCell();
    Input cb = new Input(Input.Checkbox, ROLE_PREFIX + role, "true");
    if (!role.equals(ROLE_USER_ADMIN) && acct.isUserInRole(ROLE_USER_ADMIN)) {
      cb.check();
      cb.attribute("disabled", "true");
    } else {
      if (acct.isUserInRole(role)) {
	cb.check();
      }
    }
    tbl.add(cb);
    tbl.add(rd.longDesc);
  }

  String getRolesFromForm() {
    List lst = new ArrayList();
    for (RoleDesc rd : roleDescs) {
      String role = rd.name;
      if (!StringUtil.isNullString(getParameter(ROLE_PREFIX + role))) {
	lst.add(role);
      }
    }
    return StringUtil.separatedString(lst, ",");
  }

  private Table buildEditRoleTable(UserAccount acct) {
    Table tbl = new Table(0, "align=center cellspacing=1 border=1 cellpadding=2");
    tbl.newRow();
    tbl.newCell();
    tbl.add("Permissions");
    for (RoleDesc rd : roleDescs) {
      addEditRole(tbl, acct, rd);
    }
    return tbl;
  }

  Input addTextInput(Table tbl, String label, String key, boolean isPassword) {
    tbl.newRow();
    tbl.newCell("align=right");
    tbl.add(label);
    Input in = new Input(isPassword ? Input.Password : Input.Text,
			 key);
    setTabOrder(in);
    tbl.add(in);
    return in;
  }

  Input addTextInput(Table tbl, String label, String key, boolean isPassword,
		     String initialValue) {
    tbl.newRow();
    tbl.newCell("align=right");
    tbl.add(label);
    Input in = new Input(isPassword ? Input.Password : Input.Text,
			 key, initialValue);
    setTabOrder(in);
    tbl.add(in);
    return in;
  }

  private Table buildEditAttrsTable(UserAccount acct) {
    Table tbl = new Table(0, "align=center cellspacing=1 border=1 cellpadding=2");
    addTextInput(tbl, "New password: ", KEY_NEW_PASSWD, true, "");
    addTextInput(tbl, "Confirm password: ", KEY_NEW_PASSWD_2, true, "");
    Input eml = addTextInput(tbl, "Email address: ", KEY_EMAIL, false);
    eml.attribute("value", acct.getEmail());
    return tbl;
  }

  private void displayEditAccount(UserAccount acct)
      throws IOException {
    boolean isDelete = ACTION_ADMIN_DELETE.equals(action);

    List actions;
    StringBuilder sb = new StringBuilder();
    if (acctMgr.hasUser(acct.getName())) {
      if (isDelete) {
	sb.append("Confirm delete user: ");
	actions = ListUtil.list(ACTION_ADMIN_CONFIRM_DELETE,
				ACTION_ADMIN_CANCEL);
      } else {
	sb.append("Edit user: ");
	actions = ListUtil.list(ACTION_ADMIN_UPDATE, ACTION_ADMIN_DELETE);
      }
    } else {
      sb.append("Add user: ");
      actions = ListUtil.list(ACTION_ADMIN_ADD, ACTION_ADMIN_CANCEL);
    }
    sb.append(" ");
    sb.append(acct.getName());

    String disMsg = acct.getDisabledMessage();
    if (disMsg != null) {
      sb.append("<br>");
      sb.append(disMsg);
    }
    HttpSession session = getSession();
    session.setAttribute(SESSION_KEY_USER, acct.getName());

    Page page = newPage();
    layoutErrorBlock(page);
    ServletUtil.layoutExplanationBlock(page, sb.toString());
    Form frm = ServletUtil.newForm(srvURL(myServletDescr()));
    frm.attribute("autocomplete", "OFF");
    Table tbl = new Table(0, "align=center cellspacing=4 border=1 cellpadding=2");
    tbl.newRow();
    tbl.newCell();

    if (!isDelete) {
      tbl.add(buildEditAttrsTable(acct));
      tbl.add(buildEditRoleTable(acct));
    }
    tbl.add(new Input(Input.Hidden, KEY_FORM, FORM_ADD_USER));
    tbl.add(new Input(Input.Hidden, KEY_USER, acct.getName()));
    frm.add(tbl);

    ServletUtil.layoutAuPropsButtons(this,
                                     frm,
                                     actions.iterator(),
                                     ACTION_TAG);
    page.add(frm);
    endPage(page);
  }

  // make me a link in nav table if not on summary page
  protected boolean linkMeInNav() {
    return action != null
      || !StringUtil.isNullString(getParameter(KEY_USER));
  }

}
