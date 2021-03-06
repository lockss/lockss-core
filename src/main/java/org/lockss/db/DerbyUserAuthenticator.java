/*

 Copyright (c) 2013-2016 Board of Trustees of Leland Stanford Jr. University,
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

/**
 * Derby database custom authenticator.
 * 
 * @author Fernando García-Loygorri
 */
package org.lockss.db;

import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.authentication.UserAuthenticator;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;

public class DerbyUserAuthenticator implements UserAuthenticator {

  private static final Logger log = Logger.getLogger();

  /**
   * No-argument constructor.
   */
  public DerbyUserAuthenticator() {
  }

  /**
   * Provides an indication of whether the passed credentials are valid.
   * 
   * @param userName
   *          A String with the name of the user.
   * @param userPassword
   *          A String with the user password.
   * @param databaseName
   *          A String with the name of the database.
   * @param info
   *          A Properties with the additional authentication information.
   */
  public boolean authenticateUser(String userName, String userPassword,
      String databaseName, Properties info) throws SQLException {
    final String DEBUG_HEADER = "authenticateUser(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "userName = " + userName);
      //log.debug2(DEBUG_HEADER + "userPassword = " + userPassword);
      log.debug2(DEBUG_HEADER + "databaseName = " + databaseName);
      log.debug2(DEBUG_HEADER + "info = " + info);
    }

    // Get the expected credentials.
    DbCredentials credentials =
	DbManager.getDbCredentialsMap().get(databaseName);
    if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "credentials = " + credentials);

    // Fail authentication if there are no expected credentials.
    if (credentials == null) {
      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "Missing credentials: result is false");
      return false;
    }

    // No authentication is needed when using the embedded data source.
    if (EmbeddedDataSource.class.getCanonicalName()
	.equals(credentials.getClassName())) {
      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "EmbeddedDataSource: result is true");
      return true;
    }

    String user = credentials.getUser();
    String password = credentials.getPassword();

    // Fail authentication if either the user or the password are missing.
    if (StringUtil.isNullString(userName)
	|| StringUtil.isNullString(userPassword)) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER
	  + "Missing user and/or password: result is false");
      return false;
    }

    // Fail authentication if either the user or the password do not match.
    if (!userName.equals(user) || !userPassword.equals(password)) {
      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "Invalid credentials: result is false");
      return false;
    }

    // The credentials are valid.
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "Not EmbeddedDataSource: result is true");
    return true;
  }
}
