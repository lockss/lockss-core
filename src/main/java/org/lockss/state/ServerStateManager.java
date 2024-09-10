/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.state;


import org.lockss.account.UserAccount;
import org.lockss.log.*;

/** PersistentStateManager that also sends JMS state changed notifications */
public class ServerStateManager extends PersistentStateManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  @Override
  public void startService() {
    super.startService();
    setUpJmsSend();
  }

  @Override
  public void stopService() {
    stopJms();
    super.stopService();
  }

  @Override
  protected void doNotifyAuStateChanged(String key, String json,
					String cookie) {
    log.debug("Sending AuState changed notification for {}: {}", key, json);
    sendAuStateChangedEvent(key, json, cookie);
  }

  @Override
  protected void doNotifyAuAgreementsChanged(String key, String json,
					     String cookie) {
    log.debug("Sending AuAgreement changed notification for {}: {}", key, json);
    sendAuAgreementsChangedEvent(key, json, cookie);
  }

  @Override
  protected void doNotifyAuSuspectUrlVersionsChanged(String key, String json,
					     String cookie) {
    log.debug("Sending AuSuspectUrlVersions changed notification for {}: {}",
	      key, json);
    sendAuSuspectUrlVersionsChangedEvent(key, json, cookie);
  }

  @Override
  protected void doNotifyNoAuPeerSetChanged(String key, String json,
					     String cookie) {
    log.debug("Sending NoAuPeerSet changed notification for {}: {}",
	      key, json);
    sendNoAuPeerSetChangedEvent(key, json, cookie);
  }

  @Override
  protected void doNotifyUserAccountChanged(UserAccount.UserAccountChange op, String username, String json,
                                            String cookie) {
    log.debug("Sending UserAccount changed notification for {}: {}", username, json);
    sendUserAccountChangedEvent(username, json, op, cookie);
  }
}
