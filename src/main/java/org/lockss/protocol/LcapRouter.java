/*
 * $Id$
 */

/*

Copyright (c) 2000-2008 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.protocol;
import java.io.*;
import java.net.*;
import java.util.*;
import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.io.FileUtil;
import org.lockss.util.os.PlatformUtil;
import org.lockss.config.Configuration;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.poller.*;

/**
 * LcapRouter is the topmost layer of the comm services, to which clients
 * pass outgoing messages and with which they register incoming message
 * handlers.  This class interacts with LcapStreamComm to send and receive
 * messages.
 */
public class LcapRouter
  extends BaseLockssDaemonManager implements ConfigurableManager {

  private static final String PREFIX =
    Configuration.PREFIX + "comm.";
  /** Path to directory to store temporary files holding V3 LCAP message
   * data */
  private static final String PARAM_V3_LCAP_MESSAGE_DATA_DIR =
    PREFIX + "v3LcapMessageDataDir";
  private static final String DEFAULT_V3_LCAP_MESSAGE_DATA_DIR =
    "System tmpdir";

  public static final String PARAM_MIGRATE_FROM =
      PREFIX + "migrateFrom";
  public static final String DEFAULT_MIGRATE_FROM = null;

  static Logger log = Logger.getLogger();

  private IdentityManager idMgr;
  private LcapStreamComm scomm = null;

  private List messageHandlers = new ArrayList();
  private File dataDir = null;
  private PeerIdentity migrateFrom = null;

  public void startService() {
    super.startService();
    LockssDaemon daemon = getDaemon();
    try {
      scomm = daemon.getStreamCommManager();
      scomm.registerMessageHandler(PeerMessage.PROTOCOL_LCAP_V3,
				   new LcapStreamComm.MessageHandler() {
				     public void handleMessage(PeerMessage
							       msg) {
				       handleIncomingPeerMessage(msg);
				     }});
    } catch (IllegalArgumentException e) {
      log.warning("No stream comm");
      scomm = null;
    }
    idMgr = daemon.getIdentityManager();
  }

  public void stopService() {
    if (scomm != null) {
      scomm.unregisterMessageHandler(PeerMessage.PROTOCOL_LCAP_V3);
    }
    super.stopService();
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      String paramDataDir = config.get(PARAM_V3_LCAP_MESSAGE_DATA_DIR,
				       PlatformUtil.getSystemTempDir());
      File dir = new File(paramDataDir);
      if (FileUtil.ensureDirExists(dir)) {
	dataDir = dir;
	log.debug2("V3LcapMessage data dir: " + dataDir);
      } else {
	log.warning("No V3LcapMessage data dir: " + dir);
	dataDir = null;
      }

      String migrateFromVal =
          config.get(PARAM_MIGRATE_FROM, DEFAULT_MIGRATE_FROM);
      if (!StringUtil.isNullString(migrateFromVal)) {
        try {
          migrateFrom = idMgr.findPeerIdentity(migrateFromVal);
          log.info("Forwading outgoing LCAP traffic to: " + migrateFrom);
        } catch (IdentityManager.MalformedIdentityKeyException e) {
          log.error("Malformed migrateFrom peer identity: " +
              migrateFromVal);
          migrateFrom = null;
        }
      } else {
        migrateFrom = null;
      }
    }
  }

  /** Send a message to a peer.
   * @param msg the message to send
   * @param id the identity of the peer to which to send the message
   * @throws IOException if message couldn't be sent
   */
  public void sendTo(V3LcapMessage msg, PeerIdentity id)
      throws IOException {
    PeerMessage pm = makePeerMessage(msg);
    if (migrateFrom == null) {
      scomm.sendTo(pm, id);
    } else {
      // Route outbound messages through the migrateFrom machine
      scomm.sendTo(pm, migrateFrom);
    }
  }

  /** Encode a V3LcapMessage into a PeerMessage */
  PeerMessage makePeerMessage(V3LcapMessage lmsg) throws IOException {
    OutputStream out = null;
    InputStream in = null;
    try {
      PeerMessage pmsg = newPeerMessage(lmsg.getEstimatedEncodedLength());
      pmsg.setProtocol(PeerMessage.PROTOCOL_LCAP_V3);
      pmsg.setExpiration(lmsg.getExpiration());
      int rmax = lmsg.getRetryMax();
      if (rmax >= 0) {
	pmsg.setRetryMax(rmax);
      }
      long rint = lmsg.getRetryInterval();
      if (rint > 0) {
	pmsg.setRetryInterval(rint);
      }
      out = pmsg.getOutputStream();
      in = lmsg.getInputStream();
      StreamUtil.copy(in, out);
      return pmsg;
    } finally {
      IOUtil.safeClose(in);
      IOUtil.safeClose(out);
    }
  }

  /** Decode a PeerMessage into a V3LcapMessage  */
  V3LcapMessage makeV3LcapMessage(PeerMessage pmsg) throws IOException {
    InputStream in = null;
    try {
      in = pmsg.getInputStream();
      V3LcapMessage lmsg = new V3LcapMessage(in, dataDir, getDaemon());
      lmsg.setOriginatorId(pmsg.getSender());
      return lmsg;
    } finally {
      IOUtil.safeClose(in);
    }
  }

  PeerMessage newPeerMessage(long estSize) {
    return scomm.newPeerMessage(estSize);
  }

  // Incoming peer message - decode to V3LcapMessage and run handlers
  void handleIncomingPeerMessage(PeerMessage pmsg) {
    try {
      LcapMessage lmsg = makeV3LcapMessage(pmsg);
      runHandlers(lmsg);
    } catch (Exception e) {
      log.warning("Exception while processing incoming " + pmsg, e);
    } finally {
      pmsg.delete();
    }
  }

  private void runHandlers(LcapMessage msg) {
    for (Iterator iter = messageHandlers.iterator(); iter.hasNext();) {
      runHandler((MessageHandler)iter.next(), msg);
    }
  }

  private void runHandler(MessageHandler handler, LcapMessage msg) {
    try {
      handler.handleMessage(msg);
    } catch (Exception e) {
      log.error("callback threw", e);
    }
  }

  /**
   * Register a {@link LcapRouter.MessageHandler}, which will be called
   * whenever an LcapMessage is received.
   * @param handler MessageHandler to add
   */
  public void registerMessageHandler(MessageHandler handler) {
    if (!messageHandlers.contains(handler)) {
      messageHandlers.add(handler);
    }
  }

  /**
   * Unregister a {@link LcapRouter.MessageHandler}.
   * @param handler MessageHandler to remove
   */
  public void unregisterMessageHandler(MessageHandler handler) {
    messageHandlers.remove(handler);
  }

  /**
   * The LcapRouter.MessageHandler interface defines the
   * callback registered by clients of {@link LcapRouter} who want to process
   * incoming LcapMessages.
   */
  public interface MessageHandler {
    /**
     * Callback used to inform clients that an LcapMessage has been received.
     * @param msg the received LcapMessage
     * @see LcapRouter#registerMessageHandler */
    public void handleMessage(LcapMessage msg);
  }
}
