/*
 * $Id$
 */

/*

Copyright (c) 2000-2003 Board of Trustees of Leland Stanford Jr. University,
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


package org.lockss.test;

import java.io.*;
import java.util.*;
import org.lockss.app.*;
import org.lockss.daemon.Crawler;
import org.lockss.protocol.DatedPeerIdSetImpl;
import org.lockss.protocol.IdentityManager;
import org.lockss.state.*;
import org.lockss.plugin.*;
import org.lockss.protocol.DatedPeerIdSet;
import org.lockss.config.Configuration;

/**
 * MockHistoryRepository is a mock implementation of the HistoryRepository.
 */
public class MockHistoryRepository implements HistoryRepository {
  private HashMap storedHistories = new HashMap();
  private HashMap storedNodes = new HashMap();
  private String baseDir;
  private ArchivalUnit au;
  private IdentityManager idMgr;
  private File peerIdFile;

  public MockHistoryRepository(){}

  public void initService(LockssApp app) throws LockssAppException { }
  public void startService() {
  }
  public void stopService() {
    storedHistories = new HashMap();
    storedNodes = new HashMap();
  }
  public LockssApp getApp() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public void setAuConfig(Configuration auConfig) {
  }

  public long getAuCreationTime() {
    return -1;
  }

  public void setIdMgr(IdentityManager idMgr) {
    this.idMgr = idMgr;
  }

  public void setPeerIdFile(File peerIdFile)
  {
    this.peerIdFile = peerIdFile;
  }
  public DatedPeerIdSet getNoAuPeerSet() {
    return new DatedPeerIdSetImpl(peerIdFile, idMgr);
  }

  public AuState getAuState() {
    return LockssDaemon.getManagerByTypeStatic(StateManager.class).getAuState(au);
  }

}
