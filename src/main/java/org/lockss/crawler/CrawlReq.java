/*

Copyright (c) 2000-2020 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.crawler;

import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuUtil;
import org.lockss.state.AuState;

public class CrawlReq {

  protected String auid;
  String auName;
  CrawlManager.Callback cb;
  Object cookie;
  String rateKey;
  int priority = 0;
  int refetchDepth = -1;
  private ArchivalUnit au;
  private AuState aus = null;
  protected CrawlerStatus crawlerStatus = null;

  /**
   * No-arguments constructor.
   */
  protected CrawlReq() {
  }

  public CrawlReq(ArchivalUnit au, CrawlerStatus crawlerStatus) {
    this(au, null, null, crawlerStatus);
  }

  public CrawlReq(ArchivalUnit au, CrawlManager.Callback cb, Object cookie,
      CrawlerStatus crawlerStatus) {
    this.au = au;
    this.auid = au.getAuId();
    this.auName = au.getName();
    this.cb = cb;
    this.cookie = cookie;
    this.aus = AuUtil.getAuState(au);
    this.rateKey = au.getFetchRateLimiterKey();
    this.crawlerStatus = crawlerStatus;
  }

  public void auDeleted() {
    this.au = null;
    this.aus = null;
  }

  public void refresh(ArchivalUnit au, AuState aus) {
    this.au = au;
    this.aus = aus;
  }

  public String getAuName() {
    return auName;
  }

  public boolean isActive() {
    return au != null /* && aus != null */;
  }

  public String getAuId() {
    return auid;
  }

  public ArchivalUnit getAu() {
    return au;
  }

  public CrawlManager.Callback getCb() {
    return cb;
  }

  public void setCb(CrawlManager.Callback cb) {
    this.cb = cb;
  }

  public Object getCookie() {
    return cookie;
  }

  public AuState getAuState() {
    return aus;
  }

  public String getRateKey() {
    return rateKey;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int val) {
    priority = val;
  }

  public boolean isHiPri() {
    return priority > 0;
  }

  public int getRefetchDepth() {
    return refetchDepth;
  }

  public void setRefetchDepth(int val) {
    refetchDepth = val;
  }

  public CrawlerStatus getCrawlerStatus() {
    return crawlerStatus;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[CrawlReq");
    if (au == null) {
      sb.append("(I): ");
      if (auName != null) {
        sb.append(auName);
      }
      else {
        sb.append(auid);
      }
    }
    else {
      sb.append(": ");
      sb.append(au);
    }
    sb.append(", pri: ");
    sb.append(priority);
    sb.append(", cb: ");
    sb.append(cb);
    if (refetchDepth >= 0) {
      sb.append(", depth: ");
      sb.append(refetchDepth);
    }
    sb.append(", crawlerStatus: ");
    sb.append(crawlerStatus);
    sb.append("]");
    return sb.toString();
  }
}
