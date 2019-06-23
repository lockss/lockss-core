/*
 * $Id$
 */

/*

 Copyright (c) 2012 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.plugin;

import java.util.*;
import org.apache.commons.lang3.*;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.repository.RepositoryManager;

public class AuEvent {

  /** Describes the event that caused the AuEventHandler to be invoked.
   * May provide more detail than the AuEventHandler method name. */
  public enum Type {
    /** AU created via UI. */
    Create,
    /** AU deleted via UI. */
    Delete,
    /** Previously created AU started at daemon startup. */
    StartupCreate,
    /** AU deactivated via UI. */
    Deactivate,
    /** AU reactivated via UI.  (Not currently used.) */
    Reactivate,
    /** AU briefly deleted as part of a restart operation. */
    RestartDelete,
    /** AU recreated as part of a restart operation. */
    RestartCreate,
    /** AU config changed (non-def params only, doesn't happen in normal use).
     */
    Reconfig,
    /** AU's content chaged. */
    ContentChanged
  };

  /** Details of a content change resulting from a crawl or repair
   * TK should also be triggered by import. */
  public static class ContentChangeInfo {
    /** Crawl means new content crawl, Repair means either peer repair or
     * repair crawl */
    public enum Type {
      Crawl, Repair;
    }

    private ContentChangeInfo.Type type;
    private boolean complete;
    private List<String> urls;
    private Map<String,Integer> mimeCounts;
    private int numUrls;

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[AuChange: ");
      sb.append(type);
      sb.append(", ");
      sb.append(numUrls);
      if (urls != null && numUrls != urls.size()) {
	sb.append("/");
	sb.append(urls.size());
      }
      sb.append(" urls, ");
      sb.append(mimeCounts != null ? mimeCounts.size() : 0);
      sb.append(" mime types]");
      return sb.toString();
    }

    public static final String KEY_TYPE = "type";
    public static final String KEY_COMPLETE = "complete";
    public static final String KEY_URLS = "urls";
    public static final String KEY_NUM_URLS = "num_urls";
    public static final String KEY_MIME_COUNTS = "mime_counts";

    /** Build a Map representation of the info in this
     * ContentChangeInfo, which is suitable for sending in a JMS
     * message. */
    public Map<String,Object> toMap() {
      Map<String,Object> map = new HashMap<String,Object>();
      if (type != null) map.put(KEY_TYPE, type.toString());
      map.put(KEY_COMPLETE, complete);
      if (mimeCounts != null) map.put(KEY_MIME_COUNTS, mimeCounts);
      map.put(KEY_NUM_URLS, numUrls);
      if (urls != null) map.put(KEY_URLS, urls);
      return map;
    }

    /** Build a ContentChangeInfo instance from the Map representation
     * produced by {@link #toMap()} */
    public static ContentChangeInfo fromMap(Map<String,Object> map) {
      ContentChangeInfo res = new ContentChangeInfo();
      if (map.get(KEY_TYPE) != null) {
	res.type = ContentChangeInfo.Type.valueOf((String)map.get(KEY_TYPE));
      }
      if (map.get(KEY_COMPLETE) != null) {
	res.complete = (boolean)map.get(KEY_COMPLETE);
      }
      res.mimeCounts = (Map)map.get(KEY_MIME_COUNTS);
      if (map.get(KEY_NUM_URLS) != null) {
	res.numUrls = (int)map.get(KEY_NUM_URLS);
      }
      res.urls = (List)map.get(KEY_URLS);
      return res;
    }

    public void setType(ContentChangeInfo.Type type) {
      this.type = type;
    }

    /** Type of content changes: Crawl means new content crawl, Repair
     * means either peer repair or repair crawl */
    public ContentChangeInfo.Type getType() {
      return type;
    }

    public void setComplete(boolean complete) {
      this.complete = complete;
    }

    /** True if this is expected to be a partial change (<i>e.g.</i> new
     * content crawl that ended in error and will be repeated/continued
     * soonish */
    public boolean isComplete() {
      return complete;
    }

    public void setUrls(List<String> urls) {
      this.urls = urls;
    }

    /** The list of URLs that change, if known and manageable. */
    public Collection<String> getUrls() {
      return urls;
    }

    /** True iff {@link #getUrls()} will return an accurate collection of
     * changed URLs */
    public boolean hasUrls() {
      return urls != null && numUrls == urls.size();
    }

    public void setNumUrls(int numUrls) {
      this.numUrls = numUrls;
    }

    /** The number of URLs that changed.  This is always available even if
     * {@link #hasUrls()} is false */
    public int getNumUrls() {
      return numUrls;
    }

    public void setMimeCounts(Map<String,Integer> mimeCounts) {
      this.mimeCounts = mimeCounts;
    }

    /** Returns a map from MIME type to the count of files of that MIME
     * type that were changed.  If null, the MIME counts aren't
     * available. */
    public Map<String,Integer> getMimeCounts() {
      return mimeCounts;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hcb = new HashCodeBuilder();
      hcb.append(getType());
      hcb.append(isComplete());
      hcb.append(getNumUrls());
      hcb.append(getUrls());
      hcb.append(getMimeCounts());
      return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
	return true;
      }
      if (obj instanceof ContentChangeInfo) {
	ContentChangeInfo ci = (ContentChangeInfo)obj;
	if (getType() == ci.getType() &&
	    isComplete() == ci.isComplete() &&
	    getNumUrls() == ci.getNumUrls() &&
	    ObjectUtils.equals(getUrls(), ci.getUrls()) &&
	    ObjectUtils.equals(getMimeCounts(), ci.getMimeCounts())) {
	  return true;
	}
      }
      return false;
    }
  }

  private String auid;
//   private ArchivalUnit au;
  private AuEvent.Type type;
  private boolean inBatch;
  private ContentChangeInfo changeInfo;
  private Configuration oldConfig;

  private AuEvent() {
  }

  AuEvent(ArchivalUnit au, AuEvent.Type type) {
    this(au.getAuId(), type);
//     this.au = au;
  }

  AuEvent(String auid, AuEvent.Type type) {
    this.auid = auid;
    this.type = type;
  }

  public static AuEvent forAu(ArchivalUnit au, AuEvent.Type type) {
    return new AuEvent(au, type);
  }

  public static AuEvent forAuId(String auid, AuEvent.Type type) {
    return new AuEvent(auid, type);
  }

  public static AuEvent forAu(ArchivalUnit au, AuEvent model) {
    AuEvent res = AuEvent.forAu(au, model.getType());
    res.setInBatch(model.isInBatch());
    res.setChangeInfo(model.getChangeInfo());
    res.setOldConfiguration(model.getOldConfiguration());
    return res;
  }

  public static AuEvent model(AuEvent.Type type) {
    return new AuEvent((String)null, type);
  }

  public AuEvent setInBatch() {
    return setInBatch(true);
  }

  public AuEvent setInBatch(boolean val) {
    inBatch = val;
    return this;
  }

  public AuEvent setChangeInfo(ContentChangeInfo cci) {
    changeInfo = cci;
    return this;
  }

  public AuEvent setOldConfiguration(Configuration oldConfig) {
    this.oldConfig = oldConfig;
    return this;
  }

  public String getAuId() {
    return auid;
  }

  public AuEvent.Type getType() {
    return type;
  }

  public boolean isInBatch() {
    return inBatch;
  }

  public Configuration getOldConfiguration() {
    return oldConfig;
  }

  public ContentChangeInfo getChangeInfo() {
    return changeInfo;
  }

  public static final String KEY_TYPE = "type";
  public static final String KEY_AUID = "auid";
  public static final String KEY_REPO_SPEC = "repository_spec";
  public static final String KEY_CHANGE_INFO = "change_info";
  public static final String KEY_OLD_CONFIG = "old_config";
  public static final String KEY_IN_BATCH = "in_batch";

  /** Build a Map representation of the info in this AuEvent, which is
   * suitable for sending in a JMS message. */
  public Map<String,Object> toMap() {
    Map<String,Object> map = new HashMap<String,Object>();
    map.put(KEY_AUID, auid);
    String repo =
      CurrentConfig.getParam(RepositoryManager.PARAM_V2_REPOSITORY,
			     RepositoryManager.DEFAULT_V2_REPOSITORY);
    if (!StringUtil.isNullString(repo)) {
      map.put(KEY_REPO_SPEC, repo);
    }
    if (type != null) map.put(KEY_TYPE, type.toString());
    if (changeInfo != null) map.put(KEY_CHANGE_INFO, changeInfo.toMap());
    if (oldConfig != null) map.put(KEY_OLD_CONFIG, oldConfig.toStringMap());
    if (inBatch) map.put(KEY_IN_BATCH, inBatch);
    return map;
  }

  /** Build an AuEvent instance from the Map representation produced
   * by {@link #toMap()} */
  public static AuEvent fromMap(Map<String,Object> map) {
    String auid = StringPool.AUIDS.intern((String)map.get(KEY_AUID));
    AuEvent res = AuEvent.forAuId(auid,
                                  AuEvent.Type.valueOf((String)map.get(KEY_TYPE)));
    if (map.get(KEY_CHANGE_INFO) != null) {
      res.changeInfo = ContentChangeInfo.fromMap((Map)map.get(KEY_CHANGE_INFO));
    }
    if (map.get(KEY_OLD_CONFIG) != null) {
      Properties props = new Properties();
      props.putAll((Map)map.get(KEY_OLD_CONFIG));
      res.oldConfig = ConfigManager.fromProperties(props);
    }
    if (map.get(KEY_IN_BATCH) != null) {
      res.inBatch = (boolean)map.get(KEY_IN_BATCH);
    }
    return res;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(getAuId());
    hcb.append(getType());
    hcb.append(getChangeInfo());
    hcb.append(getOldConfiguration());
    hcb.append(isInBatch());
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AuEvent) {
      AuEvent event = (AuEvent)obj;
      if (Objects.equals(getAuId(), event.getAuId()) &&
	  Objects.equals(getType(), event.getType()) &&
	  isInBatch() == event.isInBatch() &&
	  Objects.equals(getChangeInfo(), event.getChangeInfo()) &&
	  Objects.equals(getOldConfiguration(), event.getOldConfiguration())) {
	return true;
      }
    }
    return false;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[AuEvent: ");
    sb.append(type.toString());
    if (inBatch) sb.append("{B)");
    sb.append(" AU: ");
    sb.append(auid);
    if (changeInfo != null) {
      sb.append(" change: ");
      sb.append(changeInfo.toString());
    }
    if (oldConfig != null) {
      sb.append(" oldConfig: ");
      sb.append(oldConfig.toString());
    }
    sb.append("]");
    return sb.toString();
  }
}
