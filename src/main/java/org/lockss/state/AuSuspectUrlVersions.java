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

package org.lockss.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.*;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.time.TimeBase;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.AuUtil;
import org.lockss.app.LockssApp;
import org.lockss.app.LockssDaemon;
import org.lockss.hasher.HashResult;

/**
 * Instances represent the set of versions of urls in an AU that
 * have been marked suspect because they failed a local hash
 * verification, meaning that either the url's content or its
 * stored hash is corrupt.  This class is thread safe.
 */
public class AuSuspectUrlVersions implements LockssSerializable {
  private static final Logger log = Logger.getLogger();

  public static class SuspectUrlVersion implements LockssSerializable {
    private String url;
    private int version;
    private long created;
    private HashResult computedHash;
    private HashResult storedHash;

    protected SuspectUrlVersion() {
    }

    protected SuspectUrlVersion(String url, int version) {
      this.url = url;
      this.version = version;
      this.created = TimeBase.nowMs();
      this.computedHash = null;
      this.storedHash = null;
    }

    protected SuspectUrlVersion(String url, int version, String algorithm,
				byte[] computedHash, byte[] storedHash) {
      this.url = url;
      this.version = version;
      this.created = TimeBase.nowMs();
      this.computedHash = HashResult.make(computedHash, algorithm);
      this.storedHash = HashResult.make(storedHash, algorithm);
    }

    protected SuspectUrlVersion(String url, int version,
				HashResult computedHash,
				HashResult storedHash) {
      this.url = url;
      this.version = version;
      this.created = TimeBase.nowMs();
      this.computedHash = computedHash;
      this.storedHash = storedHash;
    }

    public String getUrl() {
      return url;
    }
    public int getVersion() {
      return version;
    }
    public long getCreated() {
      return created;
    }
    public HashResult getComputedHash() {
      return computedHash;
    }
    public HashResult getStoredHash() {
      return storedHash;
    }
    public int hashCode() {
      return url.hashCode() + version;
    }
    public boolean equals(Object obj) {
      if (obj instanceof SuspectUrlVersion) {
	SuspectUrlVersion suv = (SuspectUrlVersion) obj;
	return (url.equals(suv.getUrl()) && version == suv.getVersion());
      }
      return false;
    }

    public String toString() {
      return "[SuspectUrl: " + url + ":" + version +
	", comp: " + computedHash + ", stored: " + storedHash + "]";
    }
  }

  private String auid;

  private Set<SuspectUrlVersion> suspectVersions =
    new HashSet<SuspectUrlVersion>();

  protected AuSuspectUrlVersions() {
  }

  protected AuSuspectUrlVersions(String auid) {
    this.auid = auid;
  }

  /**
   * Creates and provides a new instance.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @return an AuSuspectUrlVersions with the newly created object.
   */
  @JsonCreator
  public static AuSuspectUrlVersions make(@JsonProperty("auid") String auid) {
    AuSuspectUrlVersions auSuspectUrlVersions = new AuSuspectUrlVersions(auid);
    return auSuspectUrlVersions;
  }

  /**
   * Return true if the version of the url has been marked suspect.
   * @return true if version of url has been marked suspect
   */
  public synchronized boolean isSuspect(String url, int version) {
    return suspectVersions.contains(new SuspectUrlVersion(url, version));
  }

  /**
   * Mark the version of the url as suspect
   */
  public synchronized void markAsSuspect(String url, int version) {
    if (isSuspect(url, version)) {
      throw new UnsupportedOperationException("Re-marking as suspect");
    }
    if (log.isDebug3()) {
      log.debug3("Mark suspect: ver " + version + " of: " + url);
    }
    suspectVersions.add(new SuspectUrlVersion(url, version));
  }

  /**
   * Unmark the version of the url as suspect
   */
  public synchronized void unmarkAsSuspect(String url, int version) {
    if (!isSuspect(url, version)) {
      log.warning("Unmark but not suspect: ver " + version + " of: " + url);
    }
    if (log.isDebug3()) {
      log.debug3("Unmark suspect: ver " + version + " of: " + url);
    }
    suspectVersions.remove(new SuspectUrlVersion(url, version));
  }

  /**
   * Mark the version of the url as suspect
   */
  public synchronized void markAsSuspect(String url, int version,
					 String algorithm,
					 byte[] computedHash,
					 byte[] storedHash) {
    if (isSuspect(url, version)) {
      throw new UnsupportedOperationException("Re-marking as suspect");
    }
    if (log.isDebug3()) {
      log.debug3("Mark suspect: ver " + version + " of: " + url);
    }
    suspectVersions.add(new SuspectUrlVersion(url, version, algorithm,
					      computedHash, storedHash));
  }

  /**
   * Mark the version of the url as suspect
   */
  public synchronized void markAsSuspect(String url, int version,
					 HashResult computedHash,
					 HashResult storedHash) {
    if (isSuspect(url, version)) {
      throw new UnsupportedOperationException("Re-marking as suspect");
    }
    if (log.isDebug3()) {
      log.debug3("Mark suspect: ver " + version + " of: " + url);
    }
    suspectVersions.add(new SuspectUrlVersion(url, version,
					      computedHash, storedHash));
  }

  /** Return true if the set is empty */
  public synchronized boolean isEmpty() {
    return suspectVersions.isEmpty();
  }

  /** Return the collection of SuspectUrlVersion */
  public synchronized List<SuspectUrlVersion> getSuspectList() {
    return new ArrayList(suspectVersions);
  }

  /** Return stored SuspectUrlVersion with the same url and version of the
   * specified one, if any.  Inefficient, used in testing */
  public synchronized SuspectUrlVersion getSuspectUrlVersion(String url,
							     int version) {
    SuspectUrlVersion pat = new SuspectUrlVersion(url, version);
    for (SuspectUrlVersion suv : suspectVersions) {
      if (suv.equals(pat)) {
	return suv;
      }
    }
    return null;
  }

  /**
   * Count the URLs whose current version in the AU is suspect
   * @return the number of URLs whose current version is suspect
   * @param au the ArchivalUnit
   */
  // Unit test is in TestLockssRepositoryImpl
  public int countCurrentSuspectVersions(ArchivalUnit au) {
    int ret = 0;
    for (SuspectUrlVersion suv : suspectVersions) {
      int currentVersion = au.makeCachedUrl(suv.getUrl()).getVersion();
      if (suv.getVersion() == currentVersion) {
	ret++;
	if (log.isDebug3()) {
	  log.debug3(suv.getUrl() + ": current suspect " + suv.getVersion());
	}
      } else if (log.isDebug3()) {
	log.debug3(suv.getUrl() + ": current " + currentVersion +
		   " suspect " + suv.getVersion());
      }
    }
    return ret;
  }

  /**
   * Provides the AUID.
   * 
   * @return A String with the AUID.
   */
  public String getAuid() {
    return auid;
  }

  /**
   * Provides a serialized version of this entire object as a JSON string.
   * 
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  public String toJson() throws IOException {
    return toJson((Set<SuspectUrlVersion>)null);
  }

  /**
   * Provides a serialized version of this object with the single field as a
   * JSON string.
   * 
   * @param suspectUrlVersion
   *          A SuspectUrlVersion with the field to be included.
   * 
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  public String toJson(SuspectUrlVersion suspectUrlVersion) throws IOException {
    return toJson(SetUtil.set(suspectUrlVersion));
  }

  /**
   * Provides a serialized version of this object with the passed fields as a
   * JSON string.
   * 
   * @param suspectUrlVersions
   *          A Set<SuspectUrlVersion> with the fields to be included.
   * 
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  public synchronized String toJson(Set<SuspectUrlVersion> suspectUrlVersions)
      throws IOException {
    return AuUtil.jsonFromAuSuspectUrlVersions(makeBean(suspectUrlVersions));
  }

  /**
   * Creates and provides a new instance with the passed fields.
   * 
   * @param suspectUrlVersions
   *          A Set<SuspectUrlVersion> with the fields to be included.
   * @return an AuSuspectUrlVersions with the newly created object.
   */
  AuSuspectUrlVersions makeBean(Set<SuspectUrlVersion> suspectUrlVersions) {
    if (suspectUrlVersions == null) return this;
    AuSuspectUrlVersions res = new AuSuspectUrlVersions(auid);
    res.suspectVersions = suspectUrlVersions;
    return res;
  }

  /**
   * Provides the SuspectUrlVersions that are present in a serialized JSON
   * string.
   * 
   * @param json
   *          A String with the JSON text.
   * @param app
   *          A LockssApp with the LOCKSS context.
   * @return a Set<SuspectUrlVersion> that was updated from the JSON source.
   * @throws IOException
   *           if any problem occurred during the deserialization.
   */
  public synchronized Set<SuspectUrlVersion> updateFromJson(String json,
      LockssApp app) throws IOException {
    // Deserialize the JSON text into a new, scratch instance.
    AuSuspectUrlVersions srcSuvs = AuUtil.auSuspectUrlVersionsFromJson(json);
    // Note: do NOT merge the fields in the new instance.
    suspectVersions = srcSuvs.suspectVersions;
    postUnmarshal(app);
    return suspectVersions;
  }

  /**
   * Deserializes a JSON string into a new AuSuspectUrlVersions object.
   * 
   * @param key
   *          A String with the Archival Unit identifier.
   * @param json
   *          A String with the JSON text.
   * @param daemon
   *          A LockssDaemon with the LOCKSS daemon.
   * @return a AuSuspectUrlVersions with the newly created object.
   * @throws IOException
   *           if any problem occurred during the deserialization.
   */
  public static AuSuspectUrlVersions fromJson(String key, String json,
					      LockssDaemon daemon)
      throws IOException {
    AuSuspectUrlVersions res = AuSuspectUrlVersions.make(key);
    res.updateFromJson(json, daemon);
    return res;
  }

  /** Update the saved state to reflect the current contents
   */
  public synchronized void storeAuSuspectUrlVersions() {
    getStateMgr().updateAuSuspectUrlVersions(auid, this);
  }

  private StateManager getStateMgr() {
    return LockssDaemon.getManagerByTypeStatic(StateManager.class);
  }

  public int hashCode() {
    return auid.hashCode() + suspectVersions.hashCode();
  }

  public boolean equals(Object obj) {
    if (obj instanceof AuSuspectUrlVersions) {
      AuSuspectUrlVersions asuv = (AuSuspectUrlVersions)obj;
      return StringUtil.equalStrings(auid, asuv.auid)
	&& suspectVersions.equals(asuv.suspectVersions);
    }
    return false;
  }

  /**
   * Avoids duplicating common strings.
   */
  protected void postUnmarshal(LockssApp lockssContext) {
    auid = StringPool.AUIDS.intern(auid);
  }

  public String toString() {
    return "[ASUVs: " + suspectVersions + "]";
  }
}
