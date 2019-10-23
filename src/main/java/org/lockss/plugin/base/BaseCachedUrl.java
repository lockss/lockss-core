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

package org.lockss.plugin.base;

import java.io.*;
import java.util.*;
import java.net.*;
import de.schlichtherle.truezip.file.*;
import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.plugin.definable.*;
import org.lockss.truezip.*;
import org.lockss.repository.*;
import org.lockss.util.*;
import org.lockss.ws.entities.ContentResult;
import org.lockss.ws.entities.LockssWebServicesFault;
import org.lockss.rewriter.*;
import org.lockss.extractor.*;

import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;

/** Base class for CachedUrls.  Expects the LockssRepository for storage.
 * Plugins may extend this to get some common CachedUrl functionality.
 */
public class BaseCachedUrl implements CachedUrl {

  protected ArchivalUnit au;
  // The url of the artifact.  Differs from getUrl() for archive members.
  protected String artifactUrl;
  protected static Logger logger = Logger.getLogger();

  protected Properties options;

  protected LockssRepository v2Repo;
  protected String v2Coll;
  protected Artifact art;
  protected ArtifactData artData;
  protected List<ArtifactData> allArtData = new ArrayList<ArtifactData>(2);
  protected boolean artifactObtained = false;
  protected boolean inputStreamUsed = false;
  protected InputStream restInputStream;
  protected CIProperties restProps;

  // Cached here as might be used several times in quick succession
  // (esp. by archive members).  Don't want to store in AU.
  PatternStringMap urlMimeMap = null;

  public static final String PREFIX = Configuration.PREFIX + "baseCachedUrl.";

  private static final String PARAM_SHOULD_FILTER_HASH_STREAM =
    PREFIX + "filterHashStream";
  private static final boolean DEFAULT_SHOULD_FILTER_HASH_STREAM = true;

  public static final String PARAM_FILTER_USE_CHARSET =
    PREFIX + "filterUseCharset";
  public static final boolean DEFAULT_FILTER_USE_CHARSET = true;

  /** Hide files with URLs that don't match the crawl rules (which may have
   * changed since files were collected) */
  public static final String PARAM_INCLUDED_ONLY = PREFIX + "includedOnly";
  static final boolean DEFAULT_INCLUDED_ONLY = true;

  /** Check raw Content-Type property in addition to X-Lockss-content-type.
   * Disable only for backward compatibility. */
  public static final String PARAM_USE_RAW_CONTENT_TYPE =
    PREFIX + "useRawContentType";
  public static final boolean DEFAULT_USE_RAW_CONTENT_TYPE = true;

  public static final String DEFAULT_METADATA_CONTENT_TYPE = "text/html";

  public BaseCachedUrl(ArchivalUnit owner, String url) {
    final String DEBUG_HEADER = "BaseCachedUrl(): ";
    this.au = owner;
    this.artifactUrl = url;

    RepositoryManager repomgr = getDaemon().getRepositoryManager();
    if (repomgr != null && repomgr.getV2Repository() != null) {
      v2Repo = repomgr.getV2Repository().getRepository();
      v2Coll = repomgr.getV2Repository().getCollection();
    }
    if (logger.isDebug3())
      logger.debug3(DEBUG_HEADER + "v2Repo = " + v2Repo);
  }

  protected BaseCachedUrl(ArchivalUnit owner, String url, Artifact art) {
    final String DEBUG_HEADER = "BaseCachedUrl(): ";
    this.au = owner;
    this.artifactUrl = url;
    this.art = art;
    if (art != null) {
      artifactObtained = true;
    }

    RepositoryManager repomgr = getDaemon().getRepositoryManager();
    if (repomgr != null) {
      v2Repo = repomgr.getV2Repository().getRepository();
      v2Coll = repomgr.getV2Repository().getCollection();
    }
    if (logger.isDebug3())
      logger.debug3(DEBUG_HEADER + "v2Repo = " + v2Repo);
  }

  public String getUrl() {
    return artifactUrl;
  }

  public int getType() {
    return CachedUrlSetNode.TYPE_CACHED_URL;
  }

  public boolean isLeaf() {
    return true;
  }

  /**
   * return a string "[BCU: <url>]"
   * @return the string form
   */
  public String toString() {
    return "[BCU: "+ getUrl() + "]";
  }

  /**
   * Return the ArchivalUnit to which this CachedUrl belongs.
   * @return the ArchivalUnit
   */
  public ArchivalUnit getArchivalUnit() {
    return au;
  }

  public CachedUrl getCuVersion(int version) {
    Artifact verArt = null;
    try {
      verArt =
	v2Repo.getArtifactVersion(v2Coll, au.getAuId(), artifactUrl, version);
    } catch (IOException e) {
      logger.error("Error getting Artifact version: " + artifactUrl, e);
    }
    return new Version(au, artifactUrl, version, verArt);
  }

  public CachedUrl[] getCuVersions() {
    return getCuVersions(1000/*Integer.MAX_VALUE*/);
  }

  public CachedUrl[] getCuVersions(int maxVersions) {
    List<CachedUrl> cuVers = new ArrayList<CachedUrl>();
    try {
      for (Artifact art : v2Repo.getArtifactsAllVersions(v2Coll,
							 au.getAuId(),
							 artifactUrl)) {
	if (art.getCommitted()) {
	  cuVers.add(new Version(au, artifactUrl, art.getVersion(), art));
	  if (cuVers.size() >= maxVersions) {
	    break;
	  }
	}
      }
    } catch (IOException e) {
      logger.error("Couldn't get Artifact version iterator: " + artifactUrl, e);
      return new CachedUrl[0];
    }
    return cuVers.toArray(new CachedUrl[0]);
  }

  public int getVersion() {
    ensureArtifact();
    return art.getVersion();
  }

  /**
   * Return a stream suitable for hashing.  This may be a filtered stream.
   * @return an InputStream
   */
  public InputStream openForHashing() {
    return openForHashing(null);
  }

  /**
   * Return a stream suitable for hashing with a hash of the unfiltered
   * content.
   * @param hasher HashedInputStream.Hasher containing MessageDigest to be
   * updated
   * @return an InputStream
   */
  public InputStream openForHashing(HashedInputStream.Hasher hasher) {
    if (CurrentConfig.getBooleanParam(PARAM_SHOULD_FILTER_HASH_STREAM,
				      DEFAULT_SHOULD_FILTER_HASH_STREAM)) {
      logger.debug3("Filtering on, returning filtered stream");
      return getFilteredStream(hasher);
    } else {
      logger.debug3("Filtering off, returning unfiltered stream");
      return getUncompressedInputStream(hasher);
    }
  }

  public void setOption(String option, String val) {
    if (options == null) {
      options = new Properties();
    }
    options.setProperty(option, val);
  }

  protected String getOption(String option) {
    if (options == null) {
      return null;
    }
    return options.getProperty(option);
  }

  protected boolean isIncludedOnly() {
    String incOpt = getOption(OPTION_INCLUDED_ONLY);
    if ("true".equalsIgnoreCase(incOpt)) {
      return true;
    }
    if ("false".equalsIgnoreCase(incOpt)) {
      return false;
    }
    return CurrentConfig.getBooleanParam(PARAM_INCLUDED_ONLY,
					 DEFAULT_INCLUDED_ONLY);
  }

  public boolean hasContent() {
    final String DEBUG_HEADER = "hasContent(): ";
    ensureArtifact();
    if (logger.isDebug2()) {
      logger.debug2("hasContent = " + (art != null) + ": " + art);
    }
    if (art == null) return false;
    if (isIncludedOnly() && !au.shouldBeCached(getUrl())) {
      logger.debug2("hasContent("+getUrl()+"): excluded by crawl rule");
      return false;
    }
    if (logger.isDebug2()) logger.debug2(DEBUG_HEADER + "return true");
    return true;
  }

  public InputStream getUnfilteredInputStream() {
    ensureArtifactData();
    inputStreamUsed = true;
    restInputStream = artData.getInputStream();
    return restInputStream;
  }

  public InputStream getUnfilteredInputStream(HashedInputStream.Hasher hasher) {
    InputStream is = getUnfilteredInputStream();
    if (hasher != null) {
      is = newHashedInputStream(is, hasher);
    }
    return is;
  }

  /** Return an InputStream on the content.  If a Content-Encoding header
   * is present indicating that the content is compressed, it is
   * decompressed. */
  public InputStream getUncompressedInputStream() {
    return getUncompressedInputStream(null);
  }

  /** Return an InputStream on the content.  If a Content-Encoding header
   * is present indicating that the content is compressed, it is
   * decompressed.  The Content-Encoding and Content-Length headers, and
   * the results of getContentSize(), will continue to reflect the
   * compressed content, not what is returned in this stream. */
  public InputStream getUncompressedInputStream(HashedInputStream.Hasher hasher) {
    InputStream in = getUnfilteredInputStream(hasher);
    String contentEncoding = getProperty(PROPERTY_CONTENT_ENCODING);
    // Daemon versions 1.67 and 1.68 decompressed on receipt but didn't
    // remove the Content-Encoding header.  If decompression fails return
    // the raw stream.
    return StreamUtil.getUncompressedInputStreamOrFallback(in,
							   contentEncoding,
							   getUrl());
  }

  // Clients of CachedUrl expect InputStreams to support mark/reset
  private InputStream newHashedInputStream(InputStream is,
					   HashedInputStream.Hasher hasher) {
    return new BufferedInputStream(new HashedInputStream(is, hasher));
  }

  private String getProperty(String prop) {
    CIProperties props = getProperties();
    if (props != null) {
      return props.getProperty(prop);
    }
    return null;
  }

  public String getContentType() {
    String res = null;
    CIProperties props = getProperties();
    if (props != null) {
      res = props.getProperty(PROPERTY_CONTENT_TYPE);
    }
    if (res == null &&
	CurrentConfig.getBooleanParam(PARAM_USE_RAW_CONTENT_TYPE,
				      DEFAULT_USE_RAW_CONTENT_TYPE)) {
      res = props.getProperty("Content-Type");
    }
    if (res != null) {
      return res;
    }
    return matchUrlMimeMap(getUrl());
  }

  PatternStringMap getUrlMimeTypeMap() {
    if (urlMimeMap == null) {
      urlMimeMap = au.makeUrlMimeTypeMap();
    }
    return urlMimeMap;
  }

  String matchUrlMimeMap(String url) {
    PatternStringMap map = getUrlMimeTypeMap();;
    String mime = map.getMatch(url);
    if (mime != null) {
      logger.debug("Inferred mime type: " + mime + " for " + url);
      return mime;
    }
    return null;
  }

  public String getEncoding() {
    String res = null;
    if (CurrentConfig.getBooleanParam(PARAM_FILTER_USE_CHARSET,
				      DEFAULT_FILTER_USE_CHARSET)) {
      res = HeaderUtil.getCharsetFromContentType(getContentType());
    }
    if (res == null) {
      res = Constants.DEFAULT_ENCODING;
    }
    return res;
  }

  public Reader openForReading() {
    try {
      return CharsetUtil.getReader(this);
    } catch (IOException e) {
      logger.error("Creating InputStreamReader for '" + getUrl() + "'", e);
      throw new LockssUncheckedIOException(e);
    }
  }

  public LinkRewriterFactory getLinkRewriterFactory() {
    LinkRewriterFactory ret = null;
    String ctype = getContentType();
    if (ctype != null) {
      ret = au.getLinkRewriterFactory(ctype);
    }
    return ret;
  }

  public CIProperties getProperties() {
    if (restProps == null) {
      ensureArtifactData();
      restProps = V2RepoUtil.propsFromHttpHeaders(artData.getMetadata());
      String chk = artData.getContentDigest();
      // tk - hash alg shouldn't be hardwired
      if (!StringUtil.isNullString(chk)) {
	restProps.put(PROPERTY_CHECKSUM, chk);
      }
      if (logger.isDebug3()) {
	logger.debug2("getProperties: " + artifactUrl + ": " + restProps);
      }
    }
    return restProps;
  }

  /**
   * Add to the properties attached to the url in the cache, if any.
   * Requires {@link #release()}
   * @param key
   * @param value
   * Throws IllegalOperationException if either the key is not on
   * the list of keys it is permitted to add, or if the properties
   * already contains the key.
   */
  public void addProperty(String key, String value) {
    throw new UnsupportedOperationException("addProperty no longer supporte");
  }

  public long getContentSize() {
    if (hasContent()) {
      return art.getContentLength();
    } else {
      throw new UnsupportedOperationException("No content: " + artifactUrl);
    }
  }

  /**
   * Return a FileMetadataExtractor for the CachedUrl's content type, or
   * null if the plugin has no FileMetadataExtractor for that MIME type
   * @param target the purpose for which metadata is being extracted
   */
  public FileMetadataExtractor getFileMetadataExtractor(MetadataTarget target) {
    String ct = getContentType();
    FileMetadataExtractor ret = au.getFileMetadataExtractor(target, ct);
    return ret;
  }

  // overridable for testing
  void releaseArtifactData(ArtifactData ad) {
    ad.release();
  }

  public void release() {
    for (ArtifactData ad : allArtData) {
      releaseArtifactData(ad);
    }
    allArtData.clear();
    artData = null;
    restInputStream = null;
  }

  private LockssDaemon getDaemon() {
//     if (au.getPlugin() != null) {
//       return au.getPlugin().getDaemon();
//     }
    return LockssDaemon.getLockssDaemon();
  }

  // overridable for testing
  protected Artifact getArtifact() throws IOException {
    // Note artifactUrl not getUrl(); always want actual Artifact URL, not
    // archive member
    return v2Repo.getArtifact(v2Coll, au.getAuId(), artifactUrl);
  }

  private void ensureArtifact() {
    if (!artifactObtained) {
      try {
	art = getArtifact();
	if (logger.isDebug3()) {
	  logger.debug3("Got art: " + art);
	}
      } catch (IOException e) {
	throw new LockssUncheckedIOException(e);
      }
    }
    artifactObtained = true;
  }

  ArtifactData getArtifactData(LockssRepository repo, Artifact art)
      throws IOException {
    return repo.getArtifactData(art);
  }

  private void ensureArtifactData() {
    if (hasContent()) {
      if (inputStreamUsed || artData == null) {
	try {
	  artData = getArtifactData(v2Repo, art);
	  allArtData.add(artData);
	} catch (IOException e) {
	  throw new LockssUncheckedIOException(e);
	}
	inputStreamUsed = false;
      }
    } else {
      throw new UnsupportedOperationException("No content: " + artifactUrl);
    }
  }

  protected InputStream getFilteredStream() {
    return getFilteredStream(null);
  }

  protected InputStream getFilteredStream(HashedInputStream.Hasher hasher) {
    String contentType = getContentType();
    // first look for a FilterFactory
    FilterFactory fact = au.getHashFilterFactory(contentType);
    if (fact != null) {
      if (logger.isDebug3()) {
	logger.debug3("Filtering " + contentType +
		      " with " + fact.getClass().getName());
      }
      InputStream unfis = getUncompressedInputStream(hasher);
      try {
	return fact.createFilteredInputStream(au, unfis, getEncoding());
      } catch (PluginException e) {
	IOUtil.safeClose(unfis);
	throw new LockssUncheckedPluginException(e);
      } catch (RuntimeException e) {
	IOUtil.safeClose(unfis);
	throw e;
      }
    }
    // then look for deprecated FilterRule
    FilterRule fr = au.getFilterRule(contentType);
    if (fr != null) {
      if (logger.isDebug3()) {
	logger.debug3("Filtering " + contentType +
		      " with " + fr.getClass().getName());
      }
      Reader unfrdr = openForReading();
      try {
	Reader rd = fr.createFilteredReader(unfrdr);
	return new ReaderInputStream(rd);
      } catch (PluginException e) {
	IOUtil.safeClose(unfrdr);
        throw new LockssUncheckedPluginException(e);
      }
    }
    if (logger.isDebug3()) logger.debug3("Not filtering " + contentType);
    InputStream ret = getUncompressedInputStream();
    if (hasher != null) {
      ret = newHashedInputStream(ret, hasher);
    }
    return ret;
  }

  public CachedUrl getArchiveMemberCu(ArchiveMemberSpec ams) {
    Member memb = new Member(au, artifactUrl, this, ams);
    return memb;
  }

  CachedUrl getArchiveMemberCu(ArchiveMemberSpec ams, TFile memberTf) {
    Member memb = new Member(au, artifactUrl, this, ams, memberTf);
    return memb;
  }

  @Override
  public boolean isArchiveMember() {
    return false;
  }

  /** A CachedUrl that's bound to a specific version. */
  static class Version extends BaseCachedUrl {
    protected int specVersion = -1;     // explicitly specified verson
					// (used if no Artifact)

    public Version(ArchivalUnit owner, String url, int vernum, Artifact art) {
      super(owner, url, art);
      specVersion = vernum;
    }

    protected Artifact getArtifact() throws IOException {
      return v2Repo.getArtifactVersion(v2Coll, au.getAuId(),
				       artifactUrl, specVersion);
    }

    public int getVersion() {
      if (specVersion < 0) {
	return super.getVersion();
      } else {
	return specVersion;
      }
    }

    public boolean hasContent() {
      if (!super.hasContent()) {
	return false;
      }
      if (isIncludedOnly() && !au.shouldBeCached(getUrl())) {
	logger.debug2("hasContent("+getUrl()+"): excluded by crawl rule");
	return false;
      }
      return true;
    }

    /**
     * return a string "[BCU: v=n <url>]"
     * @return the string form
     */
    public String toString() {
      int ver;
      try {
	ver = getVersion();
      } catch (RuntimeException e) {
	ver = -1;
      }
      return "[BCU: v=" + ver + " " + artifactUrl+"]";
    }
  }

  /** Special behavior for CUs that are archive members.  This isn't
   * logically a subtype of CachedUrl because not all places that accept a
   * CachedUrl can operate an archive member, but it's the convenient way
   * to implement it.  Perhaps it should be a supertype (interface)? */
  static class Member extends BaseCachedUrl {
    protected BaseCachedUrl bcu;
    protected ArchiveMemberSpec ams;
    protected TFileCache.Entry tfcEntry = null;
    protected TFile memberTf = null;
    protected CIProperties memberProps = null;

    Member(ArchivalUnit au, String url, BaseCachedUrl bcu,
	   ArchiveMemberSpec ams) {
      super(au, url);
      this.ams = ams;
      this.bcu = bcu;
    }

    Member(ArchivalUnit au, String url, BaseCachedUrl bcu,
	   ArchiveMemberSpec ams, TFile memberTf) {
      super(au, url);
      this.ams = ams;
      this.bcu = bcu;
      this.memberTf = memberTf;
    }

    @Override
    public String getUrl() {
      return ams.toUrl();
    }

    @Override
    /** True if the archive exists and the member exists */
    public boolean hasContent() {
      if (!super.hasContent()) {
	logger.debug3("No super content: " + this);
	return false;
      }
      try {
	TFile tf = getTFile();
	if (tf == null) {
	  logger.debug2("No tfile: " + this);
	  return false;
	}
	if (!tf.isDirectory()) {
	  logger.debug2("tfile not dir: " + this);
	  return false;
	}
	return getMemberTFile().exists();
      } catch (IOException e) {
	logger.error("Couldn't open member for which exists() was true: " + this,
		     e);
	throw new LockssUncheckedIOException(e);
      }
    }

    @Override
    public InputStream getUnfilteredInputStream() {
      if (!super.hasContent()) {
	return null;
      }
      try {
	TFile tf = getTFile();
	if (tf == null) {
	  return null;
	}
	if (!tf.isDirectory()) {
	  logger.error("tf.isDirectory() = false");
	  return null;
	}
	TFile membtf = getMemberTFile();
	if (!membtf.exists()) {
	  return null;
	}
	InputStream is = new TFileInputStream(membtf);
	if (CurrentConfig.getBooleanParam(LockssApp.PARAM_MONITOR_INPUT_STREAMS,
					  LockssApp.DEFAULT_MONITOR_INPUT_STREAMS)) {
	  is = new MonitoringInputStream(is, this.toString());
	}
	return is;
      } catch (IOException e) {
	logger.error("Couldn't open member for which exists() was true: " + this,
		     e);
	throw new LockssUncheckedIOException(e);
      }
    }

    /** Properties of an archive member are synthesized from its size and
     * extension, and the enclosing archive's collection properties
     * (collection date, Last-Modified) */
    @Override
    public CIProperties getProperties() {
      if (memberProps == null) {
	memberProps = synthesizeProperties();
      }
      return memberProps;
    }

    private CIProperties synthesizeProperties() {
      CIProperties res = new CIProperties();
      try {
	TFileCache.Entry ent = getTFileCacheEntry();
	if (ent.getArcCuProps() != null) {
	  res.putAll(ent.getArcCuProps());
	}
      } catch (IOException e) {
	logger.warning("Couldn't copy archive props to member CU", e);
      }

      res.put(CachedUrl.PROPERTY_NODE_URL, getUrl());
      res.put("Length", getContentSize());

      try {
	// If member has last modified, overwrite any inherited from archive
	// props.
	TFile membtf = getMemberTFile();
	long lastMod = membtf.lastModified();
	if (lastMod > 0) {
	  res.put(CachedUrl.PROPERTY_LAST_MODIFIED,
		  DateTimeUtil.GMT_DATE_FORMATTER.format(new Date(lastMod)));
	}
      } catch (IOException e) {
	logger.warning("Couldn't get member Last-Modified", e);
      }

      String ctype = inferContentType();
      if (!StringUtil.isNullString(ctype)) {
	res.put("Content-Type", ctype);
	res.put(PROPERTY_CONTENT_TYPE, ctype);

      }
      return res;
    }

    private String inferContentType() {
      String ext = FileUtil.getExtension(ams.getName());
      if (ext == null) {
	return null;
      }
      return MimeUtil.getMimeTypeFromExtension(ext);
    }

    @Override
    public long getContentSize() {
      try {
	return getMemberTFile().length();
      } catch (IOException e) {
	logger.error("Couldn't get archive member length: " + this, e);
	throw new LockssUncheckedIOException(e);
      }
    }


    // XXX Should release do something other than release the archive CU?
//     public void release() {
//       if (rnc != null) {
// 	rnc.release();
//       }
//     }

    private TFile getMemberTFile() throws IOException {
      checkValidTfcEntry();
      if (memberTf == null) {
	memberTf = new TFile(getTFile(), ams.getName());
	logger.debug("getMemberTFile: " + memberTf);
      }
      return memberTf;
    }

    private TFile getTFile() throws IOException {
      TFileCache.Entry ent = getTFileCacheEntry();
      if (ent == null) {
	return null;
      }
      return ent.getTFile();
    }

    void checkValidTfcEntry() {
      if (tfcEntry != null && !tfcEntry.isValid()) {
	tfcEntry = null;
      }
    }

    private TFileCache.Entry getTFileCacheEntry() throws IOException {
      checkValidTfcEntry();
      if (tfcEntry == null) {
	TrueZipManager tzm = bcu.getDaemon().getTrueZipManager();
	tfcEntry = tzm.getCachedTFileEntry(au.makeCachedUrl(artifactUrl));
      }
      return tfcEntry;
    }

    ArchiveMemberSpec getArchiveMemberSpec() {
      return ams;
    }

    @Override
    public boolean isArchiveMember() {
      return true;
    }

    String getArchiveUrl() {
      return super.getUrl();
    }

    @Override
    public CachedUrl getArchiveMemberCu(ArchiveMemberSpec ams) {
      throw new UnsupportedOperationException("Can't create a CU member from a CU member: "
					      + this);
    }

    @Override
    public CachedUrl getCuVersion(int version) {
      throw new UnsupportedOperationException("Can't access versions of a CU member: "
					      + this);
    }

    @Override
    public CachedUrl[] getCuVersions(int maxVersions) {
      throw new UnsupportedOperationException("Can't access versions of a CU member: "
					      + this);
    }

    public String toString() {
      return "[BCUM: "+ getUrl() + "]";
    }

  }
}
