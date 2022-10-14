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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.lang3.tuple.*;
import org.apache.http.*;
import org.apache.http.message.*;
import org.springframework.http.HttpHeaders;

import org.lockss.app.*;
import org.lockss.state.*;
import org.lockss.alert.*;
import org.lockss.config.*;
import org.lockss.crawler.*;
import org.lockss.plugin.*;
import org.lockss.repository.*;
import org.lockss.util.*;
import org.lockss.util.StreamUtil.IgnoreCloseInputStream;
import org.lockss.util.urlconn.*;
import org.lockss.util.io.*;
import org.lockss.daemon.*;

import org.lockss.rewriter.*;
import org.lockss.extractor.*;

import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;

/**
 * Basic, fully functional UrlCacher.  Utilizes the LockssRepository for
 * caching, and {@link LockssUrlConnection}s for fetching.  Plugins may
 * extend this to achieve, <i>eg</i>, specialized host connection or
 * authentication.  The redirection semantics offered here must be
 * preserved.
 */
public class DefaultUrlCacher implements UrlCacher {
  protected static Logger logger = Logger.getLogger();

  /** The algorithm to use for content checksum calculation. 
   * An empty value disables checksums 
   */
  public static final String PARAM_CHECKSUM_ALGORITHM =
		    Configuration.PREFIX + "baseuc.checksumAlgorithm";
  public static final String DEFAULT_CHECKSUM_ALGORITHM = null;
  
  protected final ArchivalUnit au;
  protected final String origUrl;   // URL with which I was created
  protected String fetchUrl;		// possibly affected by redirects
  private List<String> redirectUrls;
  private final CacheResultMap resultMap;
  private LockssWatchdog wdog;
  private BitSet fetchFlags = new BitSet();
  private InputStream input;
  private CIProperties headers;
  private boolean markLastContentChanged = true;
  private boolean alreadyHasContent;
  private LockssRepository v2Repo;
  private String v2Ns;
  private Crawler.CrawlerFacade facade;
  
  /**
   * Uncached url object and Archival Unit owner 
   * 
   * @param owner
   * @param uUrl
   */
  public DefaultUrlCacher(ArchivalUnit owner, UrlData ud) {
    if(ud.headers == null) {
      throw new NullPointerException(
          "Unable to store content with null headers");
    }
    origUrl = ud.url;
    headers = ud.headers;
    input = ud.input;
    au = owner;

    RepositoryManager repomgr =
      LockssDaemon.getLockssDaemon().getRepositoryManager();
    if (repomgr != null && repomgr.getV2Repository() != null) {
      v2Repo = repomgr.getV2Repository().getRepository();
      v2Ns = repomgr.getV2Repository().getNamespace();
    }
    Plugin plugin = au.getPlugin();
    resultMap = plugin.getCacheResultMap();
  }

  public void setCrawlerFacade(Crawler.CrawlerFacade facade) {
    this.facade = facade;
  }

  /**
   * Returns the original URL (the one the UrlCacher was created with),
   * independent of any redirects followed.
   * @return the url string
   */
  public String getUrl() {
    return origUrl;
  }

  /**
   * Return the URL that returned content
   */
  String getFetchUrl() {
    return fetchUrl != null ? fetchUrl : origUrl;
  }

  /**
   * Return the ArchivalUnit to which this UrlCacher belongs.
   * @return the owner ArchivalUnit
   */
  public ArchivalUnit getArchivalUnit() {
    return au;
  }

  /**
   * Return a CachedUrl for the content stored.  May be
   * called only after the content is completely written.
   * @return CachedUrl for the content stored.
   */
  public CachedUrl getCachedUrl() {
    return au.makeCachedUrl(getUrl());
  }

  public void setFetchFlags(BitSet fetchFlags) {
    this.fetchFlags = fetchFlags;
  }

  public BitSet getFetchFlags() {
    return fetchFlags;
  }

  public void setWatchdog(LockssWatchdog wdog) {
    this.wdog = wdog;
  }
  
  public LockssWatchdog getWatchdog() {
    return wdog;
  }
  
  public void setRedirectUrls(List<String> redirectUrls) {
    this.redirectUrls = redirectUrls;
    if(fetchUrl == null) {
      this.fetchUrl = redirectUrls.get(redirectUrls.size()-1);
    }
  }
  
  public void setFetchUrl(String fetchUrl) {
    this.fetchUrl = fetchUrl;
  }

  public void storeContent() throws IOException {
    storeContent(input, headers);
  }
  /** Store into the repository the content and headers from a successful
   * fetch.  If redirects were followed and
   * REDIRECT_OPTION_STORE_ALL was specified, store the content and
   * headers under each name in the chain of redirections.
   */
  private void storeContent(InputStream input, CIProperties headers)
      throws IOException {
    if(input != null) {
      Collection<String> startUrls = au.getStartUrls();
      if(startUrls != null && !startUrls.isEmpty() 
          && startUrls.contains(origUrl)) {
        markLastContentChanged = false;
      }
      if (logger.isDebug2()) logger.debug2("Storing url '"+ origUrl +"'");
      storeContentIn(origUrl, input, headers, true, redirectUrls);
      if (infoException != null &&
	  infoException.isAttributeSet(CacheException.ATTRIBUTE_NO_STORE)) {
        logger.debug3("Validator said no store, short-circuiting storeContent");
	return;
      }
      if (logger.isDebug3()) {
        logger.debug3("redirectUrls: " + redirectUrls);
      }
      if (redirectUrls != null && fetchUrl != null) {
        CachedUrl cu = getCachedUrl();
        CIProperties headerCopy  = CIProperties.fromProperties(headers);
        int last = redirectUrls.size() - 1;
        for (int ix = 0; ix <= last; ix++) {
          String name = redirectUrls.get(ix);
          if (logger.isDebug2())
            logger.debug2("Storing in redirected-to url: " + name);
          InputStream is;
	  try {
	    is = cu.getUnfilteredInputStream();
	  } catch (LockssUncheckedException e) {
	    throw resultMap.getRepositoryException(e.getCause());
	  }
          try {
            if (ix < last) {
              // this one was redirected, set its redirected-to prop to the
              // next in the list.
              headerCopy.setProperty(CachedUrl.PROPERTY_REDIRECTED_TO,
                  redirectUrls.get(ix + 1));
            } else if (!name.equals(fetchUrl)) {
              // Last in list.  If not same as fetchUrl, means the final
              // redirection was a directory(slash) redirection, which we don't
              // store as a different name or put on redirectUrls.  Indicate the
              // redirection to the slashed version.  The proxy must be aware
              // of this.  (It can't rely on this property being present,
              // becuase foo/ might later be fetched, not due to a redirect
              // from foo.)
              headerCopy.setProperty(CachedUrl.PROPERTY_REDIRECTED_TO, fetchUrl);
            } else {
              // This is the name that finally got fetched, don't store
              // redirect prop or content-url
              headerCopy.remove(CachedUrl.PROPERTY_REDIRECTED_TO);
              headerCopy.remove(CachedUrl.PROPERTY_CONTENT_URL);
            }
            storeContentIn(name, is, headerCopy, false, null);
          } finally {
            IOUtil.safeClose(is);
          }
        }
      }
    } else {
      logger.warning("Skipped storing a null input stream for " + origUrl);
    }
  }

  private boolean isCurrentVersionSuspect() {
    // Inefficient to call AuSuspectUrlVersions.isSuspect(url, version)
    // here as would have to find version number for each URL, which
    // require disk access.  This loop first filters on URL so finds
    // version number only when necessary.  Also, in some tests
    // getCachedUrl() will get NPE on other URLs due to MockArchivalUnit
    // not having been set up with a corresponding MockCachedUrl.

    Collection <AuSuspectUrlVersions.SuspectUrlVersion> suspects =
      AuUtil.getSuspectUrlVersions(au).getSuspectList();
    if (logger.isDebug2()) {
      logger.debug2("Checking for current suspect version: " + getUrl());
    }
    int curVer = -1;
    for (AuSuspectUrlVersions.SuspectUrlVersion suv : suspects ) {
      if (suv.getUrl().equals(getUrl())) {
	if (curVer == -1) {
	  curVer = getCachedUrl().getVersion();
	}
	if (suv.getVersion() == curVer) {
	  if (logger.isDebug3()) {
	    logger.debug3("Found suspect current version " +
			  curVer + ": " + getUrl());
	  }
	  return true;
	} else {
	  if (logger.isDebug3()) {
	    logger.debug3("Found suspect non-current version " +
			  suv.getVersion() + " != " + curVer + ": " + getUrl());
	  }
	}
      }
    }
    return false;
  }

  protected void storeContentIn(String url, InputStream input,
				CIProperties headers,
				boolean doValidate, List<String> redirUrls)
      throws IOException {
    storeContentInV2(url, input, headers, doValidate, redirUrls);
  }

  protected void storeContentInV2(String url, InputStream input,
				CIProperties headers,
				boolean doValidate, List<String> redirUrls)
      throws IOException {
    InputStream in = input;
    boolean currentWasSuspect = isCurrentVersionSuspect();
    Artifact uncommittedArt = null;
    try {
      alreadyHasContent =
	v2Repo.getArtifact(v2Ns, au.getAuId(), url) != null;
    } catch (IOException ex) {
      logger.warning("Repository error checking for existing content: " + url,
		     ex);
    }
    try {
      MessageDigest checksumProducer = null;
      String checksumAlgorithm =
	CurrentConfig.getParam(PARAM_CHECKSUM_ALGORITHM,
			       DEFAULT_CHECKSUM_ALGORITHM);
      if (!StringUtil.isNullString(checksumAlgorithm)) {
	try {
	  checksumProducer = MessageDigest.getInstance(checksumAlgorithm);
	  HashedInputStream.Hasher hasher =
	    new HashedInputStream.Hasher(checksumProducer);
	  in = new BufferedInputStream(new HashedInputStream(in, hasher));
	} catch (NoSuchAlgorithmException ex) {
	  logger.warning(String.format("Checksum algorithm %s not found, "
				       + "checksumming disabled", checksumAlgorithm));
	}
      }
      // TK shouldn't supply version number
      ArtifactIdentifier id = new ArtifactIdentifier(v2Ns, au.getAuId(),
						     url, null);

      headers.setProperty(CachedUrl.PROPERTY_NODE_URL, url);
      HttpHeaders metadata = V2RepoUtil.httpHeadersFromProps(headers);

      // tk
      BasicStatusLine statusLine =
	new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

      InputStream adin =
	new ExceptionWrappingInputStream(new IgnoreCloseInputStream(in));
      ArtifactData ad = new ArtifactData(id, metadata, adin, statusLine);
      if (logger.isDebug2()) {
        logger.debug2("Creating artifact: " + ad);
      }
      uncommittedArt = addArtifact(ad);
      long bytes = uncommittedArt.getContentLength();
      if (logger.isDebug2()) {
        logger.debug2("Stored " + bytes + " bytes: " + uncommittedArt);
      }
      if (!fetchFlags.get(DONT_CLOSE_INPUT_STREAM_FLAG)) {
        try {
          input.close();
	  IOUtil.safeClose(in);
        } catch (IOException ex) {
          CacheException closeEx =
            resultMap.mapException(au, fetchUrl, ex, null);
          if (!(closeEx instanceof CacheException.IgnoreCloseException)) {
            throw new InputIOException(ex);
          }
        }
      }
      boolean doStore = true;
      if (doValidate && !fetchFlags.get(SUPPRESS_CONTENT_VALIDATION)) {
	// Don't modify passed-in headers
	headers = CIProperties.fromProperties(headers);
	if (redirUrls != null && !redirUrls.isEmpty()) {
	  headers.put(CachedUrl.PROPERTY_VALIDATOR_REDIRECT_URLS, redirUrls);
	}
	CacheException vExp = validate(headers, uncommittedArt, bytes);
	headers.remove(CachedUrl.PROPERTY_VALIDATOR_REDIRECT_URLS);
	if (vExp != null) {
	  if (vExp.isAttributeSet(CacheException.ATTRIBUTE_FAIL) ||
	      vExp.isAttributeSet(CacheException.ATTRIBUTE_FATAL)) {
	    abandonNewVersion(uncommittedArt);
	    uncommittedArt = null;
	    throw vExp;
	  } else if (vExp.isAttributeSet(CacheException.ATTRIBUTE_NO_STORE)) {
	    abandonNewVersion(uncommittedArt);
	    uncommittedArt = null;
	    infoException = vExp;
	    doStore = false;
	  } else {
	    infoException = vExp;
	  }
	}
      }
      if (doStore && isIdenticalToPreviousVersion(uncommittedArt)) {
	abandonNewVersion(uncommittedArt);
	uncommittedArt = null;
	doStore = false;
	if (facade != null) {
	  CrawlerStatus status = facade.getCrawlerStatus();
	  if (status != null) {
	    status.signalUrlUnchanged(fetchUrl);
	  }
	}
      }
      if (doStore) {
	if (checksumProducer != null) {
	  byte bdigest[] = checksumProducer.digest();
	  String sdigest = ByteArray.toHexString(bdigest);
	  headers.setProperty(CachedUrl.PROPERTY_CHECKSUM,
			      String.format("%s:%s",
					    checksumAlgorithm, sdigest));
	}
	if (logger.isDebug2()) {
	  logger.debug2("Committing " + uncommittedArt);
	}
	Artifact committedArt = v2Repo.commitArtifact(uncommittedArt);
	if (logger.isDebug2()) {
	  logger.debug2("Committed v " + committedArt.getVersion()
			+ " of " + committedArt);
	}

	AuState aus = AuUtil.getAuState(au);
	if (aus != null && currentWasSuspect) {
	  aus.incrementNumCurrentSuspectVersions(-1);
	}
	if (aus != null && markLastContentChanged) {
	  aus.contentChanged();
	}
	// TK
	if (alreadyHasContent /*&& !isIdenticalToPreviousVersion(committedArt)*/) {
	  Alert alert = Alert.auAlert(Alert.NEW_FILE_VERSION, au);
	  alert.setAttribute(Alert.ATTR_URL, getFetchUrl());
	  String msg = "Collected an additional version: " + getFetchUrl();
	  alert.setAttribute(Alert.ATTR_TEXT, msg);
	  raiseAlert(alert);
	}
      }
    } catch (InputIOException ex) {
      // error reading from input stream
      abandonNewVersion(uncommittedArt);
      throw resultMap.mapException(au, url, ex.getIOCause(), null);
    } catch (CacheException ex) {
      // XXX some code below here maps the exception
      abandonNewVersion(uncommittedArt);
      throw ex;
    } catch (IOException ex) {
      // any other error is theoretically a repository error
      logger.error("Can't store artifact: repository error", ex);
      abandonNewVersion(uncommittedArt);
      throw resultMap.getRepositoryException(ex);
    }
  }

  // Overridable for testing
  protected Artifact addArtifact(ArtifactData ad) throws IOException {
    return v2Repo.addArtifact(ad);
  }

  protected boolean isIdenticalToPreviousVersion(Artifact art)
      throws IOException {
    if (art.getCommitted()) {
      throw new IllegalStateException("Can't perform identical check after artifact if committed");
    }
    int ver = art.getVersion();
    if (ver < 2) return false;
    String artHash = art.getContentDigest();
    if (artHash == null) return false;
    // Fetch the latest committed version, if any
    Artifact prev = v2Repo.getArtifact(v2Ns, au.getAuId(), art.getUri());
    if (prev == null) return false;
    if (art.getId().equals(prev.getId())) {
      logger.error("Uncommitted artifact has same ID as supposedly committed most recent version: " + art);
      // throw?
      return false;
    }
    boolean res = artHash.equals(prev.getContentDigest());
    if (res) logger.debug2("New version identical to old: " + art.getUri());
    return res;
  }

  void abandonNewVersion(Artifact art) {
    if (art != null) {
      try {
	v2Repo.deleteArtifact(art);
      } catch (Exception e) {
	logger.error("Error deleting uncommitted artifact: " + art, e);
      }
    }
  }


  /**
   * Overrides normal <code>toString()</code> to return a string like
   * "BUC: <url>"
   * @return the class-url string
   */
  public String toString() {
    return "[BUC: " + getUrl() + "]";
  }

  //  Beginnings of validation framework.
  protected CacheException infoException;

  public CacheException getInfoException() {
    return infoException;
  }

  protected CacheException validate(CIProperties headers,
				    Artifact art,
				    long size)
      throws CacheException {
    if (false) return null;
    LinkedList<Pair<String,Exception>> validationFailures =
      new LinkedList<Pair<String,Exception>>();

    // First check actual length = Content-Length header if any
    long contLen = getContentLength();
    if (contLen >= 0 && contLen != size) {
      Alert alert = Alert.auAlert(Alert.FILE_VERIFICATION, au);
      alert.setAttribute(Alert.ATTR_URL, getFetchUrl());
      String msg = "File size (" + size +
	") differs from Content-Length header (" + contLen + "): "
	+ getFetchUrl();
      alert.setAttribute(Alert.ATTR_TEXT, msg);
      raiseAlert(alert);
      validationFailures.add(new ImmutablePair(getUrl(),
				      new ContentValidationException.WrongLength(msg)));
    }

    // 2nd, empty file
    if (size == 0) {
      Exception ex =
	new ContentValidationException.EmptyFile("Empty file stored");
      validationFailures.add(new ImmutablePair(getUrl(), ex));
    }

    // 3rd plugin-supplied ContentValidator.  Any
    // ContentValidationException it throws will take precedence over the
    // previous (wrong length, empty), but an unexpected exception will not
    // take precedence.
    String contentType = getContentType();
    ContentValidatorFactory cvfact = au.getContentValidatorFactory(contentType);
    if (cvfact != null) {
      ContentValidator cv = cvfact.createContentValidator(au, contentType);
      if (cv != null) {
	CachedUrl cu = getTempCachedUrl(headers, size, art);
	try {
	  cv.validate(cu);
	} catch (ContentValidationException e) {
	  logger.debug2("Validation error1", e);
	  // Plugin-triggered ContentValidationException goes first
	  validationFailures.addFirst(new ImmutablePair(getUrl(), e));
	} catch (Exception e) {
	  logger.debug2("Validation error2", e);
	  // Unexpected error in validator goes first
	  validationFailures.addFirst(new ImmutablePair(getUrl(),
							new ContentValidationException.ValidatorExeception(e)));
	} finally {
	  cu.release();
	}
      }
    }
    return firstMappedException(validationFailures);
  }

  CachedUrl getTempCachedUrl(final CIProperties headers, long size,
			     Artifact art) {
    return new BaseCachedUrl(au, art.getUri(), art) {
      @Override
      public CIProperties getProperties() {
	return headers;
      }
    };
  }

  CacheException firstMappedException(List<Pair<String,Exception>> exps) {
    if (logger.isDebug3()) {
      logger.debug3("firstMappedException: " + exps);
    }
    if (exps.isEmpty()) {
      return null;
    }
    for (Pair<String,Exception> p : exps) {
      CacheException mapped = resultMap.mapException(au,
						     p.getLeft(),
						     p.getRight(),
						     null);
      if (mapped != null &&
	  ! (mapped instanceof CacheException.UnknownExceptionException)) {
	return mapped;
      }
    }
    Pair<String,Exception> first = exps.get(0);
    return resultMap.mapException(au, first.getLeft(), first.getRight(), null);
  }


  private void raiseAlert(Alert alert) {
    try {
      au.getPlugin().getDaemon().getAlertManager().raiseAlert(alert);
    } catch (RuntimeException e) {
      logger.error("Couldn't raise alert", e);
    }
  }

  private long getContentLength() {
    try {
      return Long.parseLong(headers.getProperty("content-length"));
    } catch (Exception e) {
      return -1;
    }
  }  

  private String getContentType() {
    return headers.getProperty(CachedUrl.PROPERTY_CONTENT_TYPE);
  }  

}
