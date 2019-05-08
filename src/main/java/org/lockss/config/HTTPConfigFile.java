/*

Copyright (c) 2001-2018 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.config;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.TeeInputStream;
import org.lockss.util.*;
import org.lockss.util.urlconn.*;
import org.springframework.http.MediaType;
import org.lockss.hasher.*;
import org.apache.oro.text.regex.*;

/**
 * A ConfigFile loaded from a URL.
 *
 */
public class HTTPConfigFile extends BaseConfigFile {

  public static final String PREFIX = Configuration.PREFIX + "config.";

  // Connect and data timeouts
  /** Amount of time the daemon will wait for the property server to open
   * a connection. */
  public static final String PARAM_CONNECT_TIMEOUT = PREFIX+ "timeout.connect";
  /** Amount of time the daemon will wait to receive data on an open
   * connection to the property server. */
  public static final long DEFAULT_CONNECT_TIMEOUT = 1 * Constants.MINUTE;

  public static final String PARAM_DATA_TIMEOUT = PREFIX + "timeout.data";
  public static final long DEFAULT_DATA_TIMEOUT = 10 * Constants.MINUTE;

  public static final String PARAM_CHARSET_UTIL = PREFIX + "charset.util";
  public static final boolean DEFAULT_CHARSET_UTIL = true;

  private String m_httpLastModifiedString = null;

  // The maximum size in bytes for the contents of a response to be kept in
  // memory.
  private static final int RESPONSE_MEMORY_THRESHOLD = 64 * 1024;

  // The Etag received in the last HTTP response.
  private String responseHttpEtag = null;

  private LockssUrlConnectionPool m_connPool;
  private boolean checkAuth = false;
  private MessageDigest chkDig;
  private String chkAlg;
  private String proxyUsed;

  public HTTPConfigFile(String url, ConfigManager cfgMgr) {
    super(url, cfgMgr);
  }

  public void setConnectionPool(LockssUrlConnectionPool connPool) {
    m_connPool = connPool;
  }

  LockssUrlConnectionPool getConnectionPool() {
    if (m_connPool == null) {
      m_connPool = new LockssUrlConnectionPool();
    }
    return m_connPool;
  }

  // overridden for testing
  protected LockssUrlConnection openUrlConnection(String url)
      throws IOException {
    Configuration conf = ConfigManager.getCurrentConfig();

    LockssUrlConnectionPool connPool = getConnectionPool();

    connPool.setConnectTimeout(conf.getTimeInterval(PARAM_CONNECT_TIMEOUT,
						    DEFAULT_CONNECT_TIMEOUT));
    connPool.setDataTimeout(conf.getTimeInterval(PARAM_DATA_TIMEOUT,
						 DEFAULT_DATA_TIMEOUT));
    LockssUrlConnection conn = UrlUtil.openConnection(url, connPool);
    if (m_cfgMgr != null) {

      // Set up local HTTP cache.  Some code paths will add a no-cache
      // header to prevent the request from being served from the cache.
      // All responses, including those in response to a no-cache request,
      // are eligible to be stored in the cache.
      conn.setClientCache(m_cfgMgr.getHttpCacheManager()
			  .getCacheSpec(ConfigManager.HTTP_CACHE_NAME));

      LockssSecureSocketFactory fact = m_cfgMgr.getSecureSocketFactory();
      if (fact != null) {
	checkAuth = true;
	conn.setSecureSocketFactory(fact);
      }
    }
    return conn;
  }

  /** Don't check for new file on every load, only when asked.
   */
  protected boolean isCheckEachTime() {
    return false;
  }

  /**
   * Given a URL, open an input stream, handling the appropriate
   * if-modified-since behavior.
   */
  private InputStream getUrlInputStream(LockssUrlConnection conn)
      throws IOException, MalformedURLException {
    try {
      return getUrlInputStream0(conn);
    } catch (javax.net.ssl.SSLHandshakeException e) {
      m_loadError = "Could not authenticate server: " + e.getMessage();
      throw new IOException(m_loadError, e);
    } catch (javax.net.ssl.SSLKeyException e) {
      m_loadError = "Could not authenticate; bad client or server key: "
	+ e.getMessage();
      throw new IOException(m_loadError, e);
    } catch (javax.net.ssl.SSLPeerUnverifiedException e) {
      m_loadError = "Could not verify server identity: " + e.getMessage();
      throw new IOException(m_loadError, e);
    } catch (javax.net.ssl.SSLException e) {
      m_loadError = "Error negotiating SSL session: " + e.getMessage();
      throw new IOException(m_loadError, e);
    }
  }

  private InputStream getUrlInputStream0(LockssUrlConnection conn)
      throws IOException, MalformedURLException {
    InputStream in = null;
    String url = conn.getURL();

    Configuration conf = ConfigManager.getPlatformConfig();
    String proxySpec = conf.get(ConfigManager.PARAM_PROPS_PROXY);
    String proxyHost = null;
    int proxyPort = 0;

    try {
      HostPortParser hpp = new HostPortParser(proxySpec);
      proxyHost = hpp.getHost();
      proxyPort = hpp.getPort();
    } catch (HostPortParser.InvalidSpec e) {
      log.warning("Illegal props proxy: " + proxySpec, e);
    }

    if (proxyHost != null) {
      log.debug2("Setting request proxy to: " + proxyHost + ":" + proxyPort);
      conn.setProxy(proxyHost, proxyPort);
      proxyUsed = proxySpec;
    } else {
      proxyUsed = null;
    }

    conn.setRequestProperty(HttpHeaders.ACCEPT_ENCODING, "gzip");

    if (m_props != null) {
      Object x = m_props.get(Constants.X_LOCKSS_INFO);
      if (x instanceof String) {
	conn.setRequestProperty(Constants.X_LOCKSS_INFO, (String)x);
      }
    }
    conn.execute();
    if (checkAuth && !conn.isAuthenticatedServer()) {
      IOUtil.safeRelease(conn);
      throw new IOException("Config server not authenticated");
    }

    int resp = conn.getResponseCode();
    String respMsg = conn.getResponseMessage();
    log.debug2(url + " request got response: " + resp + ": " + respMsg);
    switch (resp) {
    case HttpURLConnection.HTTP_OK:
      log.debug2("New file, or file changed.  Loading file from " +
		 "remote connection:" + url);
      in = conn.getUncompressedResponseInputStream();
      break;
    case HttpURLConnection.HTTP_NOT_MODIFIED:
      log.debug2("HTTP content not changed, not reloading.");
      break;
    case HttpURLConnection.HTTP_PRECON_FAILED:
      log.debug2("Precondition failed, not reloading.");
      break;
    case HttpURLConnection.HTTP_NOT_FOUND:
      throw new FileNotFoundException(resp + ": " + respMsg);
    case HttpURLConnection.HTTP_FORBIDDEN:
      throw new IOException(findErrorMessage(resp, conn));
    default:
      throw new IOException(resp + ": " + respMsg);
    }

    return in;
  }

  @Override
  public String getProxyUsed() {
    return proxyUsed;
  }

  private static Pattern HINT_PAT =
    RegexpUtil.uncheckedCompile("LOCKSSHINT: (.+) ENDHINT",
				Perl5Compiler.CASE_INSENSITIVE_MASK);


  // If there is a response body, include any text between LOCKSSHINT: and
  // ENDHINT in the error message.
  private String findErrorMessage(int resp, LockssUrlConnection conn) {
    String msg = resp + ": " + conn.getResponseMessage();
    try {
      long len = conn.getResponseContentLength();
      if (len == 0 || len > 10000) {
	return msg;
      }
      InputStream in = conn.getUncompressedResponseInputStream();
      String ctype = conn.getResponseContentType();
      String charset = HeaderUtil.getCharsetOrDefaultFromContentType(ctype);
      Reader rdr = CharsetUtil.getReader(in, charset);
      String body = StringUtil.fromReader(rdr, 10000);
      if (StringUtil.isNullString(body)) {
	return msg;
      }
      Perl5Matcher matcher = RegexpUtil.getMatcher();
      if (matcher.contains(body, HINT_PAT)) {
	MatchResult matchResult = matcher.getMatch();
	String hint = matchResult.group(1);
	return msg + "\n" + hint;
      }
      return msg;
    } catch (Exception e) {
      log.warning("Error finding hint", e);
      return msg;
    } finally {
      IOUtil.safeRelease(conn);
    }
  }

  FileConfigFile failoverFcf;

  /** Return an InputStream open on the HTTP url.  If inaccessible and a
   * local copy of the remote file exists, failover to it.
   *
   * Called by periodic or on-demand config reload.
   */
  protected synchronized InputStream getInputStreamIfModified()
      throws IOException {
    LockssUrlConnection conn = null;
    InputStream in = null;
    boolean gettingResponse = false;

    try {
      conn = openUrlConnection(m_fileUrl);
      // When reloading config always go to server, don't serve from local
      // cache.
      conn.addRequestProperty("Cache-Control", "no-cache");

      // Check whether this configuration file has already been retrieved
      // before.
      if (m_config != null) {
	// Yes: Set the "If-Modified-Since" request header, if possible.
	if (m_lastModified != null) {
	  log.debug2("Setting request if-modified-since to: " + m_lastModified);
	  conn.setIfModifiedSince(m_lastModified);
	}

	// Yes: Set the "If-None-Match" request header, if possible.
	if (responseHttpEtag != null) {
	  log.debug2("Setting request if-none-match to: " + responseHttpEtag);
	  conn.addRequestProperty(HttpHeaders.IF_NONE_MATCH, responseHttpEtag);
	}
      }

      gettingResponse = true;
      in = openHttpInputStream(conn);

      int resp = conn.getResponseCode();
      switch (resp) {
      case HttpURLConnection.HTTP_OK:
	// Save the "Last-Modified" response header.
	m_httpLastModifiedString =
	conn.getResponseHeaderValue(HttpHeaders.LAST_MODIFIED);
	// Save the "ETag" response header.
	responseHttpEtag = conn.getResponseHeaderValue(HttpHeaders.ETAG);
      case HttpURLConnection.HTTP_NOT_MODIFIED:
      case HttpURLConnection.HTTP_PRECON_FAILED:
	m_loadError = null;
      }

      if (in != null) {
	// If we got remote content, clear any local failover copy as it
	// may now be obsolete
	failoverFcf = null;
      }
      return in;
    } catch (IOException e) {
      if (gettingResponse) {
	m_loadError = e.getMessage();
      }

      // The HTTP fetch failed.  First see if we already found a failover
      // file.
      log.info("Couldn't load remote config URL: " + m_fileUrl + ": "
	  + e.toString());
      FileConfigFile failoverFile = getFailoverFile();
      if (failoverFile == null) {
	throw e;
      }
      return failoverFile.getInputStreamIfModified();
    } finally {
      // Release the connection only if no remote content was obtained;
      // otherwise, the returned input stream to the remote content will be
      // closed.
      if (in == null) {
	IOUtil.safeRelease(conn);
      }
    }
  }

  /**
   * Provides the fail-over file.
   * 
   * @return a FileConfigFile with the fail-over file.
   */
  private FileConfigFile getFailoverFile() {
    // The HTTP fetch failed.  First see if we already found a failover
    // file.
    if (failoverFcf == null) {
      if (m_cfgMgr == null) {
	return null;
      }
      ConfigManager.RemoteConfigFailoverInfo rcfi =
	  m_cfgMgr.getRcfi(m_fileUrl);
      if (rcfi == null || !rcfi.exists()) {
	return null;
      }
      File failoverFile = rcfi.getPermFileAbs();
      if (failoverFile == null) {
	return null;
      }
      String chksum = rcfi.getChksum();
      if (chksum != null) {
	HashResult hr = HashResult.make(chksum);
	try {
	  HashResult fileHash = hashFile(failoverFile, hr.getAlgorithm());
	  if (!hr.equals(fileHash)) {
	    log.error("Failover file checksum mismatch");
	    if (log.isDebug2()) {
	      log.debug2("state   : " + hr);
	      log.debug2("computed: " + fileHash);
	    }
	    throw new IOException("Failover file checksum mismatch");
	  }
	} catch (NoSuchAlgorithmException nsae) {
	  log.error("Failover file found has unsupported checksum: " +
	      hr.getAlgorithm());
	  return null;
	} catch (IOException ioe) {
	  log.error("Can't read failover file", ioe);
	  return null;
	}
      } else if (CurrentConfig.getBooleanParam(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER_CHECKSUM_REQUIRED,
	  ConfigManager.DEFAULT_REMOTE_CONFIG_FAILOVER_CHECKSUM_REQUIRED)) {
	log.error("Failover file found but required checksum is missing");
	return null;
      }

      // Found one, 
      long date = rcfi.getDate();
      log.info("Substituting local copy created: " + new Date(date));
      failoverFcf = new FileConfigFile(failoverFile.getPath(), m_cfgMgr);
      m_loadedUrl = failoverFile.getPath();
    }
    return failoverFcf;
  }

  /**
   * Provides an input stream to the content of this file.
   * <br>
   * Use this to stream the file contents.
   *
   * Called to read the file for local editing.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  public InputStream getInputStream() throws IOException {
    final String DEBUG_HEADER = "getInputStream(): ";
    // Get the fail-over file, if any.
    FileConfigFile failoverFile = getFailoverFile();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "failoverFile = " + failoverFile);

    // Check whether a fail-over file was found.
    if (failoverFile != null) {
      // Yes: Return the input stream to the fail-over file.
      return failoverFile.getInputStream();
    }

    // No: Get the input stream from the network request.
    LockssUrlConnection conn = null;
    InputStream in = null;

    try {
      conn = openUrlConnection(m_fileUrl);
      // When reading file for local editing always go to server, don't
      // serve from local cache.
      conn.addRequestProperty("Cache-Control", "no-cache");

      // Check whether this configuration file has already been retrieved
      // before.
      if (m_config != null) {
	// Yes: Set the "If-Modified-Since" request header, if possible.
	if (m_lastModified != null) {
	  log.debug2("Setting request if-modified-since to: " + m_lastModified);
	  conn.setIfModifiedSince(m_lastModified);
	}

	// Yes: Set the "If-None-Match" request header, if possible.
	if (responseHttpEtag != null) {
	  log.debug2("Setting request if-none-match to: " + responseHttpEtag);
	  conn.addRequestProperty(HttpHeaders.IF_NONE_MATCH, responseHttpEtag);
	}
      }

      try {
	in = openHttpInputStream(conn);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "in == null? " + (in == null));

	int resp = conn.getResponseCode();
	switch (resp) {
	case HttpURLConnection.HTTP_OK:
	  // Save the "Last-Modified" response header.
	  m_httpLastModifiedString =
	  conn.getResponseHeaderValue(HttpHeaders.LAST_MODIFIED);
	  // Save the "ETag" response header.
	  responseHttpEtag = conn.getResponseHeaderValue(HttpHeaders.ETAG);
	case HttpURLConnection.HTTP_NOT_MODIFIED:
	case HttpURLConnection.HTTP_PRECON_FAILED:
	  m_loadError = null;
	}
      } catch (IOException ioe) {
	m_loadError = ioe.getMessage();
	throw ioe;
      }

      // Check whether the network request provided no input stream.
      if (in == null) {
	// Yes: Report the problem.
	String message = "Cannot get an input stream from '" + m_fileUrl + "'";
	log.error(message);
	throw new IOException(message);
      }

      return in;
    } finally {
      // Release the connection only if no remote content was obtained;
      // otherwise, the returned input stream to the remote content will be
      // closed.
      if (in == null) {
	IOUtil.safeRelease(conn);
      }
    }
  }

  // XXX Find a place for this
  HashResult hashFile(File file, String alg)
      throws NoSuchAlgorithmException, IOException {
    InputStream is = null;
    try {
      MessageDigest md = MessageDigest.getInstance(alg);
      is = new BufferedInputStream(new FileInputStream(file));
      StreamUtil.copy(is,
		      new org.apache.commons.io.output.NullOutputStream(),
		      -1, null, false, md);
      return HashResult.make(md.digest(), alg);
    } finally {
      IOUtil.safeClose(is);
    }
  }

  protected InputStream openHttpInputStream(LockssUrlConnection conn)
      throws IOException {
    m_IOException = null;
    InputStream in = getUrlInputStream(conn);
    if (in != null) {
      m_loadedUrl = null; // we're no longer loaded from failover, if we were.
      File tmpCacheFile;
      // If so configured, save the contents of the remote file in a locally
      // cached copy.
      if (m_cfgMgr != null &&
	  (tmpCacheFile =
	   m_cfgMgr.getRemoteConfigFailoverTempFile(m_fileUrl)) != null) {
	try {
	  log.log((  m_cfgMgr.haveConfig()
		     ? Logger.LEVEL_DEBUG
		     : Logger.LEVEL_INFO),
		  "Copying remote config: " + m_fileUrl);
	  OutputStream out =
	    new BufferedOutputStream(new FileOutputStream(tmpCacheFile));
	  out = makeHashedOutputStream(out);
	  out = new GZIPOutputStream(out, true);
	  InputStream wrapped = new TeeInputStream(in, out, true);
	  return wrapped;
	} catch (IOException e) {
	  log.error("Error opening remote config failover temp file: " +
		    tmpCacheFile, e);
	  return in;
	}
      }
    }
    return in;
  }

  OutputStream makeHashedOutputStream(OutputStream out) {
    String hashAlg =
      CurrentConfig.getParam(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER_CHECKSUM_ALGORITHM,
			     ConfigManager.DEFAULT_REMOTE_CONFIG_FAILOVER_CHECKSUM_ALGORITHM);
    if (!StringUtil.isNullString(hashAlg)) {
      try {
	chkDig = MessageDigest.getInstance(hashAlg);
	chkAlg = hashAlg;
	return new HashedOutputStream(out, chkDig);
      } catch (NoSuchAlgorithmException ex) {
	log.warning(String.format("Checksum algorithm %s not found, "
				  + "checksumming disabled", hashAlg));
      }
    }
    return out;
  }

  // Finished reading input, so failover file, if any, has now been
  // written and its checksum calculated.  Store it in rcfi.
  protected void loadFinished() {
    if (chkDig != null) {
      ConfigManager.RemoteConfigFailoverInfo rcfi =
	m_cfgMgr.getRcfi(m_fileUrl);
      if (rcfi != null) {
	HashResult hres = HashResult.make(chkDig.digest(), chkAlg);
	rcfi.setChksum(hres.toString());
      }
    }      
  }

  protected String calcNewLastModified() {
    return m_httpLastModifiedString;
  }

  /**
   * Provides the input stream to the content of this configuration file if the
   * passed preconditions are met.
   *
   * Will return a cached copy of the file if one is present and not stale.
   * 
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public synchronized ConfigFileReadWriteResult conditionallyRead(HttpRequestPreconditions preconditions)
      throws IOException {
    final String DEBUG_HEADER = "conditionallyRead(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);

    LockssUrlConnection conn = null;
    InputStream inputStream = null;
    String lastModified = null;
    String etag = null;
    boolean preconditionsMet = false;
    MediaType contentType = null;
    long contentLength = -1;

    try {
      // Create the connection.
      conn = openUrlConnection(m_fileUrl);

      // Get the individual preconditions.
      List<String> ifMatch = null;
      String ifModifiedSince = null;
      List<String> ifNoneMatch = null;
      String ifUnmodifiedSince = null;

      if (preconditions != null) {
  	ifMatch = preconditions.getIfMatch();
  	ifModifiedSince = preconditions.getIfModifiedSince();
  	ifNoneMatch = preconditions.getIfNoneMatch();
  	ifUnmodifiedSince = preconditions.getIfUnmodifiedSince();
      }

      // Check whether there are If-Match entity tags to be passed.
      if (ifMatch != null && !ifMatch.isEmpty()) {
	// Yes: Loop through all the If-Match entity tags.
	for (String ifMatchEtag : ifMatch) {
	  // Add the header to the request.
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "ifMatchEtag = " + ifMatchEtag);
	  conn.addRequestProperty(HttpHeaders.IF_MATCH, ifMatchEtag);
	}
      }

      // Check whether there is an If-Modified-Since condition.
      if (!StringUtil.isNullString(ifModifiedSince)) {
	// Yes: Add the header to the request.
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "ifModifiedSince = " + ifModifiedSince);
	conn.setIfModifiedSince(ifModifiedSince);
      }

      // Check whether there are If-None-Match entity tags to be passed.
      if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
	// Yes: Loop through all the If-None-Match entity tags.
	for (String ifNoneMatchEtag : ifNoneMatch) {
	  // Add the header to the request.
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "ifNoneMatchEtag = " + ifNoneMatchEtag);
	  conn.addRequestProperty(HttpHeaders.IF_NONE_MATCH, ifNoneMatchEtag);
	}
      }

      // Check whether there is an If-Unmodified-Since condition.
      if (!StringUtil.isNullString(ifUnmodifiedSince)) {
	// Yes: Add the header to the request.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "ifUnmodifiedSince = " + ifUnmodifiedSince);
	conn.addRequestProperty(HttpHeaders.IF_UNMODIFIED_SINCE,
	    ifUnmodifiedSince);
      }

      // Make the connection and get the response.
      inputStream = openHttpInputStream(conn);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "inputStream == null = " + (inputStream == null));

      // Get any incoming last-modified header.
      lastModified = conn.getResponseHeaderValue(HttpHeaders.LAST_MODIFIED);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModified = " + lastModified);

      // Get any incoming ETag header.
      etag = conn.getResponseHeaderValue(HttpHeaders.ETAG);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "etag = " + etag);

      // Check whether the input stream was obtained.
      if (inputStream != null) {
	// Yes.
	preconditionsMet = true;

	contentType = MediaType.parseMediaType(conn.getResponseContentType());
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "contentType = " + contentType);

	// Get the response content length.
	contentLength = conn.getResponseContentLength();
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);

	// Check whether the remote server did not set the response content
	// length header. This is going to be needed later by Spring to send the
	// response contents back to the client.
	if (contentLength < 0) {
	  // Yes: Read the response content and compute its length.
	  try (DeferredTempFileOutputStream dtfos =
	      new DeferredTempFileOutputStream(RESPONSE_MEMORY_THRESHOLD,
		  "httpConfigFile")) {
	    // Copy the response content.
	    contentLength = IOUtils.copy(inputStream, dtfos);
	    IOUtil.safeClose(inputStream);

	    inputStream = dtfos.getDeleteOnCloseInputStream();
	  }

	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);
	}
      }
    } finally {
      // Release the connection only if no remote content was obtained;
      // otherwise, the returned input stream to the remote content will be
      // closed.
      if (inputStream == null) {
	IOUtil.safeRelease(conn);
      }
    }

    // Build the result to be returned.
    ConfigFileReadWriteResult readResult =
	new ConfigFileReadWriteResult(inputStream, lastModified, etag,
	    preconditionsMet, contentType, contentLength);

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "readResult = " + readResult);
    return readResult;
  }

  /**
   * Used for logging and testing and debugging.
   */
  @Override
  public String toString() {
    return "[HTTPConfigFile: m_httpLastModifiedString="
	+ m_httpLastModifiedString + ", responseHttpEtag=" + responseHttpEtag
	+ ", failoverFcf=" + failoverFcf + ", " + super.toString() + "]";
  }
}
