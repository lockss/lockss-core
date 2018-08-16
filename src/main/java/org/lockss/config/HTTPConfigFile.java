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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.io.output.DeferredFileOutputStream;
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

  // TODO: Needed as a member?
  private String responseHttpEtag = null;

  private LockssUrlConnectionPool m_connPool;
  private boolean checkAuth = false;
  private MessageDigest chkDig;
  private String chkAlg;

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
    }

    // TODO: Asymmetry with "If-[None-]Match" headers that are not always set?
    if (m_config != null && m_lastModified != null) {
      log.debug2("Setting request if-modified-since to: " + m_lastModified);
      conn.setIfModifiedSince(m_lastModified);
    }
    conn.setRequestProperty("Accept-Encoding", "gzip");

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
      m_loadError = null;
      m_httpLastModifiedString = conn.getResponseHeaderValue("last-modified");
      log.debug2("New file, or file changed.  Loading file from " +
		 "remote connection:" + url);
      in = conn.getUncompressedResponseInputStream();
      break;
    case HttpURLConnection.HTTP_NOT_MODIFIED:
      m_loadError = null;
      log.debug2("HTTP content not changed, not reloading.");
      break;
    case HttpURLConnection.HTTP_PRECON_FAILED:
      m_loadError = null;
      log.debug2("Precondition failed, not reloading.");
      break;
    case HttpURLConnection.HTTP_NOT_FOUND:
      m_loadError = resp + ": " + respMsg;
      throw new FileNotFoundException(m_loadError);
    case HttpURLConnection.HTTP_FORBIDDEN:
      m_loadError = findErrorMessage(resp, conn);
      throw new IOException(m_loadError);
    default:
      m_loadError = resp + ": " + respMsg;
      throw new IOException(m_loadError);
    }

    return in;
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
      local copy of the remote file exists, failover to it. */
  protected InputStream getInputStreamIfModified() throws IOException {
    LockssUrlConnection conn = null;

    try {
      conn = openUrlConnection(m_fileUrl);
      InputStream in = openHttpInputStream(conn);
      if (in != null) {
	// If we got remote content, clear any local failover copy as it
	// may now be obsolete
	failoverFcf = null;
      }
      return in;
    } catch (IOException e) {
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
      IOUtil.safeRelease(conn);
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

    try {
      conn = openUrlConnection(m_fileUrl);
      InputStream in = openHttpInputStream(conn);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "in == null? " + (in == null));

      // Check whether the network request provided no input stream.
      if (in == null) {
	// Yes: Report the problem.
	String message = "Cannot get an input stream from '" + m_fileUrl + "'";
	log.error(message);
	throw new IOException(message);
      }

      return in;
    } finally {
      IOUtil.safeRelease(conn);
    }
  }

  // XXX Find a place for this
  // Maybe HashResult.make(File file, String algorithm)?
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
   * @param ifMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Unmodified-Since" request header but with a granularity of 1
   *          ms.
   * @param ifNoneMatch
   *          A List<String> with an asterisk or values equivalent to the
   *          "If-Modified-Since" request header but with a granularity of 1 ms.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public ConfigFileReadWriteResult conditionallyRead(List<String> ifMatch,
      List<String> ifNoneMatch) throws IOException {
    final String DEBUG_HEADER = "conditionallyRead(" + m_fileUrl + "): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "ifMatch = " + ifMatch);
      log.debug2(DEBUG_HEADER + "ifNoneMatch = " + ifNoneMatch);
    }

    synchronized(this) {
      LockssUrlConnection conn = null;

      try {
	// Create the connection.
	conn = openUrlConnection(m_fileUrl);

	// Check whether there are If-Match entity tags to be passed.
	if (ifMatch != null && !ifMatch.isEmpty()) {
	  // Yes: Loop through all the If-Match entity tags.
	  for (String etag : ifMatch) {
	    // Add the header to the request.
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "If-Match etag = " + etag);
	    conn.addRequestProperty("If-Match", etag);
	  }
	  // No: Check whether there are If-None-Match entity tags to be passed.
	} else if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
	  // Yes: Loop through all the If-None-Match entity tags.
	  for (String etag : ifNoneMatch) {
	    // Add the header to the request.
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "If-None-Match etag = " + etag);
	    conn.addRequestProperty("If-None-Match", etag);
	  }
	}

	// Make the connection and get the response.
	InputStream inputStream = openHttpInputStream(conn);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "inputStream == null = " + (inputStream == null));

	// Handle any incoming Etag header.
	responseHttpEtag = conn.getResponseHeaderValue("ETag");
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "responseHttpEtag = " + responseHttpEtag);

	String versionUniqueId = responseHttpEtag;

	if (responseHttpEtag != null && responseHttpEtag.length() > 1
	    && responseHttpEtag.startsWith("\"")
	    && responseHttpEtag.endsWith("\"")) {
	  versionUniqueId =
	      responseHttpEtag.substring(1, responseHttpEtag.length() - 1);
	}

	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "versionUniqueId = " + versionUniqueId);

	MediaType contentType = null;
	long contentLength = -1;
	boolean preconditionMet = false;

	// Check whether the input stream was obtained.
	if (inputStream != null) {
	  // Yes.
	  preconditionMet = true;

	  contentType = MediaType.parseMediaType(conn.getResponseContentType());
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "contentType = " + contentType);

	  // Get the response content length.
	  contentLength = conn.getResponseContentLength();
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);

	  // Check whether the remote server did not set the response content
	  // length header. This is going to be needed later by Spring to send
	  // the response contents back to the client.
	  if (contentLength < 0) {
	    // Yes: Read the response and compute its length.
	    try (DeferredFileOutputStream dfos = new DeferredFileOutputStream(
		RESPONSE_MEMORY_THRESHOLD,
		FileUtil.createTempFile("httpConfigFile", ".dfos"))) {
	      // Copy the response.
	      IOUtils.copy(inputStream, dfos);

	      File dfosFile = dfos.getFile();
	      // TODO: Maybe delete proactively?
	      dfosFile.deleteOnExit();

	      if (dfos.isInMemory()) {
		inputStream = new ByteArrayInputStream(dfos.getData());
		contentLength = dfos.getData().length;
	      } else {
		inputStream = new FileInputStream(dfosFile);
		contentLength = dfosFile.length();
	      }
	    }

	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);
	  }
	}

	// Build the result to be returned.
	ConfigFileReadWriteResult readResult =
	    new ConfigFileReadWriteResult(inputStream, versionUniqueId,
		preconditionMet, contentType, contentLength);

	if (log.isDebug2())
	  log.debug2(DEBUG_HEADER + "readResult = " + readResult);
	return readResult;
      } finally {
	IOUtil.safeRelease(conn);
      }
    }
  }
}
