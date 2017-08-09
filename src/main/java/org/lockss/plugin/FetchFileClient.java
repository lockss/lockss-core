/*

 Copyright (c) 2016-2017 Board of Trustees of Leland Stanford Jr. University,
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

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import org.lockss.config.CurrentConfig;
import org.lockss.daemon.GetUrlRepositoryPropertiesClient;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.util.Logger;
import org.lockss.ws.content.ContentService;
import org.lockss.ws.entities.ContentResult;

/**
 * A client for the ContentService.fetchFile() web service operation or for the
 * equivalent Repository REST web service.
 */
public class FetchFileClient {
  private static Logger log = Logger.getLogger(FetchFileClient.class);
  private static final String TIMEOUT_KEY =
      "com.sun.xml.internal.ws.request.timeout";

  /**
   * Provides the contents of a URL of an archival unit.
   * 
   * @param url
   *          A String with the URL.
   * @param auId
   *          A String with the identifier (auid) of the archival unit.
   * @return a ContentResult with the URL contents.
   * @throws Exception
   *           if there are problems getting the URL contents.
   */
  public ContentResult getUrlContent(String url, String auId)
      throws Exception {
    final String DEBUG_HEADER = "getUrlContent(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "url = " + url);
      log.debug2(DEBUG_HEADER + "auId = " + auId);
    }

    ContentResult result = null;

    // Get the configured REST service location.
    String restServiceLocation = CurrentConfig.getParam(
	PluginManager.PARAM_URL_ARTIFACT_REST_SERVICE_LOCATION);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "restServiceLocation = "
	+ restServiceLocation);

    // Check whether a REST service location has been configured.
    if (restServiceLocation != null
	&& restServiceLocation.trim().length() > 0) {
      // Yes: Get the URL artifacts from the REST service.
      List<Artifact> artifacts = new GetUrlRepositoryPropertiesClient()
	  .getUrlRepositoryProperties(url).getItems();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "artifacts = " + artifacts);

      if (artifacts == null || artifacts.size() < 1) {
	throw new Exception("No artifacts found for URL '" + url + "'");
      }

      // Get the artifact identifier.
      String artifactId = null;

      // Loop through all the received artifacts.
      for (Artifact artifact : artifacts) {
	// Check whether the Archival Unit identifier matches.
	if (auId.equals(artifact.getAuid())) {
	  // Yes: Get its artifact identifier.
	  artifactId = artifact.getId();
	  break;
	}
      }

      // Handle error conditions.
      if (artifactId == null || artifactId.trim().length() < 1) {
	throw new Exception("No artifacts found for URL '" + url + "' and AU '"
	    + auId + "'");
      }

      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "artifactId = " + artifactId);

      // Get the content of the artifact from the repository.
      result =
	  new GetArtifactContentClient().getArtifactContent(artifactId, url);
    } else {
      // No: Get the content from the non-REST service.
      result = getProxy().fetchFile(url, auId);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
}

  /**
   * Provides a proxy to the web service.
   * 
   * @return a ContentService with the proxy to the web service.
   * @throws Exception
   *           if there are problems getting the proxy.
   */
  protected ContentService getProxy() throws Exception {
    final String DEBUG_HEADER = "getProxy(): ";
    authenticate();
    String addressLocation = CurrentConfig.getParam(
	PluginManager.PARAM_URL_CONTENT_WS_ADDRESS_LOCATION);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "addressLocation = " + addressLocation);

    String targetNamespace = CurrentConfig.getParam(
	PluginManager.PARAM_URL_CONTENT_WS_TARGET_NAMESPACE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "targetNamespace = " + targetNamespace);

    String serviceName = CurrentConfig.getParam(
	PluginManager.PARAM_URL_CONTENT_WS_SERVICE_NAME);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "serviceName = " + serviceName);

    Service service = Service.create(new URL(addressLocation), new QName(
	targetNamespace, serviceName));

    ContentService port = service.getPort(ContentService.class);

    // Set the client connection timeout.
    int timeoutValue = CurrentConfig.getIntParam(
	PluginManager.PARAM_URL_LIST_WS_TIMEOUT_VALUE,
	PluginManager.DEFAULT_URL_LIST_WS_TIMEOUT_VALUE);
    ((javax.xml.ws.BindingProvider) port).getRequestContext().put(TIMEOUT_KEY,
	new Integer(timeoutValue*1000));

    return port;
  }

  /**
   * Sets the authenticator that will be used by the networking code when the
   * HTTP server asks for authentication.
   */
  protected void authenticate() {
    Authenticator.setDefault(new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
	String userName =
	    CurrentConfig.getParam(PluginManager.PARAM_URL_LIST_WS_USER_NAME);
	String password =
	    CurrentConfig.getParam(PluginManager.PARAM_URL_LIST_WS_PASSWORD);
	return new PasswordAuthentication(userName, password.toCharArray());
      }
    });
  }
}
