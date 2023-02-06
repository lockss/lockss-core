/*

Copyright (c) 2000-2021, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Properties;
import org.lockss.app.LockssDaemon;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.plugin.PluginManager;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.util.Logger;
import org.lockss.util.MapUtil;
import org.lockss.util.TemplateUtil;
import org.lockss.util.auth.*;
import org.lockss.util.rest.RestUtil;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.FileSystemUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Base class for Spring REST web service test classes.
 */
public abstract class SpringLockssTestCase extends LockssTestCase4 {
  /**
   * The name of the file with the configured platform disk space.
   */
  public static final String PLATFORM_DISK_SPACE_CONFIG_FILENAME =
      "platform.txt";

  /**
   * The name of the file with the UI port configuration template.
   */
  public static final String UI_PORT_CONFIGURATION_TEMPLATE =
    "UiPortConfigTemplate.txt";

  /**
   * The name of the file with the configured UI port.
   */
  public static final String UI_PORT_CONFIGURATION_FILE = "UiPort.txt";

  private static final Logger log = Logger.getLogger(SpringLockssTestCase.class);

  // The path of a temporary directory where the test data will reside.
  private String tempDirPath = null;

  // The path to the configuration file with the platform disk space location
  // definition.
  private String platformDiskSpaceConfigPath = null;

  // The configuration file that specifies the UI port.
  private File uiPortConfigFile = null;

  // The configuration file that specifies the database properties.
  private File dbConfigFile = null;

  // The configuration file that specifies the repository.
  private File repoConfigFile = null;

  /**
   * Provides the path to the temporary directory where the test data will
   * reside.
   * 
   * @return a String with the path to the temporary directory where the test
   *         data will reside.
   */
  protected String getTempDirPath() {
    return tempDirPath;
  }

  /**
   * Provides the path to the configuration file with the platform disk space
   * location definition.
   * 
   * @return a String with the path to the configuration file with the platform
   *         disk space location definition.
   */
  protected String getPlatformDiskSpaceConfigPath() {
    return platformDiskSpaceConfigPath;
  }

  /**
   * Provides the value in a file for a property with a given name.
   * 
   * @param propertyName
   *          A String with the name of the configuration property.
   * @param configFile
   *          A File with the configuration file containing the configuration
   *          property value.
   * @return a String with the requested property value.
   */
  protected static String getPropertyValueFromFile(String propertyName,
      File configFile) {
    if (log.isDebug2()) {
      log.debug2("propertyName = " + propertyName);
      log.debug2("configFile = " + configFile.getAbsolutePath());
    }

    String propertyValue = null;
    FileInputStream is = null;

    try {
      // Get the properties from the configuration file.
      is = new FileInputStream(configFile);
      Properties properties = new Properties();
      properties.load(is);

      // Get the requested property value.
      propertyValue = properties.getProperty(propertyName);
      if (log.isDebug3()) log.debug3("propertyValue = " + propertyValue);
    } catch (IOException ioe) {
      log.warning("Caught exception getting properties from file: " + ioe);
    } finally {
      try {
	is.close();
      } catch (IOException ioe) {
	log.warning("Caught exception closing file input stream: " + ioe);
      }
    }

    if (log.isDebug2()) log.debug2("propertyValue = " + propertyValue);
    return propertyValue;
  }

  /**
   * Provides the indication of whether an external REST service is available.
   * 
   * @param restServiceLocation
   *          A String with the REST service location template.
   * @param uriMap
   *          A Map<String, String> with the map of values to be interpolated in
   *          the REST service location template.
   * @param successStatusCode
   *          An int with the expected successful status code returned by the
   *          REST service.
   * @return a boolean with <code>true</code> if the external REST service is
   *         available, <code> false</code> otherwise.
   */
  protected static boolean checkExternalRestService(String restServiceLocation,
      Map<String, String> uriMap, int successStatusCode) {
    if (log.isDebug2()) {
      log.debug2("restServiceLocation = " + restServiceLocation);
      log.debug2("uriMap = " + uriMap);
      log.debug2("successStatusCode = " + successStatusCode);
    }

    boolean isServiceAvailable = false;

    // Initialize the request to the REST service.
    RestTemplate restTemplate = RestUtil.getRestTemplate();

    // Initialize the request headers.
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    // Create the URI of the request to the REST service.
    UriComponents uriComponents = UriComponentsBuilder
	.fromUriString(restServiceLocation).build().expand(uriMap);

    URI uri = UriComponentsBuilder.newInstance()
	.uriComponents(uriComponents).build().encode().toUri();
    if (log.isDebug3()) log.debug3("Making request to '" + uri + "'...");

    try {
      // Make the request to the REST service.
      ResponseEntity<String> result = restTemplate.exchange(uri, HttpMethod.GET,
	  new HttpEntity<String>(null, headers), String.class);

      // Get its response.
      int statusCode = result.getStatusCode().value();
      if (log.isDebug3()) log.debug3("Done: statusCode = " + statusCode);

      // Determine whether the request has been successful.
      isServiceAvailable = statusCode == successStatusCode;
    } catch (Exception e) {
      if (log.isDebug3()) log.debug3("Done: No REST service.");
    }

    if (log.isDebug2())
      log.debug2("isServiceAvailable = " + isServiceAvailable);
    return isServiceAvailable;
  }

  /**
   * Sets up the temporary directory used for the tests.
   * 
   * @param prefix
   *          A String with the prefix of the name of the directory.
   * @throws IOException
   *           if there are problems.
   */
  protected void setUpTempDirectory(String prefix) throws IOException {
    if (log.isDebug2()) log.debug2("prefix = " + prefix);

    // Get the path of a temporary directory where the test data will reside.
    tempDirPath = getTempDir(prefix).getAbsolutePath();
    if (log.isDebug3()) log.debug3("tempDirPath = " + tempDirPath);

    // Create a file that will communicate to the test REST service where its
    // data is located.
    platformDiskSpaceConfigPath =
	createPlatformDiskSpaceConfigFile(tempDirPath);
    if (log.isDebug3()) log.debug3("platformDiskSpaceConfigPath = "
	+ platformDiskSpaceConfigPath);
  }

  /**
   * Creates a file that will communicate to the test REST service where its
   * data is located.
   *
   * @param dirPath
   *          A String with the path to the directory where the file is to be
   *          created.
   * @return a String with the path to the created file.
   * @throws IOException
   *           if there are problems.
   */
  private String createPlatformDiskSpaceConfigFile(String dirPath)
      throws IOException {
    if (log.isDebug2()) log.debug2("dirPath = " + dirPath);

    // The configuration option with the temporary directory where the test data
    // resides.
    String platformDiskSpaceConfigParam =
	ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST + "=" + dirPath
	+ File.separator + "cache" + System.lineSeparator();
    if (log.isDebug3()) log.debug3("platformDiskSpaceConfigParam = '"
	+ platformDiskSpaceConfigParam +"'.");

    // The path to the file.
    String platformDiskSpaceConfigPath =
	dirPath + File.separator + PLATFORM_DISK_SPACE_CONFIG_FILENAME;

    // Create the file.
    Files.write(Paths.get(platformDiskSpaceConfigPath),
	platformDiskSpaceConfigParam.getBytes(), StandardOpenOption.CREATE);

    if (log.isDebug2()) log.debug2("platformDiskSpaceConfigPath = "
	+ platformDiskSpaceConfigPath);
    return platformDiskSpaceConfigPath;
  }

  /**
   * Copies a file or directory to the temporary directory.
   * 
   * @param source
   *          A File with the file or directory to be copied.
   * @throws IOException
   *           if there are problems.
   */
  protected void copyToTempDir(File source) throws IOException {
    if (log.isDebug2()) log.debug2("source = " + source.getAbsolutePath());

    if (!source.isDirectory() && !source.isFile()) {
      throw new IOException(source.getAbsolutePath()
	  + " is neither a file nor a directory");
    }

    File destination = new File(new File(tempDirPath), source.getName());
    if (log.isDebug3())
	log.debug3("destination = " + destination.getAbsolutePath());

    if (source.isDirectory()) {
      // Make the copy.
      FileSystemUtils.copyRecursively(source, destination);
    } else {
      // Make the copy.
      FileCopyUtils.copy(source, destination);
    }
  }

  /**
   * Creates the configuration file that specifies the UI port, determined by
   * picking a currently unused port.
   * 
   * @param uiPortConfigTemplateName
   *          A String with the name of the template used as a source.
   * @param uiPortConfigFileName
   *          A String with the name of the UI port configuration file to be
   *          created.
   * @throws IOException
   *           if there are problems reading the template or writing the output.
   */
  protected void setUpUiPort(String uiPortConfigTemplateName,
      String uiPortConfigFileName) throws IOException {
    if (log.isDebug2())
      log.debug2("uiPortConfigFileName = " + uiPortConfigFileName);

    // Create the UI port configuration file output file.
    uiPortConfigFile = new File(getTempDirPath(), uiPortConfigFileName);
    if (log.isDebug3()) log.debug("uiPortConfigFile = " + uiPortConfigFile);

    // Find an unused port.
    int uiPort = TcpTestUtil.findUnboundTcpPort();
    if (log.isDebug3()) log.debug("uiPort = " + uiPort);

    // Write the UI port configuration file using the template file.
    try (Writer writer = new BufferedWriter(new FileWriter(uiPortConfigFile))) {
      TemplateUtil.expandTemplate(uiPortConfigTemplateName, writer,
	  MapUtil.map("UIPort", Integer.toString(uiPort)));
    }
  }

  /**
   * Provides the UI port configuration file.
   * 
   * @return a File with the UI port configuration file.
   */
  protected File getUiPortConfigFile() {
    return uiPortConfigFile;
  }

  /**
   * Creates the configuration file that specifies the database properties.
   * 
   * @param dbConfigTemplateName
   *          A String with the name of the template used as a source.
   * @param dbConfigFileName
   *          A String with the name of the database configuration file to be
   *          created.
   * @throws IOException
   *           if there are problems reading the template or writing the output.
   */
  protected void setUpDbConfig(String dbConfigTemplateName,
      String dbConfigFileName) throws IOException {
    if (log.isDebug2()) log.debug2("dbConfigFileName = " + dbConfigFileName);

    // Create the database configuration file output file.
    dbConfigFile = new File(getTempDirPath(), dbConfigFileName);
    if (log.isDebug3()) log.debug("dbConfigFile = " + dbConfigFile);

    // Write the database configuration file using the template file.
    try (Writer writer = new BufferedWriter(new FileWriter(dbConfigFile))) {
      TemplateUtil.expandTemplate(dbConfigTemplateName, writer,
	  MapUtil.map("DbPath", getTempDirPath()));
    }
  }

  /**
   * Creates the configuration file that specifies the repository.
   * 
   * @param repoConfigTemplateName
   *          A String with the name of the template used as a source.
   * @param repoConfigFileName
   *          A String with the name of the repository configuration file to be
   *          created.
   * @throws IOException
   *           if there are problems reading the template or writing the output.
   */
  protected void setUpRepositoryConfig(String repoConfigTemplateName,
      String repoConfigFileName) throws IOException {
    if (log.isDebug2())
      log.debug2("repoConfigFileName = " + repoConfigFileName);

    // Create the repository configuration file output file.
    repoConfigFile = new File(getTempDirPath(), repoConfigFileName);
    if (log.isDebug3()) log.debug("repoConfigFile = " + repoConfigFile);

    // Write the repository configuration file using the template file.
    try (Writer writer = new BufferedWriter(new FileWriter(repoConfigFile))) {
      TemplateUtil.expandTemplate(repoConfigTemplateName, writer,
	  MapUtil.map("RepoPath", getTempDirPath()));
    }
  }

  /**
   * Provides the database configuration file.
   * 
   * @return a File with the database configuration file.
   */
  protected File getDbConfigFile() {
    return dbConfigFile;
  }

  /**
   * Provides the repository configuration file.
   * 
   * @return a File with the repository configuration file.
   */
  protected File getRepositoryConfigFile() {
    return repoConfigFile;
  }

  /**
   * Runs the Swagger-related tests.
   * 
   * @param restTemplate
   *          A RestTemplate to be used to call the REST service.
   * @param url
   *          A String with the URL of the REST service.
   * 
   * @throws Exception
   *           if there are problems.
   */
  protected void runGetSwaggerDocsTest(String url) throws Exception {
    log.debug3("url = " + url);

    // Perform the call to the REST service.
    ResponseEntity<String> successResponse = RestUtil.getRestTemplate()
	.exchange(url, HttpMethod.GET, null, String.class);

    // Check the status.
    HttpStatus statusCode = successResponse.getStatusCode();
    assertEquals(HttpStatus.OK, statusCode);

    // Read the Swagger YAML configuration file.
    try (InputStream is = Thread.currentThread().getContextClassLoader()
	  .getResourceAsStream("swagger/swagger.yaml")) {
      ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
      JsonNode root = yamlMapper.readTree(is);
      log.debug3("swaggerConf = " + root);

      // Get the Swagger version.
      String swaggerVersion = root.get("swagger").textValue();
      log.debug3("swaggerVersion = " + swaggerVersion);

      // Get the Swagger description.
      String swaggerDescription = root.get("info").get("description").textValue();
      log.debug3("swaggerDescription = " + swaggerDescription);

      // Verify the body of the response.
      String expectedBody = "{'swagger':'" + swaggerVersion + "',"
  	+ "'info':{'description':'" + swaggerDescription + "'}}";

      JSONAssert.assertEquals(expectedBody, successResponse.getBody(), false);
    } catch (Exception e) {
      log.error("Exception caught getting the Swagger configuration: ", e);
      throw e;
    }

    log.debug2("Done");
  }

  /** Intended for tests of code that normally runs in on-demand AU
   * creation mode (e.g., mdq & mdx services), to make the tests work in
   * startAllAus mode.  Creates an AU from config inferred from the AUID,
   * iff the AU doesn't already exist and the daemon is not running in
   * on-demand mode. */

  protected void startAuIfNecessary(String auId) {
    log.debug("startAuIfNecessary("+auId+")");
    if (auId == null) {
      // avoid making tests of illegal/missing auids get NPE
      return;
    }
    LockssDaemon daemon = LockssDaemon.getLockssDaemon();
    PluginManager pmgr = daemon.getPluginManager();
    if (pmgr.isStartAusOnDemand()) {
      return;
    }
    ArchivalUnit au = pmgr.getAuFromId(auId);
    if (au != null) {
      return;
    }
    Configuration auConfig = PluginManager.getAuConfigFromAuId(auId);
    if (auConfig == null) {
      log.error("Couldn't infer AU config in order to create it: " + auId);
      return;
    }
    String pluginId = PluginManager.pluginIdFromAuId(auId);
    pmgr.ensurePluginLoaded(pluginId);
    try {
      pmgr.createAndSaveAuConfiguration(pmgr.getPlugin(pluginId), auConfig);
    } catch (org.lockss.plugin.ArchivalUnit.ConfigurationException |
	     org.lockss.db.DbException |
	     org.lockss.util.rest.exception.LockssRestException e) {
      log.error("Error creating AU", e);
    }
  }

  /**
   * Encapsulation of web authentication credentials.
   */
  protected class Credentials {
    private final String user;
    private final String password;

    /**
     * Constructor.
     * 
     * @param user
     *          A String with the user identifier.
     * @param password
     *          A String with the password used to authenticate the user.
     */
    public Credentials (String user, String password) {
      this.user = user;
      this.password = password;
    }

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
    }

    /**
     * Sets up these credentials for Basic authentication.
     * 
     * @param headers
     *          An HttpHeaders with the HTTP headers where to set up Basic
     *          authentication.
     */
    public void setUpBasicAuthentication(HttpHeaders headers) {
      // Check whether there are credentials to be added.
      if (user != null && password != null) {
        // Yes: Set the authentication credentials.
	String authHeaderValue = AuthUtil.basicAuthHeaderValue(user, password);
        headers.set("Authorization", authHeaderValue);
      }
    }

    @Override
    public String toString() {
      return "[Credentials user=" + user + ", password=" + password + "]";
    }
  }
}
