/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.laaws.status.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;
import org.lockss.log.L4JLogger;
import org.lockss.util.BuildInfo;

/**
 * Representation of the status of a REST web service.
 */
public class ApiStatus {
  private static final L4JLogger log = L4JLogger.getLogger();

  /**
   * The name of the component.
   */
  private String componentName = null;

  /**
   * The name of the service.
   */
  private String serviceName = null;

  /**
   * The version of the LOCKSS system.
   */
  private String lockssVersion = null;

  /**
   * The version of the component software.
   */
  private String componentVersion = null;

  /**
   * The version of the API.
   */
  private String apiVersion = null;

  /**
   * An indication of whether the REST web service is ready to process requests.
   */
  private Boolean ready = Boolean.FALSE;

  /**
   * The time the service became ready
   */
  private long readyTime = 0;

  /**
   * Explanation for not ready.
   */
  private String reason = null;

  /**
   * No-argument constructor.
   */
  public ApiStatus() {
  }

  /**
   * Constructor with Swagger YAML resource location.
   *
   * @param swaggerYamlFileResource
   *          A String with the Swagger YAML resource location.
   */
  public ApiStatus(String swaggerYamlFileResource) {
    // Use an input stream to the Swagger YAML resource.
    try (InputStream is = Thread.currentThread().getContextClassLoader()
	  .getResourceAsStream(swaggerYamlFileResource)) {
      // Get the name of the component.
      componentName = BuildInfo.getBuildProperty(BuildInfo.BUILD_NAME);
      log.trace("componentName = {}", componentName);

      // Get the Swagger YAML resource "info" entry.
      SwaggerInfo swaggerInfo = new ObjectMapper(new YAMLFactory())
	  .readValue(is, SwaggerYaml.class).getInfo();

      // Get the service name.
      serviceName = swaggerInfo.getTitle();
      log.trace("serviceName = {}", serviceName);

      // Get the LOCKSS version.
      lockssVersion = BuildInfo.getBuildProperty(BuildInfo.BUILD_RELEASENAME);
      log.trace("lockssVersion = {}", lockssVersion);

      // Get the component version.
      componentVersion = BuildInfo.getBuildProperty(BuildInfo.BUILD_VERSION);
      log.trace("componentVersion = {}", componentVersion);

      // Get the API version.
      apiVersion = swaggerInfo.getVersion();
      log.trace("apiVersion = {}", apiVersion);
    } catch (Exception e) {
      log.error("Exception caught getting the API version: ", e);
    }
  }

  /**
   * Provides the name of the component.
   * 
   * @return a String with the name of the component.
   */
  public String getComponentName() {
    return componentName;
  }

  /**
   * Saves the name of the component.
   * 
   * @param componentName
   *          A String with the name of the component.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setComponentName(String componentName) {
    this.componentName = componentName;
    return this;
  }

  /**
   * Provides the name of the service.
   * 
   * @return a String with the name of the service.
   */
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Saves the name of the service.
   * 
   * @param serviceName
   *          A String with the name of the service.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setServiceName(String serviceName) {
    this.serviceName = serviceName;
    return this;
  }

  /**
   * Provides the version of the LOCKSS system.
   * 
   * @return a String with the version of the LOCKSS system.
   */
  public String getLockssVersion() {
    return lockssVersion;
  }

  /**
   * Saves the version of the LOCKSS system.
   * 
   * @param lockssVersion
   *          A String with the version of the LOCKSS system.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setLockssVersion(String lockssVersion) {
    this.lockssVersion = lockssVersion;
    return this;
  }

  /**
   * Provides the version of the component.
   * 
   * @return a String with the version of the component.
   */
  public String getComponentVersion() {
    return componentVersion;
  }

  /**
   * Saves the version of the component.
   * 
   * @param componentVersion
   *          A String with the version of the component.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setComponentVersion(String componentVersion) {
    this.componentVersion = componentVersion;
    return this;
  }

  /**
   * Provides the version of the API.
   * 
   * @return a String with the version of the API.
   */
  public String getApiVersion() {
    return apiVersion;
  }

  /**
   * Saves the version of the API.
   * 
   * @param apiVersion
   *          A String with the version of the API.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
    return this;
  }

  /**
   * Saves the version of the API.
   * 
   * @param apiVersion
   *          A String with the version of the API.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setVersion(String apiVersion) {
    this.apiVersion = apiVersion;
    return this;
  }

  /**
   * Provides an indication of whether the REST web service is available.
   * 
   * @return a Boolean with the indication of whether the REST web service is
   *         available.
   */
  public Boolean isReady() {
    return ready;
  }

  /**
   * Saves the indication of whether the REST web service is available.
   * 
   * @param ready
   *          A Boolean with the indication of whether the REST web service is
   *          available.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setReady(Boolean ready) {
    this.ready = ready;
    return this;
  }

  /**
   * Return the time the service last became ready
   * 
   * @return a long indicating the time the service last became ready
   */
  public long getReadyTime() {
    return readyTime;
  }

  /**
   * Set the time the service last became ready
   * 
   * @param time time service became ready
   * @return an ApiStatus with this object.
   */
  public ApiStatus setReadyTime(long time) {
    this.readyTime = time;
    return this;
  }

  /**
   * Provides the reason the service isn't ready
   * 
   * @return a Boolean with the indication of the reason the service isn't
   * ready.
   */
  public String getReason() {
    return reason;
  }

  /**
   * Saves the indication of whether the REST web service is available.
   * 
   * @param reason
   *          A Boolean with the indication of whether the REST web service is
   *          available.
   * @return an ApiStatus with this object.
   */
  public ApiStatus setReason(String reason) {
    this.reason = reason;
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[ApiStatus ");
    sb.append(ready ? " Ready" : " Not Ready");
    appendIf(sb, reason, "reason");
    appendIf(sb, componentName, "component");
    appendIf(sb, serviceName, "service");
    appendIf(sb, lockssVersion, "lockssVer");
    appendIf(sb, componentVersion, "componentVer");
    appendIf(sb, apiVersion, "apiVer");
    sb.append("]");
    return sb.toString();
  }

  private void appendIf(StringBuilder sb, String val, String name) {
    if (val != null) {
      sb.append(", ");
      sb.append(name);
      sb.append(": ");
      sb.append(val);
    }
  }

  /**
   * Provides a JSON version of this object.
   * 
   * @return a String with a JSON version of this object.
   * @throws JsonProcessingException
   *           if there are problems generating the JSON string.
   */
  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }

  /**
   * A partial representation of a Swagger YAML file.
   */
  // Ignore uninteresting Swagger YAML file top entries.
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SwaggerYaml {
    // The info entry in the Swagger YAML file.
    @JsonProperty
    private SwaggerInfo info;

    /**
     * Provides the info entry in the Swagger YAML file.
     * 
     * @return a SwaggerInfo with the info entry in the Swagger YAML file.
     */
    public SwaggerInfo getInfo() {
      return info;
    }

    /**
     * Saves the info entry in the Swagger YAML file.
     * 
     * @param info
     *          A SwaggerInfo with the info entry in the Swagger YAML file.
     */
    public void setInfo(SwaggerInfo info) {
      this.info = info;
    }
  }

  /**
   * A partial representation of an "info" top entry of a Swagger YAML file.
   */
  // Ignore uninteresting entries in the Swagger YAML file top "info" entry.
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class SwaggerInfo {
    // The service name.
    @JsonProperty
    private String title;

    // The API version.
    @JsonProperty
    private String version;

    /**
     * Provides the API title.
     * 
     * @return a String with the API title.
     */
    public String getTitle() {
      return title;
    }

    /**
     * Saves the API title.
     * 
     * @param title
     *          A String with the API title.
     */
    public void setTitle(String title) {
      this.title = title;
    }

    /**
     * Provides the API version.
     * 
     * @return a String with the API version.
     */
    public String getVersion() {
      return version;
    }

    /**
     * Saves the API version.
     * 
     * @param version
     *          A String with the API version.
     */
    public void setVersion(String version) {
      this.version = version;
    }
  }
}
