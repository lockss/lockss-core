/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.config;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashMap;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.*;
import org.lockss.util.*;
import org.lockss.plugin.PluginManager;

/**
 * The encapsulation of an Archival Unit configuration
 */
@ApiModel(description = "The encapsulation of an Archival Unit configuration")

public class AuConfiguration   {
  @JsonProperty("auId")
  private String auId = null;

  @JsonProperty("auConfig")
  @Valid
  private Map<String, String> auConfig = new HashMap<>();

  /**
   * Default constructor.
   *
   * Needed by JSON serialization machinery. Otherwise, IT SHOULD NOT BE USED,
   * as there is no upfront validation.
   */
  private AuConfiguration() {
  }

  /**
   * Constructor from members.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @param auConfig
   *          A Map<String, String> with the Archival Unit configuration.
   */
  public AuConfiguration(String auId, Map<String, String> auConfig) {
    super();

    // Validation.
    if (auId == null || auId.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid Archival Unit identifier: '"
	  + auId + "'");
    }

    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '" + auConfig
	  + "'");
    }

    this.auId = auId;
    this.auConfig = auConfig;
  }

  public AuConfiguration auId(String auId) {
    // Validation.
    if (auId == null || auId.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid Archival Unit identifier: '"
	  + auId + "'");
    }

    this.auId = auId;
    return this;
  }

  /**
   * The identifier of the Archival Unit
   * @return auId
  **/
  @ApiModelProperty(required = true, value = "The identifier of the Archival Unit")
  @NotNull


  public String getAuId() {
    // Validation.
    if (auId == null || auId.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid Archival Unit identifier: '"
	  + auId + "'");
    }

    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '" + auConfig
	  + "'");
    }

    return auId;
  }

  public void setAuId(String auId) {
    // Validation.
    if (auId == null || auId.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid Archival Unit identifier: '"
	  + auId + "'");
    }

    this.auId = auId;
  }

  public AuConfiguration auConfig(Map<String, String> auConfig) {
    // Validation.
    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '" + auConfig
	  + "'");
    }

    this.auConfig = auConfig;
    return this;
  }

  public AuConfiguration putAuConfigItem(String key, String auConfigItem) {
    this.auConfig.put(StringPool.AU_CONFIG_PROPS.intern(key), auConfigItem);
    return this;
  }

  /**
   * The map of Archival Unit configuration items
   * @return auConfig
  **/
  @ApiModelProperty(required = true, value = "The map of Archival Unit configuration items")
  @NotNull


  public Map<String, String> getAuConfig() {
    // Validation.
    if (auId == null || auId.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid Archival Unit identifier: '"
	  + auId + "'");
    }

    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '" + auConfig
	  + "'");
    }

    return auConfig;
  }

  public void setAuConfig(Map<String, String> auConfig) {
    // Validation.
    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '" + auConfig
	  + "'");
    }

    this.auConfig = auConfig;
  }


  public String get(String key) {
    if (auConfig == null || auConfig.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '" + auConfig
	  + "'");
    }

    return auConfig.get(key);
  }

  public boolean getBoolean(String key, boolean dfault) {
    String val = get(key);
    if (StringUtil.isNullString(val)) {
      return dfault;
    }
    Boolean bool = Configuration.stringToBool(val);
    if (bool != null) {
      return bool.booleanValue();
    }
    return dfault;
  }

  /** Return true iff the AU config is not marked as deactivated */
  public boolean isActive() {
    return !getBoolean(PluginManager.AU_PARAM_DISABLED, false);
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuConfiguration auConfiguration = (AuConfiguration) o;
    return Objects.equals(this.auId, auConfiguration.auId) &&
        Objects.equals(this.auConfig, auConfiguration.auConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(auId, auConfig);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AuConfiguration {\n");
    
    sb.append("    auId: ").append(toIndentedString(auId)).append("\n");
    sb.append("    auConfig: ").append(toIndentedString(auConfig)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
