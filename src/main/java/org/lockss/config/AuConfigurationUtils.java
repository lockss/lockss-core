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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lockss.plugin.PluginManager;
import org.lockss.util.StringUtil;

public class AuConfigurationUtils {
  static String BACKUP_LINE_FIELD_SEPARATOR = "\t";

  /**
   * Provides a Configuration object, with unprefixed keys, equivalent to a
   * passed AuConfiguration object.
   * 
   * @param auConfiguration
   *          An AuConfiguration with the Archival Unit configuration
   *          properties.
   * @return a Configuration object, with unprefixed keys, that is equivalent to
   *         the passed AuConfiguration object.
   */
  public static Configuration toUnprefixedConfiguration(
      AuConfiguration auConfiguration) {
    Configuration result = ConfigManager.newConfiguration();

    for (String key : auConfiguration.getAuConfig().keySet()) {
      result.put(key, auConfiguration.getAuConfig().get(key));
    }

    return result;
  }

  /**
   * Provides an AuConfiguration object equivalent to a generic Configuration.
   * 
   * @param auPropKey
   *          A String with the key under which the Archival Unit configuration
   *          properties are listed.
   * @param configuration
   *          A Configuration with the Archival Unit configuration properties.
   * 
   * @return an AuConfiguration object that is equivalent to the passed generic
   *         Configuration.
   */
  public static AuConfiguration fromConfiguration(String auPropKey,
      Configuration configuration) {

    // Validation.
    if (auPropKey == null || auPropKey.trim().isEmpty()) {
      throw new IllegalArgumentException("Invalid Archival Unit key: '"
	  + auPropKey + "'");
    }

    if (configuration == null || configuration.isEmpty()) {
      throw new IllegalArgumentException("Invalid configuration: '"
	  + configuration + "'");
    }

    // Get the subtree of Archival Unit configuration properties.
    Configuration newConf = configuration.getConfigTree(auPropKey);

    // Save the Archival Unit configuration properties.
    Map<String, String> auConfig = new HashMap<String, String>();

    for (String key : newConf.keySet()) {
      auConfig.put(key, newConf.get(key));
    }

    return new AuConfiguration(PluginManager.auIdFromConfigPrefixAndKey(auPropKey),
                               auConfig)
      .intern();
  }

  /**
   * Provides a Configuration object, with keys prefixed by the Archival Unit
   * identifier, equivalent to a passed AuConfiguration object.
   * 
   * @param auConfiguration
   *          An AuConfiguration with the Archival Unit configuration
   *          properties.
   * @return a Configuration object, with keys prefixed by the Archival Unit
   *         identifier, that is equivalent to the passed AuConfiguration
   *         object.
   */
  public static Configuration toAuidPrefixedConfiguration(AuConfiguration
      auConfiguration) {
    Configuration result = ConfigManager.newConfiguration();

    String configKey =
	PluginManager.configKeyFromAuId(auConfiguration.getAuId());

    for (String key : auConfiguration.getAuConfig().keySet()) {
      result.put(configKey + "." + key, auConfiguration.getAuConfig().get(key));
    }

    return result;
  }

  /**
   * Provides the configuration of an Archival Unit stored in a backup file
   * line.
   * 
   * @param line
   *          A String with the contents of the backup file line.
   * @return an AuConfiguration with the Archival Unit configuration.
   */
  public static AuConfiguration fromBackupLine(String line) {
    List<String> tokens = StringUtil.breakAt(line, BACKUP_LINE_FIELD_SEPARATOR);

    if (tokens == null || tokens.isEmpty() || (tokens.size() - 1) % 2 != 0) {
      throw new IllegalArgumentException("Invalid Archival Unit backup line: '"
	  + line + "'");
    }

    String auId = tokens.get(0);
    Map<String, String> props = new HashMap<>();

    for (int index = 1; index < tokens.size() - 1; index = index + 2) {
      props.put(tokens.get(index), tokens.get(index + 1));
    }

    return new AuConfiguration(auId, props).intern();
  }

  /**
   * Provides the backup file line that corresponds to an archival Unit
   * configuration.
   * 
   * @param auConfiguration
   *          An AuConfiguration with the Archival Unit configuration
   *          properties.
   * @return a String with the contents of the backup file line.
   */
  public static String toBackupLine(AuConfiguration auConfiguration) {
    // Write the Archival Unit identifier.
    StringBuilder sb = new StringBuilder(
	StringUtil.blankOutNlsAndTabs(auConfiguration.getAuId()));

    Map<String, String> auConfig = auConfiguration.getAuConfig();

    // Loop through all the configuration properties.
    for (String key : auConfig.keySet()) {
      // Write the property.
      sb.append(BACKUP_LINE_FIELD_SEPARATOR)
      .append(StringUtil.blankOutNlsAndTabs(key))
      .append(BACKUP_LINE_FIELD_SEPARATOR)
      .append(StringUtil.blankOutNlsAndTabs(auConfig.get(key)));
    }

    return sb.toString();
  }
}
