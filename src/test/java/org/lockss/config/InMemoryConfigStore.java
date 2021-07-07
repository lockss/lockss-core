/*

Copyright (c) 2018-2019 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.stream.*;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.PluginManager;
import org.lockss.util.Logger;
import org.lockss.util.time.TimeBase;

/**
 * In-memory implementation of ConfigStore, for testing
 */
public class InMemoryConfigStore implements ConfigStore {
  private static L4JLogger log = L4JLogger.getLogger();

  private Map<String, Map<String,String>> storedAuConfigs = new HashMap<>();
  private Map<String, Long> createTime = new HashMap<>();
  private Map<String, Long> updateTime = new HashMap<>();


  public InMemoryConfigStore() {
  }

  @Override
  public Connection getConnection() throws DbException {
    return null;
  }

  @Override
  public void safeRollbackAndClose(Connection conn) { }
  @Override
  public void commitOrRollback(Connection conn, L4JLogger logger) { }
  @Override
  public void commitOrRollback(Connection conn, Logger logger) { }


  public Long addArchivalUnitConfiguration(String auid,
					   Map<String,String> auConfig)
      throws DbException {
    return addArchivalUnitConfiguration(PluginManager.pluginIdFromAuId(auid),
					PluginManager.auKeyFromAuId(auid),
					auConfig);
  }

  @Override
  public Long addArchivalUnitConfiguration(String pluginId,
					   String auKey,
					   Map<String,String> auConfig)
      throws DbException {
    return addArchivalUnitConfiguration(null, pluginId, auKey, auConfig, true);
  }

  @Override
  public Long addArchivalUnitConfiguration(Connection conn,
					   String pluginId,
					   String auKey,
					   Map<String,String> auConfig,
					   boolean commitAfterAdd)
      throws DbException {

    log.debug2("addArchivalUnitConfiguration({}): {}",
	       auid(pluginId, auKey), auConfig);
    storedAuConfigs.put(auid(pluginId, auKey), auConfig);
    return 1L;
  }

  @Override
  public Map<String, Map<String,String>> findAllArchivalUnitConfiguration()
      throws IOException, DbException {
    return storedAuConfigs;
  }

  @Override
  public Map<String, Map<String,String>>
    processAllArchivalUnitConfigurations(OutputStream outputStream)
      throws IOException, DbException {

    if (outputStream == null) {
      return findAllArchivalUnitConfiguration();
    } else {
      for (Map.Entry<String, Map<String,String>> ent :
	     storedAuConfigs.entrySet()) {
	writeAuConfig(ent.getKey(), ent.getValue(), outputStream);
      }
      return null;
    }
  }

  private void writeAuConfig(String auId,
			     Map<String,String> auConfiguration,
			     OutputStream outputStream)
      throws IOException {
    log.debug2("auId = {}", auId);
    log.debug2("auConfiguration = {}", auConfiguration);

    // Validation.
    if (auId == null || auId.trim().isEmpty()) {
      log.warn("Configuration for null/empty AUId not added to backup file.");
      return;
    }

    if (auConfiguration == null || auConfiguration.isEmpty()) {
      log.warn("Null/empty AU configuration not added to backup file.");
      return;
    }

    // Create the entry.
    String line
      = AuConfigurationUtils.toBackupLine(new AuConfiguration(auId,
							      auConfiguration));
    log.trace("line = {}", line);

    // Write the entry to the output stream.
    Writer writer = new OutputStreamWriter(outputStream);
    writer.write(line + System.lineSeparator());
    writer.flush();

    log.debug2("Done");
  }

  @Override
  public Map<String,String> findArchivalUnitConfiguration(String pluginId,
							  String auKey)
      throws DbException {

    return storedAuConfigs.getOrDefault(auid(pluginId, auKey), new HashMap());
  }

  @Override
  public Map<String,String> findArchivalUnitConfiguration(Connection conn,
							  String pluginId,
							  String auKey)
      throws DbException {
    return findArchivalUnitConfiguration(pluginId, auKey);
  }

  @Override
  public Long findArchivalUnitCreationTime(String pluginId, String auKey)
      throws DbException {
    return createTime.get(auid(pluginId, auKey));
  }

  @Override
  public Long findArchivalUnitLastUpdateTime(String pluginId, String auKey)
      throws DbException {
    return updateTime.get(auid(pluginId, auKey));
  }

  @Override
  public int removeArchivalUnit(String pluginId, String auKey)
      throws DbException {
    String auid = auid(pluginId, auKey);
    Map<String, String> old = storedAuConfigs.get(auid);
    int res = -1;
    if (old != null) {
      res = old.size();
      storedAuConfigs.remove(auid);
    }
    return res;
  }

  @Override
  public Collection<String> findAllPluginIds() throws DbException {
    return storedAuConfigs.keySet().stream()
      .map(x -> PluginManager.pluginIdFromAuId(x))
      .collect(Collectors.toSet());
  }

  @Override
  public Map<String, Map<String,String>>
    findPluginAuConfigurations(String pluginId)
      throws DbException {
    return storedAuConfigs.entrySet().stream()
      .filter(e -> PluginManager.pluginIdFromAuId(e.getKey()).equals(pluginId))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  String auid(String pluginId, String auKey) {
    return PluginManager.generateAuId(pluginId, auKey);
  }

}
