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
import org.lockss.db.DbException;
import org.lockss.log.*;
import org.lockss.util.*;

/** interface between ConfigManager and persistent config store
 * implementations.
 */
public interface ConfigStore {

  /**
   * Adds to the database the configuration of an Archival Unit.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param auConfig
   *          A Map<String,String> with the Archival Unit configuration.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitConfiguration(String pluginId,
					   String auKey,
					   Map<String,String> auConfig)
      throws DbException;

  /**
   * Adds to the database the configuration of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param auConfig
   *          A Map<String,String> with the Archival Unit configuration.
   * @param commitAfterAdd
   *          A boolean with the indication of whether the addition should be
   *          committed in this method, or not.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitConfiguration(Connection conn,
					   String pluginId,
					   String auKey,
					   Map<String,String> auConfig,
					   boolean commitAfterAdd)
      throws DbException;

  /**
   * Provides all the Archival Unit configurations stored in the database.
   * 
   * @return a Map<String, Map<String,String>> with all the Archival Unit
   *         configurations, keyed by each Archival Unit identifier.
   * @throws IOException
   *           if there are problems writing to the output stream
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Map<String,String>> findAllArchivalUnitConfiguration()
      throws IOException, DbException;

  /**
   * Processes all the Archival Unit configurations stored in the database,
   * either returning them or writing them to an aoutput stream.
   * 
   * @param outputStream
   *          An OutputStream where to write the configurations of all the
   *          Archival Units stored in the database, or <code>null</code> if the
   *          configurations are to be returned, instead.
   * @return a Map<String, Map<String,String>> with all the Archival Unit
   *         configurations, keyed by each Archival Unit identifier, if no
   *         output stream is passed.
   * @throws IOException
   *           if there are problems writing to the output stream
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Map<String,String>>
    processAllArchivalUnitConfigurations(OutputStream outputStream)
      throws IOException, DbException;

  /**
   * Provides the configuration of an Archival Unit stored in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Map<String,String> with the Archival Unit configurations.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String,String> findArchivalUnitConfiguration(String pluginId,
							  String auKey)
      throws DbException;

  /**
   * Provides the configuration of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Map<String, String> with the Archival Unit configurations.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String,String> findArchivalUnitConfiguration(Connection conn,
							  String pluginId,
							  String auKey)
      throws DbException;

  /**
   * Provides the configuration creation time of an Archival Unit stored in the
   * database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Long with the Archival Unit configuration creation time, as epoch
   *         milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findArchivalUnitCreationTime(String pluginId, String auKey)
      throws DbException;

  /**
   * Provides the configuration last update time of an Archival Unit stored in
   * the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Long with the Archival Unit configuration last update time, as
   *         epoch milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findArchivalUnitLastUpdateTime(String pluginId, String auKey)
      throws DbException;

  /**
   * Removes from the database an Archival Unit.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return an int with the count of database rows removed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public int removeArchivalUnit(String pluginId, String auKey)
      throws DbException;

  /**
   * Provides all the plugin identifiers stored in the database.
   * 
   * @return a Collection<String> with all the plugin identifiers.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<String> findAllPluginIds() throws DbException;

  /**
   * Provides the Archival Unit configurations for a plugin that are stored in
   * the database.
   * 
   * @param pluginId
   *          A String with the plugin identifier.
   * @return a Map<String, Map<String, String>> with the Archival Unit
   *         configurations for the plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Map<String,String>>
    findPluginAuConfigurations(String pluginId)
      throws DbException;

  public Connection getConnection() throws DbException;

  public void safeRollbackAndClose(Connection conn);

  public void commitOrRollback(Connection conn, L4JLogger logger)
      throws DbException;
  public void commitOrRollback(Connection conn, Logger logger)
      throws DbException;
  
}
