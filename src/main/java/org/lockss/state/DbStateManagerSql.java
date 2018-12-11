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
package org.lockss.state;

import static org.lockss.config.db.SqlConstants.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.config.db.ConfigManagerSql;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.state.AuState.AccessType;
import org.lockss.state.SubstanceChecker.State;
import org.lockss.util.time.TimeBase;

/**
 * The DbStateManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class DbStateManagerSql extends ConfigManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final String NULL_LAST_CRAWL_RESULT_MSG =
      "DbStateManagerSql-LastCrawlResult_Message.Is.Null";
  
  // Query to find all the state access types.
  private static final String GET_ALL_STATE_ACCESS_TYPE_QUERY = "select "
      + STATE_ACCESS_TYPE_SEQ_COLUMN
      + "," + STATE_ACCESS_TYPE_COLUMN
      + " from " + STATE_ACCESS_TYPE_TABLE;

  // Query to find all the has substance kinds.
  private static final String GET_ALL_STATE_HAS_SUBSTANCE_QUERY = "select "
      + STATE_HAS_SUBSTANCE_SEQ_COLUMN
      + "," + STATE_HAS_SUBSTANCE_COLUMN
      + " from " + STATE_HAS_SUBSTANCE_TABLE;

  // Query to find the state of an Archival Unit.
  private static final String GET_AU_STATE_QUERY = "select "
      + "a." + ARCHIVAL_UNIT_SEQ_COLUMN
      + ", a." + CREATION_TIME_COLUMN
      + ", s." + LAST_CRAWL_TIME_COLUMN
      + ", s." + LAST_CRAWL_ATTEMPT_COLUMN
      + ", lcrm." + STATE_LAST_CRAWL_RESULT_MSG_COLUMN
      + ", s." + LAST_CRAWL_RESULT_COLUMN
      + ", s." + LAST_TOP_LEVEL_POLL_TIME_COLUMN
      + ", s." + LAST_POLL_START_COLUMN
      + ", s." + LAST_POLL_RESULT_COLUMN
      + ", s." + POLL_DURATION_COLUMN
      + ", s." + AVERAGE_HASH_DURATION_COLUMN
      + ", s." + V3_AGREEMENT_COLUMN
      + ", s." + HIGHEST_V3_AGREEMENT_COLUMN
      + ", s." + STATE_ACCESS_TYPE_SEQ_COLUMN
      + ", s." + STATE_HAS_SUBSTANCE_SEQ_COLUMN
      + ", s." + SUBSTANCE_VERSION_COLUMN
      + ", s." + METADATA_VERSION_COLUMN
      + ", s." + LAST_METADATA_INDEX_COLUMN
      + ", s." + LAST_CONTENT_CHANGE_COLUMN
      + ", s." + LAST_POP_POLL_COLUMN
      + ", s." + LAST_POP_POLL_RESULT_COLUMN
      + ", s." + LAST_LOCAL_HASH_SCAN_COLUMN
      + ", s." + NUM_AGREE_PEERS_LAST_POR_COLUMN
      + ", s." + NUM_WILLING_REPAIRERS_COLUMN
      + ", s." + NUM_CURRENT_SUSPECT_VERSIONS_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_STATE_TABLE + " s"
      + ", " + STATE_LAST_CRAWL_RESULT_MSG_TABLE + " lcrm"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = s."
      + ARCHIVAL_UNIT_SEQ_COLUMN
      + " and s." + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN + " = lcrm."
      + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN;

  // Query to find the CDN stems of an Archival Unit.
  private static final String GET_AU_CDN_STEMS_QUERY = "select "
      + "scs." + STATE_CDN_STEM_COLUMN
      + ", aucs." + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN
      + " from " + STATE_CDN_STEM_TABLE + " scs"
      + ", " + ARCHIVAL_UNIT_CDN_STEM_TABLE + " aucs"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + " where a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = aucs."
      + ARCHIVAL_UNIT_SEQ_COLUMN
      + " and aucs." + STATE_CDN_STEM_SEQ_COLUMN + " = scs."
      + STATE_CDN_STEM_SEQ_COLUMN
      + " order by aucs." + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN + " asc";

  // Query to find a last crawl result message by its identifier.
  private static final String FIND_LAST_CRAWL_RESULT_MSG_QUERY = "select "
      + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN
      + " from " + STATE_LAST_CRAWL_RESULT_MSG_TABLE
      + " where " + STATE_LAST_CRAWL_RESULT_MSG_COLUMN + " = ?";

  // Query to add a last crawl result message.
  private static final String INSERT_LAST_CRAWL_RESULT_MSG_QUERY =
      "insert into " + STATE_LAST_CRAWL_RESULT_MSG_TABLE
      + "(" + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN
      + "," + STATE_LAST_CRAWL_RESULT_MSG_COLUMN
      + ") values (default,?)";

  // Query to find a CDN message by its identifier.
  private static final String FIND_CDN_STEM_QUERY = "select "
      + STATE_CDN_STEM_SEQ_COLUMN
      + " from " + STATE_CDN_STEM_TABLE
      + " where " + STATE_CDN_STEM_COLUMN + " = ?";

  // Query to add a CDN stem.
  private static final String INSERT_CDN_STEM_QUERY =
      "insert into " + STATE_CDN_STEM_TABLE
      + "(" + STATE_CDN_STEM_SEQ_COLUMN
      + "," + STATE_CDN_STEM_COLUMN
      + ") values (default,?)";

  // Query to add a metadata item author.
  private static final String INSERT_ARCHIVAL_UNIT_CDN_STEM_QUERY =
      "insert into " + ARCHIVAL_UNIT_CDN_STEM_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + STATE_CDN_STEM_SEQ_COLUMN
      + "," + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN
      + ") values (?,?,("
      + "select coalesce(max(" + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN + "), 0) + 1"
      + " from " + ARCHIVAL_UNIT_CDN_STEM_TABLE
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?"
      + " and " + STATE_CDN_STEM_SEQ_COLUMN + " = ?))";
  
  // Query to add a state of an Archival Unit.
  private static final String ADD_AU_STATE_QUERY = "insert into "
      + ARCHIVAL_UNIT_STATE_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + LAST_CRAWL_TIME_COLUMN
      + "," + LAST_CRAWL_ATTEMPT_COLUMN
      + "," + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN
      + "," + LAST_CRAWL_RESULT_COLUMN
      + "," + LAST_TOP_LEVEL_POLL_TIME_COLUMN
      + "," + LAST_POLL_START_COLUMN
      + "," + LAST_POLL_RESULT_COLUMN
      + "," + POLL_DURATION_COLUMN
      + "," + AVERAGE_HASH_DURATION_COLUMN
      + "," + V3_AGREEMENT_COLUMN
      + "," + HIGHEST_V3_AGREEMENT_COLUMN
      + "," + STATE_ACCESS_TYPE_SEQ_COLUMN
      + "," + STATE_HAS_SUBSTANCE_SEQ_COLUMN
      + "," + SUBSTANCE_VERSION_COLUMN
      + "," + METADATA_VERSION_COLUMN
      + "," + LAST_METADATA_INDEX_COLUMN
      + "," + LAST_CONTENT_CHANGE_COLUMN
      + "," + LAST_POP_POLL_COLUMN
      + "," + LAST_POP_POLL_RESULT_COLUMN
      + "," + LAST_LOCAL_HASH_SCAN_COLUMN
      + "," + NUM_AGREE_PEERS_LAST_POR_COLUMN
      + "," + NUM_WILLING_REPAIRERS_COLUMN
      + "," + NUM_CURRENT_SUSPECT_VERSIONS_COLUMN
      + ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

  private Map<Long, String> accessTypeNameByPk = new HashMap<Long, String>();
  private Map<String, Long> accessTypePkByName = new HashMap<String, Long>();
  private Map<Long, String> hasSubstanceNameByPk = new HashMap<Long, String>();
  private Map<String, Long> hasSubstancePkByName = new HashMap<String, Long>();

  /**
   * Constructor.
   * 
   * @param configDbManager
   *          A ConfigDbManager with the database manager.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public DbStateManagerSql(ConfigDbManager configDbManager) throws DbException {
    super(configDbManager);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      populateAccessTypeMaps(conn);
      populateHasSubstanceMaps(conn);
    } catch (DbException dbe) {
      String message = "Cannot populate maps from database enums";
      log.error(message, dbe);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Populates the Access Type maps from the contents in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void populateAccessTypeMaps(Connection conn) throws DbException {
    log.debug2("Invoked");

    PreparedStatement getAccessTypes = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get state access types";

    try {
      // Prepare the query.
      getAccessTypes = configDbManager.prepareStatement(conn,
	  GET_ALL_STATE_ACCESS_TYPE_QUERY);

      // Get the state access types.
      resultSet = configDbManager.executeQuery(getAccessTypes);

      // Loop while there are more results.
      while (resultSet.next()) {
	// Get the state access type identifier of this result.
	Long accessTypeSeq = resultSet.getLong(STATE_ACCESS_TYPE_SEQ_COLUMN);
	log.trace("accessTypeSeq = {}", accessTypeSeq);
 
	// Get the state access type of this result.
	String accessType = resultSet.getString(STATE_ACCESS_TYPE_COLUMN);
	log.trace("accessType = {}", accessType);
 
  	// Save them.
	accessTypeNameByPk.put(accessTypeSeq, accessType);
	accessTypePkByName.put(accessType, accessTypeSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_ALL_STATE_ACCESS_TYPE_QUERY);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_ALL_STATE_ACCESS_TYPE_QUERY);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAccessTypes);
    }

    log.debug2("Done");
  }

  /**
   * Populates the SubstanceChecker State maps from the contents in the
   * database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void populateHasSubstanceMaps(Connection conn) throws DbException {
    log.debug2("Invoked");

    PreparedStatement getHasSubstanceKinds = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get state substance kinds";

    try {
      // Prepare the query.
      getHasSubstanceKinds = configDbManager.prepareStatement(conn,
	  GET_ALL_STATE_HAS_SUBSTANCE_QUERY);

      // Get the state has substance kinds.
      resultSet = configDbManager.executeQuery(getHasSubstanceKinds);

      // Loop while there are more results.
      while (resultSet.next()) {
	// Get the state substance kind identifier of this result.
	Long hasSubstanceSeq =
	    resultSet.getLong(STATE_HAS_SUBSTANCE_SEQ_COLUMN);
	log.trace("hasSubstanceSeq = {}", hasSubstanceSeq);
 
	// Get the state substance kind of this result.
	String hasSubstance = resultSet.getString(STATE_HAS_SUBSTANCE_COLUMN);
	log.trace("hasSubstance = {}", hasSubstance);
 
  	// Save them.
	hasSubstanceNameByPk.put(hasSubstanceSeq, hasSubstance);
	hasSubstancePkByName.put(hasSubstance, hasSubstanceSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_ALL_STATE_HAS_SUBSTANCE_QUERY);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_ALL_STATE_HAS_SUBSTANCE_QUERY);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getHasSubstanceKinds);
    }

    log.debug2("Done");
  }

  /**
   * Provides the state of an Archival Unit stored in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Map<String, Object> with the Archival Unit state properties.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Object> findArchivalUnitState(String pluginId,
      String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the state of the Archival Unit, if it exists.
      return findArchivalUnitState(conn, pluginId, auKey);
    } catch (DbException dbe) {
      String message = "Cannot find AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the state of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Map<String, Object> with the Archival Unit state properties.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Object> findArchivalUnitState(Connection conn,
      String pluginId, String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Map<String, Object> result = null;
    PreparedStatement getAuState = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU state";

    try {
      // Prepare the query.
      getAuState =
	  configDbManager.prepareStatement(conn, GET_AU_STATE_QUERY);

      // Populate the query.
      getAuState.setString(1, pluginId);
      getAuState.setString(2, auKey);

      // Get the configuration of the Archival Unit.
      resultSet = configDbManager.executeQuery(getAuState);

      // Get the single result, if any.
      if (resultSet.next()) {
	result = new HashMap<>();

	// Get the Archival Unit creation time.
  	long auCreationTime = resultSet.getLong(CREATION_TIME_COLUMN);
  	log.trace("auCreationTime = {}", auCreationTime);
  	result.put("auCreationTime", auCreationTime);

	// Get the last crawl time.
  	long lastCrawlTime = resultSet.getLong(LAST_CRAWL_TIME_COLUMN);
  	log.trace("lastCrawlTime = {}", lastCrawlTime);
  	result.put("lastCrawlTime", lastCrawlTime);

	// Get the last crawl attempt.
  	long lastCrawlAttempt = resultSet.getLong(LAST_CRAWL_ATTEMPT_COLUMN);
  	log.trace("lastCrawlAttempt = {}", lastCrawlAttempt);
  	result.put("lastCrawlAttempt", lastCrawlAttempt);

  	// Get the last crawl result message.
  	String lastCrawlResultMessage =
  	    resultSet.getString(STATE_LAST_CRAWL_RESULT_MSG_COLUMN);
  	log.trace("lastCrawlResultMessage = {}", lastCrawlResultMessage);

  	if (!NULL_LAST_CRAWL_RESULT_MSG.equals(lastCrawlResultMessage)) {
  	  result.put("lastCrawlResultMsg", lastCrawlResultMessage);
  	}

	// Get the last crawl result.
  	int lastCrawlResult = resultSet.getInt(LAST_CRAWL_RESULT_COLUMN);
  	log.trace("lastCrawlResult = {}", lastCrawlResult);
  	result.put("lastCrawlResult", lastCrawlResult);

	// Get the last top level poll time.
  	long lastTopLevelPollTime =
  	    resultSet.getLong(LAST_TOP_LEVEL_POLL_TIME_COLUMN);
  	log.trace("lastTopLevelPollTime = {}", lastTopLevelPollTime);
  	result.put("lastTopLevelPollTime", lastTopLevelPollTime);

	// Get the last poll start.
  	long lastPollStart = resultSet.getLong(LAST_POLL_START_COLUMN);
  	log.trace("lastPollStart = {}", lastPollStart);
  	result.put("lastPollStart", lastPollStart);

	// Get the last poll result.
  	int lastPollResult = resultSet.getInt(LAST_POLL_RESULT_COLUMN);
  	log.trace("lastPollResult = {}", lastPollResult);
  	result.put("lastPollResult", lastPollResult);

	// Get the poll duration.
  	long pollDuration = resultSet.getLong(POLL_DURATION_COLUMN);
  	log.trace("pollDuration = {}", pollDuration);
  	result.put("pollDuration", pollDuration);

	// Get the average hash duration.
  	long averageHashDuration =
  	    resultSet.getLong(AVERAGE_HASH_DURATION_COLUMN);
  	log.trace("averageHashDuration = {}", averageHashDuration);
  	result.put("averageHashDuration", averageHashDuration);

	// Get the V3 agreement.
  	double v3Agreement = resultSet.getDouble(V3_AGREEMENT_COLUMN);
  	log.trace("v3Agreement = {}", v3Agreement);
  	result.put("v3Agreement", v3Agreement);

	// Get the highest V3 agreement.
  	double highestV3Agreement =
  	    resultSet.getDouble(HIGHEST_V3_AGREEMENT_COLUMN);
  	log.trace("highestV3Agreement = {}", highestV3Agreement);
  	result.put("highestV3Agreement", highestV3Agreement);

	// Get the access type.
  	long accessTypeSeq = resultSet.getLong(STATE_ACCESS_TYPE_SEQ_COLUMN);
  	log.trace("accessTypeSeq = {}", accessTypeSeq);
  	String accessTypeText = accessTypeNameByPk.get(accessTypeSeq);
  	log.trace("accessTypeText = {}", accessTypeText);

  	if (!STATE_ACCESS_TYPE_NOT_SET.equals(accessTypeText)) {
  	  AccessType accessType =
  	      AccessType.valueOf(accessTypeText);
  	  log.trace("accessType = {}", accessType);
  	  result.put("accessType", accessType);
  	}

	// Get the substance kind.
  	long hasSubstanceSeq =
  	    resultSet.getLong(STATE_HAS_SUBSTANCE_SEQ_COLUMN);
  	log.trace("hasSubstanceSeq = {}", hasSubstanceSeq);
  	String hasSubstanceText = hasSubstanceNameByPk.get(hasSubstanceSeq);
  	log.trace("hasSubstanceText = {}", hasSubstanceText);
  	State hasSubstance = State.valueOf(hasSubstanceText);
  	  log.trace("hasSubstance = {}", hasSubstance);
  	result.put("hasSubstance", hasSubstance);

	// Get the substance version.
  	String substanceVersion = resultSet.getString(SUBSTANCE_VERSION_COLUMN);
  	log.trace("substanceVersion = {}", substanceVersion);

  	if (substanceVersion != null) {
  	  result.put("substanceVersion", substanceVersion);
  	}

	// Get the metadata version.
  	String metadataVersion = resultSet.getString(METADATA_VERSION_COLUMN);
  	log.trace("metadataVersion = {}", metadataVersion);

  	if (metadataVersion != null) {
  	  result.put("metadataVersion", metadataVersion);
  	}

	// Get the last metadata index.
  	long lastMetadataIndex = resultSet.getLong(LAST_METADATA_INDEX_COLUMN);
  	log.trace("lastMetadataIndex = {}", lastMetadataIndex);
  	result.put("lastMetadataIndex", lastMetadataIndex);

	// Get the last content change.
  	long lastContentChange = resultSet.getLong(LAST_CONTENT_CHANGE_COLUMN);
  	log.trace("lastContentChange = {}", lastContentChange);
  	result.put("lastContentChange", lastContentChange);

	// Get the last PoP poll.
  	long lastPoPPoll = resultSet.getLong(LAST_POP_POLL_COLUMN);
  	log.trace("lastPoPPoll = {}", lastPoPPoll);
  	result.put("lastPoPPoll", lastPoPPoll);

	// Get the last PoP poll result.
  	int lastPoPPollResult = resultSet.getInt(LAST_POP_POLL_RESULT_COLUMN);
  	log.trace("lastPoPPollResult = {}", lastPoPPollResult);
  	result.put("lastPoPPollResult", lastPoPPollResult);

	// Get the last local hash scan.
  	long lastLocalHashScan = resultSet.getLong(LAST_LOCAL_HASH_SCAN_COLUMN);
  	log.trace("lastLocalHashScan = {}", lastLocalHashScan);
  	result.put("lastLocalHashScan", lastLocalHashScan);

	// Get the number of agreeing peers in the last PoR.
  	int numAgreePeersLastPoR =
  	    resultSet.getInt(NUM_AGREE_PEERS_LAST_POR_COLUMN);
  	log.trace("numAgreePeersLastPoR = {}", numAgreePeersLastPoR);
  	result.put("numAgreePeersLastPoR", numAgreePeersLastPoR);

	// Get the number of willing repairers.
  	int numWillingRepairers =
  	    resultSet.getInt(NUM_WILLING_REPAIRERS_COLUMN);
  	log.trace("numWillingRepairers = {}", numWillingRepairers);
  	result.put("numWillingRepairers", numWillingRepairers);

	// Get the number of URLs with current version suspect.
  	int numCurrentSuspectVersions =
  	    resultSet.getInt(NUM_CURRENT_SUSPECT_VERSIONS_COLUMN);
  	log.trace("numCurrentSuspectVersions = {}", numCurrentSuspectVersions);
  	result.put("numCurrentSuspectVersions", numCurrentSuspectVersions);

	// Get the Archival Unit sequential identifier.
  	long auSeq = resultSet.getLong(ARCHIVAL_UNIT_SEQ_COLUMN);
  	log.trace("auSeq = {}", auSeq);

  	// Get the CDN stems;
  	List<String> cdnStems = findArchivalUnitCdnStems(conn, auSeq);
  	log.trace("cdnStems = {}", cdnStems);
  	result.put("cdnStems", cdnStems);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_STATE_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_STATE_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuState);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides the CDN stems of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @return a List<String> with the CDN stems of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public List<String> findArchivalUnitCdnStems(Connection conn, Long auSeq)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);

    List<String> result = null;
    PreparedStatement getAuCdnStems = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU state";

    try {
      // Prepare the query.
      getAuCdnStems =
	  configDbManager.prepareStatement(conn, GET_AU_CDN_STEMS_QUERY);

      // Populate the query.
      getAuCdnStems.setLong(1, auSeq);

      // Get the Archival Unit CDN stems.
      resultSet = configDbManager.executeQuery(getAuCdnStems);

      // Loop through the results.
      while (resultSet.next()) {
	if (result == null) {
	  result = new ArrayList<>();
	}

	// Get the CDN stem.
  	String cdnStem = resultSet.getString(STATE_CDN_STEM_COLUMN);
  	log.trace("cdnStem = {}", cdnStem);

  	result.add(cdnStem);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_CDN_STEMS_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_CDN_STEMS_QUERY);
      log.error("auSeq = {}", auSeq);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuCdnStems);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the state of an Archival Unit.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param auStateProps
   *          A Map<String, Object> with the Archival Unit state properties.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitState(String pluginId, String auKey,
      Map<String, Object> auStateProps) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("auStateProps = {}", auStateProps);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the state of the Archival Unit, if it exists.
      Map<String, Object> existingAuState =
	  findArchivalUnitState(conn, pluginId, auKey);
      log.trace("existingAuState = {}", existingAuState);

      if (existingAuState != null) {
	String message = "Attempt to replace existing AuState";
	log.error(message);
	log.error("Existing AuState properties = '{}'", existingAuState);
	log.error("Replacement AuState properties = '{}'", auStateProps);
	log.error("pluginId = '{}, auKey = '{}'", pluginId, auKey);
	throw new IllegalStateException(message);
      }

      return addArchivalUnitState(conn, pluginId, auKey, auStateProps, true);
    } catch (DbException dbe) {
      String message = "Cannot add AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("auStateProps = {}", auStateProps);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Adds to the database the state of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param auStateProps
   *          A Map<String, Object> with the Archival Unit state properties.
   * @param commitAfterAdd
   *          A boolean with the indication of whether the addition should be
   *          committed in this method, or not.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitState(Connection conn, String pluginId,
      String auKey, Map<String, Object> auStateProps, boolean commitAfterAdd)
	  throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("auStateProps = {}", auStateProps);
    log.debug2("commitAfterAdd = {}", commitAfterAdd);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin, adding it if necessary.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

      // Add the new state of the Archival Unit.
      addArchivalUnitState(conn, auSeq, auStateProps);

      if (commitAfterAdd) {
	// Commit the transaction.
	ConfigDbManager.commitOrRollback(conn, log);
      }
    } catch (DbException dbe) {
      String message = "Cannot add AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("auStateProps = {}", auStateProps);
      log.error("commitAfterAdd = {}", commitAfterAdd);
      throw dbe;
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Adds to the database the state of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @param auStateProps
   *          A Map<String, Object> with the Archival Unit state properties.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitState(Connection conn, Long auSeq,
      Map<String, Object> originalAuStateProps) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("originalAuStateProps = {}", originalAuStateProps);

    Map<String, Object> auStateProps = fixAuStateTypes(originalAuStateProps);
    log.trace("auStateProps = {}", auStateProps);

    // Get the foreign key of the access type.
    String accessTypeText = (String)auStateProps.get("accessType");
    log.trace("accessTypeText = {}", accessTypeText);

    Long accessTypeSeq = null;

    if (accessTypeText == null) {
      accessTypeSeq = accessTypePkByName.get(STATE_ACCESS_TYPE_NOT_SET);
    } else {
      accessTypeSeq = accessTypePkByName.get(accessTypeText);
    }

    log.trace("accessTypeSeq = {}", accessTypeSeq);

    if (accessTypeSeq == null) {
      throw new IllegalStateException("Invalid state access type '"
	  + accessTypeText + "'");
    }

    // Get the foreign key of the has substance.
    String hasSubstanceText = (String)auStateProps.get("hasSubstance");
    log.trace("hasSubstanceText = {}", hasSubstanceText);

    Long hasSubstanceSeq = null;

    if (hasSubstanceText != null) {
      hasSubstanceSeq = hasSubstancePkByName.get(hasSubstanceText);
    }

    log.trace("hasSubstanceSeq = {}", hasSubstanceSeq);

    if (hasSubstanceSeq == null) {
      throw new IllegalStateException("Invalid substance checker state '"
	  + hasSubstanceText + "'");
    }

    // Get the foreign key of the last crawl result message.
    String lastCrawlResultMsg = (String)auStateProps.get("lastCrawlResultMsg");
    log.trace("lastCrawlResultMsg = {}", lastCrawlResultMsg);

    if (lastCrawlResultMsg == null) {
      lastCrawlResultMsg = NULL_LAST_CRAWL_RESULT_MSG;
    }

    Long lastCrawlResultMsgSeq =
	findOrCreateLastCrawlResultMsg(conn, lastCrawlResultMsg);

    // Add the CDN stems to the Archival Unit.
    List<String> cdnStems = (List<String>)auStateProps.get("cdnStems");
    log.trace("cdnStems = {}", cdnStems);

    if (cdnStems != null && !cdnStems.isEmpty()) {
      for (String cdnStem : cdnStems) {
	Long cdnStemSeq = findOrCreateCdnStem(conn, cdnStem);
	addArchivalUnitCdnStem(conn, auSeq, cdnStemSeq);
      }
    }

    PreparedStatement addState = null;
    String errorMessage = "Cannot add Archival Unit configuration";

    try {
      // Prepare the query.
      addState = configDbManager.prepareStatement(conn, ADD_AU_STATE_QUERY);

      // Populate the query.
      addState.setLong(1, auSeq);
      addState.setLong(2, (Long)auStateProps.get("lastCrawlTime"));
      addState.setLong(3, (Long)auStateProps.get("lastCrawlAttempt"));
      addState.setLong(4, lastCrawlResultMsgSeq);
      addState.setInt(5, (Integer)auStateProps.get("lastCrawlResult"));
      addState.setLong(6, (Long)auStateProps.get("lastTopLevelPollTime"));
      addState.setLong(7, (Long)auStateProps.get("lastPollStart"));
      addState.setInt(8, (Integer)auStateProps.get("lastPollResult"));
      addState.setLong(9, (Long)auStateProps.get("pollDuration"));
      addState.setLong(10, (Long)auStateProps.get("averageHashDuration"));
      addState.setDouble(11, (Double)auStateProps.get("v3Agreement"));
      addState.setDouble(12, (Double)auStateProps.get("highestV3Agreement"));
      addState.setLong(13, accessTypeSeq);
      addState.setLong(14, hasSubstanceSeq);

      String substanceVersion = (String)auStateProps.get("substanceVersion");

      if (substanceVersion == null) {
	addState.setNull(15, Types.VARCHAR);
      } else {
	addState.setString(15, substanceVersion);
      }

      String metadataVersion = (String)auStateProps.get("metadataVersion");

      if (metadataVersion == null) {
	addState.setNull(16, Types.VARCHAR);
      } else {
	addState.setString(16, metadataVersion);
      }

      addState.setLong(17, (Long)auStateProps.get("lastMetadataIndex"));
      addState.setLong(18, (Long)auStateProps.get("lastContentChange"));
      addState.setLong(19, (Long)auStateProps.get("lastPoPPoll"));
      addState.setInt(20, (Integer)auStateProps.get("lastPoPPollResult"));
      addState.setLong(21, (Long)auStateProps.get("lastLocalHashScan"));
      addState.setInt(22, (Integer)auStateProps.get("numAgreePeersLastPoR"));
      addState.setInt(23, (Integer)auStateProps.get("numWillingRepairers"));
      addState.setInt(24,
	  (Integer)auStateProps.get("numCurrentSuspectVersions"));

      int count = configDbManager.executeUpdate(addState);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("auStateProps = {}", auStateProps);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("auStateProps = {}", auStateProps);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addState);
    }
  }

  /**
   * Provides the database identifier of a last crawl result message if existing
   * or after creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param lastCrawlResultMsg
   *          A String with the last crawl result message.
   * @return a Long with the database identifier of the last crawl result
   *         message.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  protected Long findOrCreateLastCrawlResultMsg(Connection conn,
      String lastCrawlResultMsg) throws DbException {
    log.debug2("lastCrawlResultMsg = {}", lastCrawlResultMsg);

    // Find the last crawl result message in the database.
    Long lastCrawlResultMsgSeq =
	findLastCrawlResultMsg(conn, lastCrawlResultMsg);
    log.trace("lastCrawlResultMsgSeq = {}", lastCrawlResultMsgSeq);

    // Check whether it is a new last crawl result message.
    if (lastCrawlResultMsgSeq == null) {
      // Yes: Add to the database the new last crawl result message.
      lastCrawlResultMsgSeq = addLastCrawlResultMsg(conn, lastCrawlResultMsg);
      log.trace("new lastCrawlResultMsgSeq = {}", lastCrawlResultMsgSeq);
    }

    log.debug2("lastCrawlResultMsgSeq = {}", lastCrawlResultMsgSeq);
    return lastCrawlResultMsgSeq;
  }

  /**
   * Provides the database identifier of a last crawl result message.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param lastCrawlResultMsg
   *          A String with the last crawl result message.
   * @return a Long with the database identifier of the last crawl result
   *         message.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findLastCrawlResultMsg(Connection conn,
      String lastCrawlResultMsg) throws DbException {
    log.debug2("lastCrawlResultMsg = {}", lastCrawlResultMsg);
    Long lastCrawlResultMsgSeq = null;
    PreparedStatement findLastCrawlResultMsg = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find last crawl result message";

    try {
      // Prepare the query.
      findLastCrawlResultMsg = configDbManager.prepareStatement(conn,
	  FIND_LAST_CRAWL_RESULT_MSG_QUERY);

      // Populate the query.
      findLastCrawlResultMsg.setString(1, lastCrawlResultMsg);

      // Get the last crawl result message.
      resultSet = configDbManager.executeQuery(findLastCrawlResultMsg);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the last crawl result message database identifier.
	lastCrawlResultMsgSeq =
	    resultSet.getLong(STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_LAST_CRAWL_RESULT_MSG_QUERY);
      log.error("lastCrawlResultMsg = {}", lastCrawlResultMsg);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", FIND_LAST_CRAWL_RESULT_MSG_QUERY);
      log.error("lastCrawlResultMsg = {}", lastCrawlResultMsg);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findLastCrawlResultMsg);
    }

    log.debug2("lastCrawlResultMsgSeq = {}", lastCrawlResultMsgSeq);
    return lastCrawlResultMsgSeq;
  }

  /**
   * Adds a last crawl result message to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param lastCrawlResultMsg
   *          A String with the last crawl result message.
   * @return a Long with the database identifier of the last crawl result
   *         message just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long addLastCrawlResultMsg(Connection conn, String lastCrawlResultMsg)
      throws DbException {
    log.debug2("lastCrawlResultMsg = {}", lastCrawlResultMsg);

    Long lastCrawlResultMsgSeq = null;
    PreparedStatement insertLastCrawlResultMsg = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add last crawl result message";

    try {
      // Prepare the query.
      insertLastCrawlResultMsg = configDbManager.prepareStatement(conn,
	  INSERT_LAST_CRAWL_RESULT_MSG_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query. Skip auto-increment column #0.
      insertLastCrawlResultMsg.setString(1, lastCrawlResultMsg);

      // Add the last crawl result message.
      configDbManager.executeUpdate(insertLastCrawlResultMsg);
      resultSet = insertLastCrawlResultMsg.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
	// Yes: Report the problem.
	String message = "Unable to create last crawl result message table row"
	    + " for lastCrawlResultMsg = " + lastCrawlResultMsg;
	log.error(message);
	throw new DbException(message);
      }

      // No: Get the last crawl result message database identifier.
      lastCrawlResultMsgSeq = resultSet.getLong(1);
      log.trace("Added lastCrawlResultMsgSeq = {}", lastCrawlResultMsgSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_LAST_CRAWL_RESULT_MSG_QUERY);
      log.error("lastCrawlResultMsg = {}", lastCrawlResultMsg);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_LAST_CRAWL_RESULT_MSG_QUERY);
      log.error("lastCrawlResultMsg = {}", lastCrawlResultMsg);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertLastCrawlResultMsg);
    }

    log.debug2("lastCrawlResultMsgSeq = {}", lastCrawlResultMsgSeq);
    return lastCrawlResultMsgSeq;
  }

  /**
   * Provides the database identifier of a CDN stem if existing or after
   * creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param cdnStem
   *          A String with the CDN stem.
   * @return a Long with the database identifier of the CDN stem.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  protected Long findOrCreateCdnStem(Connection conn, String cdnStem)
      throws DbException {
    log.debug2("cdnStem = {}", cdnStem);

    // Find the CDN stem in the database.
    Long cdnStemSeq = findCdnStem(conn, cdnStem);
    log.trace("cdnStemSeq = {}", cdnStemSeq);

    // Check whether it is a new CDN stem.
    if (cdnStemSeq == null) {
      // Yes: Add to the database the new CDN stem.
      cdnStemSeq = addCdnStem(conn, cdnStem);
      log.trace("new cdnStemSeq = {}", cdnStemSeq);
    }

    log.debug2("cdnStemSeq = {}", cdnStemSeq);
    return cdnStemSeq;
  }

  /**
   * Provides the database identifier of a CDN stem.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param cdnStem
   *          A String with the CDN stem.
   * @return a Long with the database identifier of the CDN stem.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findCdnStem(Connection conn, String cdnStem) throws DbException {
    log.debug2("cdnStem = {}", cdnStem);
    Long cdnStemSeq = null;
    PreparedStatement findCdnStem = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find CDN stem";

    try {
      // Prepare the query.
      findCdnStem = configDbManager.prepareStatement(conn, FIND_CDN_STEM_QUERY);

      // Populate the query.
      findCdnStem.setString(1, cdnStem);

      // Get the CDN stem.
      resultSet = configDbManager.executeQuery(findCdnStem);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the CDN stem database identifier.
	cdnStemSeq = resultSet.getLong(STATE_CDN_STEM_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_CDN_STEM_QUERY);
      log.error("cdnStem = {}", cdnStem);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", FIND_CDN_STEM_QUERY);
      log.error("cdnStem = {}", cdnStem);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findCdnStem);
    }

    log.debug2("cdnStemSeq = {}", cdnStemSeq);
    return cdnStemSeq;
  }

  /**
   * Adds a CDN stem to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param cdnStem
   *          A String with the CDN stem.
   * @return a Long with the database identifier of the CDN stem just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long addCdnStem(Connection conn, String cdnStem) throws DbException {
    log.debug2("cdnStem = {}", cdnStem);

    Long cdnStemSeq = null;
    PreparedStatement insertCdnStem = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add CDN stem";

    try {
      // Prepare the query.
      insertCdnStem = configDbManager.prepareStatement(conn,
	  INSERT_CDN_STEM_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query. Skip auto-increment column #0.
      insertCdnStem.setString(1, cdnStem);

      // Add the CDN stem.
      configDbManager.executeUpdate(insertCdnStem);
      resultSet = insertCdnStem.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
	// Yes: Report the problem.
	String message =
	    "Unable to create CDN stem table row for cdnStem = " + cdnStem;
	log.error(message);
	throw new DbException(message);
      }

      // No: Get the CDN stem database identifier.
      cdnStemSeq = resultSet.getLong(1);
      log.trace("Added cdnStemSeq = {}", cdnStemSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_CDN_STEM_QUERY);
      log.error("cdnStem = {}", cdnStem);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_CDN_STEM_QUERY);
      log.error("cdnStem = {}", cdnStem);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertCdnStem);
    }

    log.debug2("cdnStemSeq = {}", cdnStemSeq);
    return cdnStemSeq;
  }

  /**
   * Adds a CDN stem to an archival Unit in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @param cdnStemSeq
   *          A Long with the database identifier of the CDN stem.
   * @return a Long with the database identifier of the CDN stem just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void addArchivalUnitCdnStem(Connection conn, Long auSeq,
      Long cdnStemSeq) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("cdnStemSeq = {}", cdnStemSeq);

    PreparedStatement insertAuCdnStem = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add CDN stem to Archival unit";

    try {
      // Prepare the query.
      insertAuCdnStem = configDbManager.prepareStatement(conn,
	  INSERT_ARCHIVAL_UNIT_CDN_STEM_QUERY);

      // Populate the query.
      insertAuCdnStem.setLong(1, auSeq);
      insertAuCdnStem.setLong(2, cdnStemSeq);
      insertAuCdnStem.setLong(3, auSeq);
      insertAuCdnStem.setLong(4, cdnStemSeq);

      // Add the CDN stem.
      int count = configDbManager.executeUpdate(insertAuCdnStem);
      log.trace("count = {}", count);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_ARCHIVAL_UNIT_CDN_STEM_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("cdnStemSeq = {}", cdnStemSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_ARCHIVAL_UNIT_CDN_STEM_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("cdnStemSeq = {}", cdnStemSeq);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertAuCdnStem);
    }

    log.debug2("Done");
  }

  /**
   * Sets the proper type of AuState properties in a map of objects.
   * @param originalAuStateProps A Map<String, Object> with the original map.
   * @return a Map<String, Object> with the fixed map.
   */
  private Map<String, Object> fixAuStateTypes(
      Map<String, Object> originalAuStateProps) {
    log.debug2("originalAuStateProps = {}", originalAuStateProps);

    Map<String, Object> result = new HashMap<>();

    for (String key : originalAuStateProps.keySet()) {
      log.trace("key = {}", key);
      Object value = originalAuStateProps.get(key);

      switch (key) {
        case "auCreationTime":
        case "lastCrawlTime":
        case "lastCrawlAttempt":
        case "lastTopLevelPollTime":
        case "lastPollStart":
        case "pollDuration":
        case "averageHashDuration":
        case "lastMetadataIndex":
        case "lastContentChange":
        case "lastPoPPoll":
        case "lastLocalHashScan":
          if (value instanceof Integer) {
            value = Long.valueOf(((Integer)value).longValue());
            log.trace("converted value = {}", value);
          } else if (value instanceof Long) {
            log.trace("It is already a Long");
          } else {
            throw new IllegalArgumentException("Unexpected type for key '" + key
        	+ "': " + value.getClass());
          }
          break;
        default:
      }

      result.put(key, value);
    }

    return result;
  }
}
