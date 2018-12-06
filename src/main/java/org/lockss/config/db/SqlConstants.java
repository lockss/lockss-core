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
package org.lockss.config.db;

/**
 * Constants used in SQL code used to access the configuration database.
 * 
 * @author Fernando García-Loygorri
 */
public class SqlConstants {
  //
  // Database table names.
  //
  /** Name of the plugin table. */
  public static final String PLUGIN_TABLE = "plugin";

  /** Name of the archival unit table. */
  public static final String ARCHIVAL_UNIT_TABLE = "archival_unit";

  /** Name of the archival unit configuration table. */
  public static final String ARCHIVAL_UNIT_CONFIG_TABLE =
      "archival_unit_config";

  /** Name of the archival unit state access type values table. */
  public static final String STATE_ACCESS_TYPE_TABLE = "state_access_type";

  /** Name of the archival unit state has substance values table. */
  public static final String STATE_HAS_SUBSTANCE_TABLE = "state_has_substance";

  /** Name of the archival unit state last crawl result message de-duplicated
   * values table. */
  public static final String STATE_LAST_CRAWL_RESULT_MSG_TABLE =
      "state_last_crawl_result_msg";

  /** Name of the archival unit state CDN stem de-duplicated values table. */
  public static final String STATE_CDN_STEM_TABLE = "state_cdn_stem";

  /** Name of the archival unit CDN stem relationship table. */
  public static final String ARCHIVAL_UNIT_CDN_STEM_TABLE =
      "archival_unit_cdn_stem";

  /** Name of the archival unit state table. */
  public static final String ARCHIVAL_UNIT_STATE_TABLE = "archival_unit_state";

  //
  // Database table column names.
  //
  /** Plugin sequential identifier column. */
  public static final String PLUGIN_SEQ_COLUMN = "plugin_seq";

  /** Name of plugin_id column. */
  public static final String PLUGIN_ID_COLUMN = "plugin_id";

  /** Archival unit sequential identifier column. */
  public static final String ARCHIVAL_UNIT_SEQ_COLUMN = "archival_unit_seq";

  /** Name of archival unit_key column. */
  public static final String ARCHIVAL_UNIT_KEY_COLUMN = "archival_unit_key";

  /** Archival Unit creation time column. */
  public static final String CREATION_TIME_COLUMN = "creation_time";

  /** Archival Unit last update time column. */
  public static final String LAST_UPDATE_TIME_COLUMN = "last_update_time";

  /** Name of archival unit configuration key column. */
  public static final String CONFIG_KEY_COLUMN = "config_key";

  /** Name of archival unit configuration value column. */
  public static final String CONFIG_VALUE_COLUMN = "config_value";

  /** State access type value sequential identifier column. */
  public static final String STATE_ACCESS_TYPE_SEQ_COLUMN = "access_type_seq";

  /** State access type value column. */
  public static final String STATE_ACCESS_TYPE_COLUMN = "access_type";

  /** State has substance value sequential identifier column. */
  public static final String STATE_HAS_SUBSTANCE_SEQ_COLUMN =
      "has_substance_seq";

  /** State has substance value column. */
  public static final String STATE_HAS_SUBSTANCE_COLUMN = "has_substance";

  /** State last crawl result message value sequential identifier column. */
  public static final String STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN =
      "last_crawl_result_message_seq";

  /** State last crawl result message value column. */
  public static final String STATE_LAST_CRAWL_RESULT_MSG_COLUMN =
      "last_crawl_result_message";

  /** State CDN stem value sequential identifier column. */
  public static final String STATE_CDN_STEM_SEQ_COLUMN = "cdn_stem_seq";

  /** State CDN stem value column. */
  public static final String STATE_CDN_STEM_COLUMN = "cdn_stem";

  /** State Archival Unit CDN stem index column. */
  public static final String ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN =
      "archival_unit_cdn_stem_idx";

  /** State last crawl time column. */
  public static final String LAST_CRAWL_TIME_COLUMN = "last_crawl_time";

  /** State last crawl attempt column. */
  public static final String LAST_CRAWL_ATTEMPT_COLUMN = "last_crawl_attempt";

  /** State last crawl result column. */
  public static final String LAST_CRAWL_RESULT_COLUMN = "last_crawl_result";

  /** State last top level poll time column. */
  public static final String LAST_TOP_LEVEL_POLL_TIME_COLUMN =
      "last_top_level_poll_time";

  /** State last poll start column. */
  public static final String LAST_POLL_START_COLUMN = "last_poll_start";

  /** State last poll result column. */
  public static final String LAST_POLL_RESULT_COLUMN = "last_poll_result";

  /** State poll duration column. */
  public static final String POLL_DURATION_COLUMN = "poll_duration";

  /** State average hash duration column. */
  public static final String AVERAGE_HASH_DURATION_COLUMN =
      "average_hash_duration";

  /** State V3 agreement column. */
  public static final String V3_AGREEMENT_COLUMN = "v3_agreement";

  /** State highest V3 agreement column. */
  public static final String HIGHEST_V3_AGREEMENT_COLUMN =
      "highest_v3_agreement";

  /** State substance version column. */
  public static final String SUBSTANCE_VERSION_COLUMN = "substance_version";

  /** State metadata version column. */
  public static final String METADATA_VERSION_COLUMN = "metadata_version";

  /** State last metadata index column. */
  public static final String LAST_METADATA_INDEX_COLUMN = "last_metadata_index";

  /** State last content change column. */
  public static final String LAST_CONTENT_CHANGE_COLUMN = "last_content_change";

  /** State last PoP poll column. */
  public static final String LAST_POP_POLL_COLUMN = "last_pop_poll";

  /** State last PoP poll result column. */
  public static final String LAST_POP_POLL_RESULT_COLUMN =
      "last_pop_poll_result";

  /** State last local hash scan column. */
  public static final String LAST_LOCAL_HASH_SCAN_COLUMN =
      "last_local_hash_scan";

  /** State number of agreement peers in last PoR column. */
  public static final String NUM_AGREE_PEERS_LAST_POR_COLUMN =
      "num_agree_peers_last_por";

  /** State number of willing repairers column. */
  public static final String NUM_WILLING_REPAIRERS_COLUMN =
      "num_willing_repairers";

  /** State number of current suspect versions column. */
  public static final String NUM_CURRENT_SUSPECT_VERSIONS_COLUMN =
      "num_current_suspect_versions";

  //
  // Maximum lengths of variable text length database columns.
  //
  public static final int MAX_PLUGIN_ID_COLUMN = 256;

  /** Length of the archival unit key column. */
  public static final int MAX_ARCHIVAL_UNIT_KEY_COLUMN = 512;

  /** Length of the archival unit configuration key column. */
  public static final int MAX_CONFIG_KEY_COLUMN = 128;

  /** Length of the archival unit configuration value column. */
  public static final int MAX_CONFIG_VALUE_COLUMN = 4096;

  /** Length of the state access type value column. */
  public static final int MAX_STATE_ACCESS_TYPE_COLUMN = 32;

  /** Length of the state has substance value column. */
  public static final int MAX_STATE_HAS_SUBSTANCE_COLUMN = 32;

  /** Length of the state last crawl result message value column. */
  public static final int MAX_STATE_LAST_CRAWL_RESULT_MSG_COLUMN = 512;

  /** Length of the state substance version column. */
  public static final int MAX_SUBSTANCE_VERSION_COLUMN = 8;

  /** Length of the state metadata version column. */
  public static final int MAX_METADATA_VERSION_COLUMN = 8;

  /** Length of the state CDN stem value column. */
  public static final int MAX_STATE_CDN_STEM_COLUMN = 270;

  //
  //Types of state access type items.
  //
  public static final String STATE_ACCESS_TYPE_NOT_SET = "NotSet";
  public static final String STATE_ACCESS_TYPE_OPEN_ACCESS = "OpenAccess";
  public static final String STATE_ACCESS_TYPE_SUBSCRIPTION = "Subscription";

  //
  //Types of state has substance type items.
  //
  public static final String STATE_HAS_SUBSTANCE_TYPE_UNKNOWN = "Unknown";
  public static final String STATE_HAS_SUBSTANCE_TYPE_YES = "Yes";
  public static final String STATE_HAS_SUBSTANCE_TYPE_NO = "No";
}
