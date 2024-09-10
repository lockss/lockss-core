/*

Copyright (c) 2018-2024 Board of Trustees of Leland Stanford Jr. University,
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

  /** Name of the archival unit state table. */
  public static final String ARCHIVAL_UNIT_STATE_TABLE = "archival_unit_state";

  /** Name of the archival unit poll agreements table. */
  public static final String ARCHIVAL_UNIT_AGREEMENTS_TABLE =
      "archival_unit_agreements";

  /** Name of the archival unit suspect URL versions table. */
  public static final String ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE =
      "archival_unit_suspect_url_versions";

  /** Name of the archival unit no AU peer set table. */
  public static final String ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE =
      "archival_unit_no_au_peer_set";

  /** Name of the archival unit statistics table. */
  public static final String ARCHIVAL_UNIT_SIZE_TABLE =
      "archival_unit_size";

  /** Name of the archival unit statistics table. */
  public static final String AUID_TABLE = "auids";

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

  /** AUID sequential identifier column. */
  public static final String AUID_SEQ_COLUMN = "auid_seq";

  /** Name of AUID column. */
  public static final String AUID_COLUMN = "auid";

  /** Archival Unit creation time column. */
  public static final String CREATION_TIME_COLUMN = "creation_time";

  /** Archival Unit last update time column. */
  public static final String LAST_UPDATE_TIME_COLUMN = "last_update_time";

  /** Name of archival unit configuration key column. */
  public static final String CONFIG_KEY_COLUMN = "config_key";

  /** Name of archival unit configuration value column. */
  public static final String CONFIG_VALUE_COLUMN = "config_value";
  
  /** Name of archival unit state string column */
  public static final String STATE_STRING_COLUMN = "state_string";
  
  /** Name of archival unit poll agreements string column */
  public static final String AGREEMENTS_STRING_COLUMN = "agreements_string";
  
  /** Name of archival unit suspect URL versions string column */
  public static final String SUSPECT_URL_VERSIONS_STRING_COLUMN =
      "suspect_url_versions_string";
  
  /** Name of archival unit no AU peer set string column */
  public static final String NO_AU_PEER_SET_STRING_COLUMN =
      "no_au_peer_set_string";

  /** Name of the archival unit size on disk column */
  public static final String AU_DISK_SIZE_COLUMN =
      "disk_size";

  /** Name of the archival unit all versions size column */
  public static final String AU_ALL_VERSIONS_SIZE_COLUMN =
      "all_versions_size";

  /** Name of the archival unit latest versions size column */
  public static final String AU_LATEST_VERSIONS_SIZE_COLUMN =
      "latest_versions_size";

  //
  // Maximum lengths of variable text length database columns.
  //
  /** Length of the plugin identifier column. */
  public static final int MAX_PLUGIN_ID_COLUMN = 256;

  /** Length of the archival unit key column. */
  public static final int MAX_ARCHIVAL_UNIT_KEY_COLUMN = 512;

  /** Length of the AUID column */
  public static final int MAX_AUID_COLUMN =
      MAX_PLUGIN_ID_COLUMN + MAX_ARCHIVAL_UNIT_KEY_COLUMN;

  /** Length of the archival unit configuration key column. */
  public static final int MAX_CONFIG_KEY_COLUMN = 128;

  /** Length of the archival unit configuration value column. */
  public static final int MAX_CONFIG_VALUE_COLUMN = 4096;

  /** Length of the archival unit state string column */
  public static final int MAX_STATE_STRING_COLUMN = 1024;

  /** Length of the archival unit poll agreements string column */
  public static final int MAX_AGREEMENTS_STRING_COLUMN = 8192;

  /** Length of the archival unit suspect URL versions string column */
  public static final int MAX_SUSPECT_URL_VERSIONS_STRING_COLUMN = 8192;

  /** Length of the archival unit no AU peer set string column */
  public static final int MAX_NO_AU_PEER_SET_STRING_COLUMN = 8192;

  public static final int MAX_VARCHAR = 32000;
}
