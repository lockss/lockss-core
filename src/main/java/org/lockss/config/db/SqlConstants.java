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
}
