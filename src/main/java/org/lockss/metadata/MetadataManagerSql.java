/*

 Copyright (c) 2015-2018 Board of Trustees of Leland Stanford Jr. University,
 all rights reserved.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 Except as contained in this notice, the name of Stanford University shall not
 be used in advertising or otherwise to promote the sale, use or other dealings
 in this Software without prior written authorization from Stanford University.

 */
package org.lockss.metadata;

import static java.sql.Types.BIGINT;
import static org.lockss.metadata.SqlConstants.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.plugin.PluginManager;
import org.lockss.util.KeyPair;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;

public class MetadataManagerSql {
  private static final Logger log = Logger.getLogger();

  // Query to count publication items that have associated AU_ITEMs
  // of type 'journal' or 'book' or 'proceedings'.
  private static final String COUNT_PUBLICATION_QUERY = 
        "select count(distinct "
      + PUBLICATION_TABLE + "." + MD_ITEM_SEQ_COLUMN + ") from "
      + PUBLISHER_TABLE + "," + PUBLICATION_TABLE + "," 
      + MD_ITEM_TABLE + "," + MD_ITEM_TYPE_TABLE
      + " where " + PUBLISHER_TABLE + "." + PUBLISHER_SEQ_COLUMN
      + "=" + PUBLICATION_TABLE + "." + PUBLISHER_SEQ_COLUMN
      + " and " + PUBLICATION_TABLE + "." + MD_ITEM_SEQ_COLUMN
      + "=" + MD_ITEM_TABLE + "." + MD_ITEM_SEQ_COLUMN
      + " and " + MD_ITEM_TABLE + "." + MD_ITEM_TYPE_SEQ_COLUMN
      + "=" + MD_ITEM_TYPE_TABLE + "." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and " + MD_ITEM_TYPE_TABLE + "." + TYPE_NAME_COLUMN
      + " in ('" + MD_ITEM_TYPE_JOURNAL + "','" + MD_ITEM_TYPE_BOOK + "','"
      + MD_ITEM_TYPE_PROCEEDINGS + "')";

  // Query to find a plugin by its identifier.
  private static final String FIND_PLUGIN_QUERY = "select "
      + PLUGIN_SEQ_COLUMN
      + " from " + PLUGIN_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?";

  // Query to add a plugin.
  private static final String INSERT_PLUGIN_QUERY = "insert into "
      + PLUGIN_TABLE
      + "(" + PLUGIN_SEQ_COLUMN
      + "," + PLUGIN_ID_COLUMN
      + "," + PLATFORM_SEQ_COLUMN
      + "," + IS_BULK_CONTENT_COLUMN
      + ") values (default,?,?,?)";

  // Query to find an Archival Unit by its plugin and key.
  private static final String FIND_AU_QUERY = "select "
      + AU_SEQ_COLUMN
      + " from " + AU_TABLE
      + " where " + PLUGIN_SEQ_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to add an Archival Unit.
  private static final String INSERT_AU_QUERY = "insert into "
      + AU_TABLE
      + "(" + AU_SEQ_COLUMN
      + "," + PLUGIN_SEQ_COLUMN
      + "," + AU_KEY_COLUMN
      + ") values (default,?,?)";

  // Query to add an Archival Unit metadata entry.
  private static final String INSERT_AU_MD_QUERY = "insert into "
      + AU_MD_TABLE
      + "(" + AU_MD_SEQ_COLUMN
      + "," + AU_SEQ_COLUMN
      + "," + MD_VERSION_COLUMN
      + "," + EXTRACT_TIME_COLUMN
      + "," + CREATION_TIME_COLUMN
      + "," + PROVIDER_SEQ_COLUMN
      + ") values (default,?,?,?,?,?)";

  // Query to add a publication.
  private static final String INSERT_PUBLICATION_QUERY = "insert into "
      + PUBLICATION_TABLE
      + "(" + PUBLICATION_SEQ_COLUMN
      + "," + MD_ITEM_SEQ_COLUMN
      + "," + PUBLISHER_SEQ_COLUMN
      + ") values (default,?,?)";

  // Query to find the metadata item of a publication.
  private static final String FIND_PUBLICATION_METADATA_ITEM_QUERY = "select "
      + MD_ITEM_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE
      + " where " + PUBLICATION_SEQ_COLUMN + " = ?";

  // Query to find the parent metadata item
  private static final String FIND_PARENT_METADATA_ITEM_QUERY = "select "
      + PARENT_SEQ_COLUMN
      + " from " + MD_ITEM_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";
	
  // Query to add an ISSN.
  private static final String INSERT_ISSN_QUERY = "insert into "
      + ISSN_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + ISSN_COLUMN
      + "," + ISSN_TYPE_COLUMN
      + ") values (?,?,?)";
	
  // Query to add an ISBN.
  private static final String INSERT_ISBN_QUERY = "insert into "
      + ISBN_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + ISBN_COLUMN
      + "," + ISBN_TYPE_COLUMN
      + ") values (?,?,?)";

  // Query to find the ISSNs of a metadata item.
  private static final String FIND_MD_ITEM_ISSN_QUERY = "select "
      + ISSN_COLUMN
      + "," + ISSN_TYPE_COLUMN
      + " from " + ISSN_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to find the proprietary identifiers of a metadata item.
  private static final String FIND_MD_ITEM_PROPRIETARY_ID_QUERY = "select "
      + PROPRIETARY_ID_COLUMN
      + " from " + PROPRIETARY_ID_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to find the ISBNs of a metadata item.
  private static final String FIND_MD_ITEM_ISBN_QUERY = "select "
      + ISBN_COLUMN
      + "," + ISBN_TYPE_COLUMN
      + " from " + ISBN_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to find a publication by its ISSNs.
  private static final String FIND_PUBLICATION_BY_ISSNS_QUERY = "select"
      + " p." + PUBLICATION_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE + " p,"
      + ISSN_TABLE + " i,"
      + MD_ITEM_TABLE + " m,"
      + MD_ITEM_TYPE_TABLE + " t"
      + " where p." + PUBLISHER_SEQ_COLUMN + " = ?"
      + " and m." + AU_MD_SEQ_COLUMN + " is null"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = i." + MD_ITEM_SEQ_COLUMN
      + " and (i." + ISSN_COLUMN + " = ?"
      + " or i." + ISSN_COLUMN + " = ?)"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = m." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_TYPE_SEQ_COLUMN + " = t." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and t." + TYPE_NAME_COLUMN + " = ?";

  // Query to find a publication by its ISBNs.
  private static final String FIND_PUBLICATION_BY_ISBNS_QUERY = "select"
      + " p." + PUBLICATION_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE + " p,"
      + ISBN_TABLE + " i,"
      + MD_ITEM_TABLE + " m,"
      + MD_ITEM_TYPE_TABLE + " t"
      + " where p." + PUBLISHER_SEQ_COLUMN + " = ?"
      + " and m." + AU_MD_SEQ_COLUMN + " is null"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = i." + MD_ITEM_SEQ_COLUMN
      + " and (i." + ISBN_COLUMN + " = ?"
      + " or i." + ISBN_COLUMN + " = ?)"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = m." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_TYPE_SEQ_COLUMN + " = t." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and t." + TYPE_NAME_COLUMN + " = ?";

  // Query to find a publication by its name.
  private static final String FIND_PUBLICATION_BY_NAME_QUERY =
      "select p." + PUBLICATION_SEQ_COLUMN
      + " from " + PUBLICATION_TABLE + " p,"
      + MD_ITEM_TABLE + " m,"
      + MD_ITEM_NAME_TABLE + " n,"
      + MD_ITEM_TYPE_TABLE + " t"
      + " where p." + PUBLISHER_SEQ_COLUMN + " = ?"
      + " and m." + AU_MD_SEQ_COLUMN + " is null"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = m." + MD_ITEM_SEQ_COLUMN
      + " and p." + MD_ITEM_SEQ_COLUMN + " = n." + MD_ITEM_SEQ_COLUMN
      + " and n." + NAME_COLUMN + " = ?"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = m." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_TYPE_SEQ_COLUMN + " = t." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and t." + TYPE_NAME_COLUMN + " = ?";
  
  // Query to find a metadata item type by its name.
  private static final String FIND_MD_ITEM_TYPE_QUERY = "select "
      + MD_ITEM_TYPE_SEQ_COLUMN
      + " from " + MD_ITEM_TYPE_TABLE
      + " where " + TYPE_NAME_COLUMN + " = ?";

  // Query to add a metadata item.
  private static final String INSERT_MD_ITEM_QUERY = "insert into "
      + MD_ITEM_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + PARENT_SEQ_COLUMN
      + "," + MD_ITEM_TYPE_SEQ_COLUMN
      + "," + AU_MD_SEQ_COLUMN
      + "," + DATE_COLUMN
      + "," + COVERAGE_COLUMN
      + "," + FETCH_TIME_COLUMN
      + ") values (default,?,?,?,?,?,?)";
  
  // Query to count the ISBNs of a publication.
  private static final String COUNT_PUBLICATION_ISBNS_QUERY = "select "
      + "count(*)"
      + " from " + ISBN_TABLE + " i"
      + "," + PUBLICATION_TABLE + " p"
      + " where p." + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = i." + MD_ITEM_SEQ_COLUMN;
  
  // Query to count the ISSNs of a publication.
  private static final String COUNT_PUBLICATION_ISSNS_QUERY = "select "
      + "count(*)"
      + " from " + ISSN_TABLE + " i"
      + "," + PUBLICATION_TABLE + " p"
      + " where p." + PUBLICATION_SEQ_COLUMN + " = ?"
      + " and p." + MD_ITEM_SEQ_COLUMN + " = i." + MD_ITEM_SEQ_COLUMN;

  // Query to find the secondary names of a metadata item.
  private static final String FIND_MD_ITEM_NAME_QUERY = "select "
      + NAME_COLUMN
      + "," + NAME_TYPE_COLUMN
      + " from " + MD_ITEM_NAME_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?";

  // Query to add a metadata item name.
  private static final String INSERT_MD_ITEM_NAME_QUERY = "insert into "
      + MD_ITEM_NAME_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + NAME_COLUMN
      + "," + NAME_TYPE_COLUMN
      + ") values (?,?,?)";

  // Query to add a metadata item URL.
  private static final String INSERT_URL_QUERY = "insert into "
      + URL_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + FEATURE_COLUMN
      + "," + URL_COLUMN
      + ") values (?,?,?)";

  // Query to add a metadata item DOI.
  private static final String INSERT_DOI_QUERY = "insert into "
      + DOI_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + DOI_COLUMN
      + ") values (?,?)";

  // Query to find a platform by its name.
  private static final String FIND_PLATFORM_QUERY = "select "
      + PLATFORM_SEQ_COLUMN
      + " from " + PLATFORM_TABLE
      + " where " + PLATFORM_NAME_COLUMN + " = ?";

  // Query to add a platform.
  private static final String INSERT_PLATFORM_QUERY = "insert into "
      + PLATFORM_TABLE
      + "(" + PLATFORM_SEQ_COLUMN
      + "," + PLATFORM_NAME_COLUMN
      + ") values (default,?)";

  // Query to add an archival unit to the UNCONFIGURED_AU table.
  private static final String INSERT_UNCONFIGURED_AU_QUERY = "insert into "
      + UNCONFIGURED_AU_TABLE
      + "(" + PLUGIN_ID_COLUMN
      + "," + AU_KEY_COLUMN
      + ") values (?,?)";

  // Query to count recorded unconfigured archival units.
  private static final String UNCONFIGURED_AU_COUNT_QUERY = "select "
      + "count(*)"
      + " from " + UNCONFIGURED_AU_TABLE;
  
  // Query to find if an archival unit is in the UNCONFIGURED_AU table.
  private static final String FIND_UNCONFIGURED_AU_COUNT_QUERY = "select "
      + "count(*)"
      + " from " + UNCONFIGURED_AU_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to retrieve all the publisher names.
  private static final String GET_PUBLISHER_NAMES_QUERY = "select "
      + PUBLISHER_NAME_COLUMN
      + " from " + PUBLISHER_TABLE
      + " order by " + PUBLISHER_NAME_COLUMN;

  // Derby query to retrieve all the different DOI prefixes of all the
  // publishers with multiple DOI prefixes.
  private static final String GET_PUBLISHERS_MULTIPLE_DOI_PREFIXES_DERBY_QUERY =
      "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and pr." + PUBLISHER_NAME_COLUMN + " in ("
      + " select subq." + PUBLISHER_NAME_COLUMN
      + " from ("
      + "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + PUBLISHER_NAME_COLUMN
      + " having count(subq." + PUBLISHER_NAME_COLUMN + ") > 1)"
      + " order by pr." + PUBLISHER_NAME_COLUMN
      + ", prefix";

  // PostgreSQL query to retrieve all the different DOI prefixes of all the
  // publishers with multiple DOI prefixes.
  private static final String GET_PUBLISHERS_MULTIPLE_DOI_PREFIXES_PG_QUERY =
      "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and pr." + PUBLISHER_NAME_COLUMN + " in ("
      + " select subq." + PUBLISHER_NAME_COLUMN
      + " from ("
      + "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + PUBLISHER_NAME_COLUMN
      + " having count(subq." + PUBLISHER_NAME_COLUMN + ") > 1)"
      + " order by pr." + PUBLISHER_NAME_COLUMN
      + ", prefix";

  // MySQL query to retrieve all the different DOI prefixes of all the
  // publishers with multiple DOI prefixes.
  private static final String GET_PUBLISHERS_MULTIPLE_DOI_PREFIXES_MYSQL_QUERY =
      "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substring_index(d." + DOI_COLUMN + ", '/', 1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and pr." + PUBLISHER_NAME_COLUMN + " in ("
      + " select subq." + PUBLISHER_NAME_COLUMN
      + " from ("
      + "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substring_index(d." + DOI_COLUMN + ", '/', 1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + PUBLISHER_NAME_COLUMN
      + " having count(subq." + PUBLISHER_NAME_COLUMN + ") > 1)"
      + " order by pr." + PUBLISHER_NAME_COLUMN
      + ", prefix";

  // Derby query to retrieve all the different publishers linked to all the DOI
  // prefixes that are linked to multiple publishers.
  private static final String GET_DOI_PREFIXES_MULTIPLE_PUBLISHERS_DERBY_QUERY =
      "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) in ("
      + " select subq.prefix"
      + " from ("
      + "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq.prefix"
      + " having count(subq.prefix) > 1)"
      + " order by prefix, pr."
      + PUBLISHER_NAME_COLUMN;

  // PostgreSql query to retrieve all the different publishers linked to all the
  // DOI prefixes that are linked to multiple publishers.
  private static final String GET_DOI_PREFIXES_MULTIPLE_PUBLISHERS_PG_QUERY =
      "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) in ("
      + " select subq.prefix"
      + " from ("
      + "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq.prefix"
      + " having count(subq.prefix) > 1)"
      + " order by prefix, pr."
      + PUBLISHER_NAME_COLUMN;

  // MySQL query to retrieve all the different publishers linked to all the DOI
  // prefixes that are linked to multiple publishers.
  private static final String GET_DOI_PREFIXES_MULTIPLE_PUBLISHERS_MYSQL_QUERY =
      "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substring_index(d." + DOI_COLUMN + ", '/', 1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and substring_index(d." + DOI_COLUMN + ", '/', 1) in ("
      + " select subq.prefix"
      + " from ("
      + "select distinct pr." + PUBLISHER_NAME_COLUMN
      + ", substring_index(d." + DOI_COLUMN + ", '/', 1) as prefix"
      + " from " + PUBLISHER_TABLE + " pr"
      + ", " + DOI_TABLE + " d"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pr." + PUBLISHER_SEQ_COLUMN + " = pn." + PUBLISHER_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = m." + PARENT_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq.prefix"
      + " having count(subq.prefix) > 1)"
      + " order by prefix, pr."
      + PUBLISHER_NAME_COLUMN;

  // Derby query to retrieve all the different DOI prefixes of all the Archival
  // Units with multiple DOI prefixes.
  private static final String GET_AUS_MULTIPLE_DOI_PREFIXES_DERBY_QUERY =
      "select distinct pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", au." + AU_SEQ_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) as prefix"
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + DOI_TABLE + " d"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " in ("
      + " select subq." + AU_SEQ_COLUMN
      + " from ("
      + "select distinct au." + AU_SEQ_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, locate('/', d." + DOI_COLUMN
      + ")-1) as prefix"
      + " from " + AU_TABLE
      + ", " + DOI_TABLE + " d"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + " where au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + AU_SEQ_COLUMN
      + " having count(subq." + AU_SEQ_COLUMN + ") > 1)"
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", prefix";

  // PostgreSQL query to retrieve all the different DOI prefixes of all the
  // Archival Units with multiple DOI prefixes.
  private static final String GET_AUS_MULTIPLE_DOI_PREFIXES_PG_QUERY =
      "select distinct " + " pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", au." + AU_SEQ_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) as prefix"
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + DOI_TABLE + " d"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " in ("
      + " select subq." + AU_SEQ_COLUMN
      + " from ("
      + "select distinct au." + AU_SEQ_COLUMN
      + ", substr(d." + DOI_COLUMN + ", 1, strpos(d." + DOI_COLUMN
      + ", '/')-1) as prefix"
      + " from " + AU_TABLE
      + ", " + DOI_TABLE + " d"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + " where au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + AU_SEQ_COLUMN
      + " having count(subq." + AU_SEQ_COLUMN + ") > 1)"
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", prefix";

  // MySQL query to retrieve all the different DOI prefixes of all the Archival
  // Units with multiple DOI prefixes.
  private static final String GET_AUS_MULTIPLE_DOI_PREFIXES_MYSQL_QUERY =
      "select distinct pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", au." + AU_SEQ_COLUMN
      + ", substring_index(d." + DOI_COLUMN + ", '/', 1) as prefix"
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + DOI_TABLE + " d"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " in ("
      + " select subq." + AU_SEQ_COLUMN
      + " from ("
      + "select distinct au." + AU_SEQ_COLUMN
      + ", substring_index(d." + DOI_COLUMN + ", '/', 1) as prefix"
      + " from " + AU_TABLE
      + ", " + DOI_TABLE + " d"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + " where au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = d." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + AU_SEQ_COLUMN
      + " having count(subq." + AU_SEQ_COLUMN + ") > 1)"
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", prefix";

  // Query to retrieve all the different ISBNs of all the publications with more
  // than 2 ISBNs.
  private static final String GET_PUBLICATIONS_MORE_2_ISBNS_QUERY = "select"
      + " distinct mn." + NAME_COLUMN
      + ", isbn." + ISBN_COLUMN
      + ", isbn." + ISBN_TYPE_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISBN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = isbn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and mn." + NAME_COLUMN + " in ("
      + "select subq." + NAME_COLUMN + " from ("
      + " select distinct mn." + NAME_COLUMN
      + ", isbn." + ISBN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISBN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = isbn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + ") as subq"
      + " group by subq." + NAME_COLUMN
      + " having count(subq." + NAME_COLUMN + ") > 2)"
      + " order by mn." + NAME_COLUMN
      + ", isbn." + ISBN_COLUMN
      + ", isbn." + ISBN_TYPE_COLUMN;

  // Query to retrieve all the different ISSNs of all the publications with more
  // than 2 ISSNs.
  private static final String GET_PUBLICATIONS_MORE_2_ISSNS_QUERY = "select"
      + " distinct mn." + NAME_COLUMN
      + ", issn." + MD_ITEM_SEQ_COLUMN
      + ", issn." + ISSN_COLUMN
      + ", issn." + ISSN_TYPE_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISSN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = issn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and mn." + NAME_COLUMN + " in ("
      + "select subq." + NAME_COLUMN + " from ("
      + " select distinct mn." + NAME_COLUMN
      + ", issn." + ISSN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISSN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = issn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + ") as subq"
      + " group by subq." + NAME_COLUMN
      + " having count(subq." + NAME_COLUMN + ") > 2)"
      + " order by mn." + NAME_COLUMN
      + ", issn." + ISSN_COLUMN
      + ", issn." + ISSN_TYPE_COLUMN;

  // Query to retrieve all the different publications linked to all the ISBNs
  // that are linked to multiple publications.
  private static final String GET_ISBNS_MULTIPLE_PUBLICATIONS_QUERY = "select"
      + " distinct mn." + NAME_COLUMN
      + ", isbn." + ISBN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISBN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = isbn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and isbn." + ISBN_COLUMN + " in ("
      + "select subq." + ISBN_COLUMN + " from ("
      + " select distinct mn." + NAME_COLUMN
      + ", isbn." + ISBN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISBN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = isbn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + ") as subq"
      + " group by subq." + ISBN_COLUMN
      + " having count(subq." + ISBN_COLUMN + ") > 1)"
      + " order by isbn." + ISBN_COLUMN
      + ", mn." + NAME_COLUMN;

  // Query to retrieve all the different publications linked to all the ISSNs
  // that are linked to multiple publications.
  private static final String GET_ISSNS_MULTIPLE_PUBLICATIONS_QUERY = "select"
      + " distinct mn." + NAME_COLUMN
      + ", issn." + ISSN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISSN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = issn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and issn." + ISSN_COLUMN + " in ("
      + "select subq." + ISSN_COLUMN + " from ("
      + " select distinct mn." + NAME_COLUMN
      + ", issn." + ISSN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + ISSN_TABLE
      + " where mn." + MD_ITEM_SEQ_COLUMN + " = issn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + ") as subq"
      + " group by subq." + ISSN_COLUMN
      + " having count(subq." + ISSN_COLUMN + ") > 1)"
      + " order by issn." + ISSN_COLUMN
      + ", mn." + NAME_COLUMN;

  // Query to retrieve all the different ISSNs that are linked to books.
  private static final String GET_BOOKS_WITH_ISSNS_QUERY = "select distinct"
      + " mn." + NAME_COLUMN
      + ", mit." + TYPE_NAME_COLUMN
      + ", issn." + ISSN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + MD_ITEM_TYPE_TABLE + " mit"
      + ", " + ISSN_TABLE
      + ", " + PUBLICATION_TABLE + " p"
      + ", " + MD_ITEM_TABLE + " m"
      + " where p." + MD_ITEM_SEQ_COLUMN + " = m." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = mn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and m." + MD_ITEM_SEQ_COLUMN + " = issn." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mit." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mit." + TYPE_NAME_COLUMN + " != '" + MD_ITEM_TYPE_BOOK_SERIES
      + "'"
      + " and mit." + TYPE_NAME_COLUMN + " != '" + MD_ITEM_TYPE_JOURNAL + "'"
      + " and mit." + TYPE_NAME_COLUMN + " != '" + MD_ITEM_TYPE_PROCEEDINGS
      + "'"
      + " and mit." + TYPE_NAME_COLUMN + " != '"
      + MD_ITEM_TYPE_UNKNOWN_PUBLICATION + "'"
      + " order by mn." + NAME_COLUMN
      + ", mit." + TYPE_NAME_COLUMN
      + ", issn." + ISSN_COLUMN;

  // Query to retrieve all the different ISBNs that are linked to periodicals.
  private static final String GET_PERIODICALS_WITH_ISBNS_QUERY = "select"
      + " distinct mn." + NAME_COLUMN
      + ", mit." + TYPE_NAME_COLUMN
      + ", isbn." + ISBN_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " mn"
      + ", " + MD_ITEM_TYPE_TABLE + " mit"
      + ", " + ISBN_TABLE
      + ", " + PUBLICATION_TABLE + " p"
      + ", " + MD_ITEM_TABLE + " m"
      + " where p." + MD_ITEM_SEQ_COLUMN + " = m." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_SEQ_COLUMN + " = mn." + MD_ITEM_SEQ_COLUMN
      + " and mn." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and m." + MD_ITEM_SEQ_COLUMN + " = isbn." + MD_ITEM_SEQ_COLUMN
      + " and m." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mit." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mit." + TYPE_NAME_COLUMN + " != '" + MD_ITEM_TYPE_BOOK + "'"
      + " order by mn." + NAME_COLUMN
      + ", mit." + TYPE_NAME_COLUMN
      + ", isbn." + ISBN_COLUMN;

  // Query to retrieve all the Archival Units with an unknown provider.
  private static final String GET_UNKNOWN_PROVIDER_AUS_QUERY = "select"
      + " pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + AU_MD_TABLE + " am"
      + ", " + PROVIDER_TABLE + " pr"
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + PROVIDER_SEQ_COLUMN + " = pr." + PROVIDER_SEQ_COLUMN
      + " and pr." + PROVIDER_NAME_COLUMN
      + " = '" + UNKNOWN_PROVIDER_NAME + "'"
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN;

  // Query to retrieve all the journal articles in the database whose parent
  // is not a journal.
  private static final String GET_MISMATCHED_PARENT_JOURNAL_ARTICLES_QUERY =
	"select min1." + NAME_COLUMN + " as \"col1\""
	+ ", min2." + NAME_COLUMN + " as \"col2\""
	+ ", mit." + TYPE_NAME_COLUMN + " as \"col3\""
	+ ", au." + AU_KEY_COLUMN + " as \"col4\""
	+ ", pl." + PLUGIN_ID_COLUMN + " as \"col5\""
	+ " from " + MD_ITEM_TYPE_TABLE + " mit"
	+ ", " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " pl"
	+ ", " + AU_MD_TABLE + " am"
	+ ", " + MD_ITEM_TABLE + " mi1"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min1"
	+ " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
	+ " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ ", " + MD_ITEM_TABLE + " mi2"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min2"
	+ " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
	+ " and min2." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ " where mi1." + PARENT_SEQ_COLUMN + " = mi2." + MD_ITEM_SEQ_COLUMN
	+ " and mit." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " = mi2." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
	+ " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
	+ " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
	+ " and mi1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 5"
	+ " and mi2." + MD_ITEM_TYPE_SEQ_COLUMN + " != 4"
	+ " union "
	+ "select min1." + NAME_COLUMN + " as \"col1\""
	+ ", '' as \"col2\""
	+ ", '' as \"col3\""
	+ ", au." + AU_KEY_COLUMN + " as \"col4\""
	+ ", pl." + PLUGIN_ID_COLUMN + " as \"col5\""
	+ " from " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " pl"
	+ ", " + AU_MD_TABLE + " am"
	+ ", " + MD_ITEM_TABLE + " mi1"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min1"
	+ " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
	+ " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ " where mi1." + PARENT_SEQ_COLUMN + " is null"
	+ " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
	+ " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
	+ " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
	+ " and mi1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 5"
	+ " order by \"col5\", \"col4\", \"col2\", \"col1\"";

  // Query to retrieve all the book chapters in the database whose parent is not
  // a book nor a book series.
  private static final String GET_MISMATCHED_PARENT_BOOK_CHAPTERS_QUERY =
	"select min1." + NAME_COLUMN + " as \"col1\""
	+ ", min2." + NAME_COLUMN + " as \"col2\""
	+ ", mit." + TYPE_NAME_COLUMN + " as \"col3\""
	+ ", au." + AU_KEY_COLUMN + " as \"col4\""
	+ ", pl." + PLUGIN_ID_COLUMN + " as \"col5\""
	+ " from " + MD_ITEM_TYPE_TABLE + " mit"
	+ ", " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " pl"
	+ ", " + AU_MD_TABLE + " am"
	+ ", " + MD_ITEM_TABLE + " mi1"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min1"
	+ " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
	+ " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ ", " + MD_ITEM_TABLE + " mi2"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min2"
	+ " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
	+ " and min2." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ " where mi1." + PARENT_SEQ_COLUMN + " = mi2." + MD_ITEM_SEQ_COLUMN
	+ " and mit." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " = mi2." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
	+ " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
	+ " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
	+ " and mi1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 3"
	+ " and mi2." + MD_ITEM_TYPE_SEQ_COLUMN + " != 2"
	+ " and mi2." + MD_ITEM_TYPE_SEQ_COLUMN + " != 1"
	+ " union "
	+ "select min1." + NAME_COLUMN + " as \"col1\""
	+ ", '' as \"col2\""
	+ ", '' as \"col3\""
	+ ", au." + AU_KEY_COLUMN + " as \"col4\""
	+ ", pl." + PLUGIN_ID_COLUMN + " as \"col5\""
	+ " from " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " pl"
	+ ", " + AU_MD_TABLE + " am"
	+ ", " + MD_ITEM_TABLE + " mi1"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min1"
	+ " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
	+ " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ " where mi1." + PARENT_SEQ_COLUMN + " is null"
	+ " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
	+ " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
	+ " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
	+ " and mi1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 3"
	+ " order by \"col5\", \"col4\", \"col2\", \"col1\"";

  // Query to retrieve all the book volumes in the database whose parent is not
  // a book nor a book series.
  private static final String GET_MISMATCHED_PARENT_BOOK_VOLUMES_QUERY =
	"select min1." + NAME_COLUMN + " as \"col1\""
	+ ", min2." + NAME_COLUMN + " as \"col2\""
	+ ", mit." + TYPE_NAME_COLUMN + " as \"col3\""
	+ ", au." + AU_KEY_COLUMN + " as \"col4\""
	+ ", pl." + PLUGIN_ID_COLUMN + " as \"col5\""
	+ " from " + MD_ITEM_TYPE_TABLE + " mit"
	+ ", " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " pl"
	+ ", " + AU_MD_TABLE + " am"
	+ ", " + MD_ITEM_TABLE + " mi1"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min1"
	+ " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
	+ " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ ", " + MD_ITEM_TABLE + " mi2"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min2"
	+ " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
	+ " and min2." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ " where mi1." + PARENT_SEQ_COLUMN + " = mi2." + MD_ITEM_SEQ_COLUMN
	+ " and mit." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " = mi2." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
	+ " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
	+ " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
	+ " and mi1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 6"
	+ " and mi2." + MD_ITEM_TYPE_SEQ_COLUMN + " != 2"
	+ " and mi2." + MD_ITEM_TYPE_SEQ_COLUMN + " != 1"
	+ " union "
	+ "select min1." + NAME_COLUMN + " as \"col1\""
	+ ", '' as \"col2\""
	+ ", '' as \"col3\""
	+ ", au." + AU_KEY_COLUMN + " as \"col4\""
	+ ", pl." + PLUGIN_ID_COLUMN + " as \"col5\""
	+ " from " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " pl"
	+ ", " + AU_MD_TABLE + " am"
	+ ", " + MD_ITEM_TABLE + " mi1"
	+ " left outer join " + MD_ITEM_NAME_TABLE + " min1"
	+ " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
	+ " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
	+ " where mi1." + PARENT_SEQ_COLUMN + " is null"
	+ " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
	+ " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
	+ " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
	+ " and mi1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 6"
	+ " order by \"col5\", \"col4\", \"col2\", \"col1\"";

  // Query to retrieve all the different publishers of all the Archival Units
  // with multiple publishers.
  private static final String GET_AUS_MULTIPLE_PUBLISHERS_QUERY = "select "
      + "distinct pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", pr." + PUBLISHER_NAME_COLUMN
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + PUBLISHER_TABLE + " pr"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + ", " + PUBLICATION_TABLE + " pn"
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + PARENT_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + PUBLISHER_SEQ_COLUMN + " = pr." + PUBLISHER_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " in ("
      + " select subq." + AU_SEQ_COLUMN
      + " from ("
      + "select distinct au." + AU_SEQ_COLUMN
      + ", pr." + PUBLISHER_SEQ_COLUMN
      + " from " + AU_TABLE
      + ", " + PUBLISHER_TABLE + " pr"
      + ", " + AU_MD_TABLE + " am"
      + ", " + MD_ITEM_TABLE + " m"
      + ", " + PUBLICATION_TABLE + " pn"
      + " where au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + AU_MD_SEQ_COLUMN + " = m." + AU_MD_SEQ_COLUMN
      + " and m." + PARENT_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + PUBLISHER_SEQ_COLUMN + " = pr." + PUBLISHER_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + AU_SEQ_COLUMN
      + " having count(subq." + AU_SEQ_COLUMN + ") > 1)"
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", pr." + PUBLISHER_NAME_COLUMN;

  // Query to retrieve all the metadata items that have no name.
  private static final String GET_UNNAMED_ITEMS_QUERY = "select "
      + "count(mi1." + MD_ITEM_SEQ_COLUMN + ") as \"col1\""
      + ", mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " as \"ts1\""
      + ", mit1." + TYPE_NAME_COLUMN + " as \"col2\""
      + ", min2." + NAME_COLUMN + " as \"col3\""
      + ", mit2." + MD_ITEM_TYPE_SEQ_COLUMN + " as \"ts2\""
      + ", mit2." + TYPE_NAME_COLUMN + " as \"col4\""
      + ", au." + AU_KEY_COLUMN + " as \"col5\""
      + ", pl." + PLUGIN_ID_COLUMN + " as \"col6\""
      + ", pr." + PUBLISHER_NAME_COLUMN + " as \"col7\""
      + " from " + MD_ITEM_TYPE_TABLE + " mit1"
      + ", " + MD_ITEM_TYPE_TABLE + " mit2"
      + ", " + AU_TABLE
      + ", " + PLUGIN_TABLE + " pl"
      + ", " + AU_MD_TABLE + " am"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + PUBLISHER_TABLE + " pr"
      + ", " + MD_ITEM_TABLE + " mi1"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min1"
      + " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
      + " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + ", " + MD_ITEM_TABLE + " mi2"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min2"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
      + " and min2." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + " where mi1." + PARENT_SEQ_COLUMN + " = mi2." + MD_ITEM_SEQ_COLUMN
      + " and mit1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mit2." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi2." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
      + " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
      + " and mi2." + MD_ITEM_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + PUBLISHER_SEQ_COLUMN + " = pr." + PUBLISHER_SEQ_COLUMN
      + " and min1." + NAME_COLUMN + " is null"
      + " group by mit1." + MD_ITEM_TYPE_SEQ_COLUMN
      + ", mit1." + TYPE_NAME_COLUMN
      + ", min2." + NAME_COLUMN
      + ", mit2." + MD_ITEM_TYPE_SEQ_COLUMN
      + ", mit2." + TYPE_NAME_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", pl." + PLUGIN_ID_COLUMN
      + ", pr." + PUBLISHER_NAME_COLUMN
      + " union "
      + "select count(mi1." + MD_ITEM_SEQ_COLUMN + ") as \"col1\""
      + ", mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " as \"ts1\""
      + ", mit1." + TYPE_NAME_COLUMN + " as \"col2\""
      + ", '' as \"col3\""
      + ", 0 as \"ts2\""
      + ", '' as \"col4\""
      + ", au." + AU_KEY_COLUMN + " as \"col5\""
      + ", pl." + PLUGIN_ID_COLUMN + " as \"col6\""
      + ", pr." + PUBLISHER_NAME_COLUMN + " as \"col7\""
      + " from " + MD_ITEM_TYPE_TABLE + " mit1"
      + ", " + AU_TABLE
      + ", " + PLUGIN_TABLE + " pl"
      + ", " + AU_MD_TABLE + " am"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + PUBLISHER_TABLE + " pr"
      + ", " + MD_ITEM_TABLE + " mi1"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min1"
      + " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
      + " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + " where mi1." + PARENT_SEQ_COLUMN + " is null"
      + " and mit1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
      + " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
      + " and mi1." + MD_ITEM_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + PUBLISHER_SEQ_COLUMN + " = pr." + PUBLISHER_SEQ_COLUMN
      + " and min1." + NAME_COLUMN + " is null"
      + " group by mit1." + MD_ITEM_TYPE_SEQ_COLUMN
      + ", mit1." + TYPE_NAME_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", pl." + PLUGIN_ID_COLUMN
      + ", pr." + PUBLISHER_NAME_COLUMN
      + " order by \"col7\", \"col6\", \"col5\", \"ts2\", \"col3\", \"ts1\"";

  // Query to find the publication date interval of an Archival Unit.
  private static final String FIND_PUBLICATION_DATE_INTERVAL_QUERY = "select "
      + "min(mi." + DATE_COLUMN + ") as earliest"
      + ", max(mi." + DATE_COLUMN + ") as latest"
      + " from " + MD_ITEM_TABLE + " mi"
      + ", " + AU_MD_TABLE + " am"
      + ", " + AU_TABLE
      + ", " + PLUGIN_TABLE + " p"
      + " where mi." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
      + " and " + AU_TABLE + "." + AU_KEY_COLUMN + " = ?"
      + " and " + AU_TABLE + "." + PLUGIN_SEQ_COLUMN
      + " = p." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?";

  // Query to retrieve all the different proprietary identifiers of all the
  // publications with multiple proprietary identifiers.
  private static final String GET_PUBLICATIONS_MULTIPLE_PIDS_QUERY =
      "select n." + NAME_COLUMN
      + ", pi." + PROPRIETARY_ID_COLUMN
      + " from " + MD_ITEM_NAME_TABLE + " n"
      + ", " + PROPRIETARY_ID_TABLE + " pi"
      + ", " + PUBLICATION_TABLE + " pn"
      + " where n." + MD_ITEM_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + MD_ITEM_SEQ_COLUMN + " = pi." + MD_ITEM_SEQ_COLUMN
      + " and n." + NAME_TYPE_COLUMN + " = 'primary'"
      + " and pn." + MD_ITEM_SEQ_COLUMN + " in ("
      + " select subq." + MD_ITEM_SEQ_COLUMN
      + " from ("
      + "select pn." + MD_ITEM_SEQ_COLUMN
      + ", pi." + PROPRIETARY_ID_COLUMN
      + " from " + PUBLICATION_TABLE + " pn"
      + ", " + PROPRIETARY_ID_TABLE + " pi"
      + " where pn." + MD_ITEM_SEQ_COLUMN + " = pi." + MD_ITEM_SEQ_COLUMN
      + ") as subq"
      + " group by subq." + MD_ITEM_SEQ_COLUMN
      + " having count(subq." + MD_ITEM_SEQ_COLUMN + ") > 1)"
      + " order by n." + NAME_COLUMN
      + ", pi." + PROPRIETARY_ID_COLUMN;

  // Query to retrieve all the non-parent metadata items that have no DOI.
  private static final String GET_NO_DOI_ITEMS_QUERY = "select "
      + "min1." + NAME_COLUMN + " as \"col1\""
      + ", mit1." + TYPE_NAME_COLUMN + " as \"col2\""
      + ", min2." + NAME_COLUMN + " as \"col3\""
      + ", mit2." + TYPE_NAME_COLUMN + " as \"col4\""
      + ", au." + AU_KEY_COLUMN + " as \"col5\""
      + ", pl." + PLUGIN_ID_COLUMN + " as \"col6\""
      + ", pr." + PUBLISHER_NAME_COLUMN + " as \"col7\""
      + " from " + MD_ITEM_TYPE_TABLE + " mit1"
      + ", " + MD_ITEM_TYPE_TABLE + " mit2"
      + ", " + AU_TABLE
      + ", " + PLUGIN_TABLE + " pl"
      + ", " + AU_MD_TABLE + " am"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + PUBLISHER_TABLE + " pr"
      + ", " + MD_ITEM_TABLE + " mi1"
      + " left outer join " + DOI_TABLE
      + " on mi1." + MD_ITEM_SEQ_COLUMN
      + " = " + DOI_TABLE + "." + MD_ITEM_SEQ_COLUMN
      + " left outer join " + MD_ITEM_NAME_TABLE + " min1"
      + " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
      + " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + ", " + MD_ITEM_TABLE + " mi2"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min2"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
      + " and min2." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + " where mi1." + PARENT_SEQ_COLUMN + " = mi2." + MD_ITEM_SEQ_COLUMN
      + " and mit1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mit2." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi2." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
      + " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
      + " and mi2." + MD_ITEM_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + PUBLISHER_SEQ_COLUMN + " = pr." + PUBLISHER_SEQ_COLUMN
      + " and (mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 3"
      + " or mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 5"
      + " or mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 6"
      + " or mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 8)"
      + " and " + DOI_TABLE + "." + DOI_COLUMN + " is null"
      + " order by \"col7\", \"col6\", \"col5\", \"col3\", \"col1\"";

  // Query to retrieve all the non-parent metadata items that have no Access
  // URL.
  private static final String GET_NO_ACCESS_URL_ITEMS_QUERY = "select "
      + "min1." + NAME_COLUMN + " as \"col1\""
      + ", mit1." + TYPE_NAME_COLUMN + " as \"col2\""
      + ", min2." + NAME_COLUMN + " as \"col3\""
      + ", mit2." + TYPE_NAME_COLUMN + " as \"col4\""
      + ", au." + AU_KEY_COLUMN + " as \"col5\""
      + ", pl." + PLUGIN_ID_COLUMN + " as \"col6\""
      + ", pr." + PUBLISHER_NAME_COLUMN + " as \"col7\""
      + " from " + MD_ITEM_TYPE_TABLE + " mit1"
      + ", " + MD_ITEM_TYPE_TABLE + " mit2"
      + ", " + AU_TABLE
      + ", " + PLUGIN_TABLE + " pl"
      + ", " + AU_MD_TABLE + " am"
      + ", " + PUBLICATION_TABLE + " pn"
      + ", " + PUBLISHER_TABLE + " pr"
      + ", " + MD_ITEM_TABLE + " mi1"
      + " left outer join " + URL_TABLE
      + " on mi1." + MD_ITEM_SEQ_COLUMN
      + " = " + URL_TABLE + "." + MD_ITEM_SEQ_COLUMN
      + " and " + URL_TABLE + "." + FEATURE_COLUMN
      + " = '" + MetadataManager.ACCESS_URL_FEATURE + "'"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min1"
      + " on mi1." + MD_ITEM_SEQ_COLUMN + " = min1." + MD_ITEM_SEQ_COLUMN
      + " and min1." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + ", " + MD_ITEM_TABLE + " mi2"
      + " left outer join " + MD_ITEM_NAME_TABLE + " min2"
      + " on mi2." + MD_ITEM_SEQ_COLUMN + " = min2." + MD_ITEM_SEQ_COLUMN
      + " and min2." + NAME_TYPE_COLUMN + " = '" + PRIMARY_NAME_TYPE + "'"
      + " where mi1." + PARENT_SEQ_COLUMN + " = mi2." + MD_ITEM_SEQ_COLUMN
      + " and mit1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi1." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mit2." + MD_ITEM_TYPE_SEQ_COLUMN
      + " = mi2." + MD_ITEM_TYPE_SEQ_COLUMN
      + " and mi1." + AU_MD_SEQ_COLUMN + " = am." + AU_MD_SEQ_COLUMN
      + " and am." + AU_SEQ_COLUMN + " = au." + AU_SEQ_COLUMN
      + " and au." + PLUGIN_SEQ_COLUMN + " = pl." + PLUGIN_SEQ_COLUMN
      + " and mi2." + MD_ITEM_SEQ_COLUMN + " = pn." + MD_ITEM_SEQ_COLUMN
      + " and pn." + PUBLISHER_SEQ_COLUMN + " = pr." + PUBLISHER_SEQ_COLUMN
      + " and (mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 3"
      + " or mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 5"
      + " or mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 6"
      + " or mit1." + MD_ITEM_TYPE_SEQ_COLUMN + " = 8)"
      + " and " + URL_TABLE + "." + FEATURE_COLUMN + " is null"
      + " order by \"col7\", \"col6\", \"col5\", \"col3\", \"col1\"";

  // Query to delete an ISSN linked to a publication.
  private static final String DELETE_ISSN_QUERY = "delete from "
      + ISSN_TABLE
      + " where " + MD_ITEM_SEQ_COLUMN + " = ?"
      + " and " + ISSN_COLUMN + " = ?"
      + " and " + ISSN_TYPE_COLUMN + " = ?";

  // Query to retrieve all the Archival Units with no metadata items.
  private static final String GET_NO_ITEMS_AUS_QUERY = "select"
      + " pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", count(mi." + MD_ITEM_SEQ_COLUMN + ")"
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + AU_MD_TABLE + " am"
      + " left outer join " + MD_ITEM_TABLE + " mi"
      + " on am." + AU_MD_SEQ_COLUMN + " = mi."+ AU_MD_SEQ_COLUMN
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " group by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN
      + " having count(mi." + MD_ITEM_SEQ_COLUMN + ") = 0"
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN;

  // Query to get the metadata information of an Archival Unit.
  private static final String GET_AU_MD_QUERY = "select "
      + "m." + AU_MD_SEQ_COLUMN
      + ", m." + AU_SEQ_COLUMN
      + ", m." + MD_VERSION_COLUMN
      + ", m." + EXTRACT_TIME_COLUMN
      + ", m." + CREATION_TIME_COLUMN
      + ", m." + PROVIDER_SEQ_COLUMN
      + " from " + AU_MD_TABLE + " m,"
      + AU_TABLE + " a,"
      + PLUGIN_TABLE + " p"
      + " where m." + AU_SEQ_COLUMN + " = " + " a." + AU_SEQ_COLUMN
      + " and a." + PLUGIN_SEQ_COLUMN + " = " + " p." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?"
      + " and a." + AU_KEY_COLUMN + " = ?";

  // Query to retrieve the data of theArchival Units in the database.
  private static final String GET_DB_ARCHIVAL_UNITS_QUERY = "select "
      + "pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_SEQ_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", am." + CREATION_TIME_COLUMN
      + ", am." + MD_VERSION_COLUMN
      + ", am." + EXTRACT_TIME_COLUMN
      + ", pv." + PROVIDER_NAME_COLUMN
      + ", count(mi." + MD_ITEM_SEQ_COLUMN + ") as \"item_count\""
      + " from " + PLUGIN_TABLE + " pl"
      + ", " + AU_TABLE
      + ", " + PROVIDER_TABLE + " pv"
      + ", " + AU_MD_TABLE + " am"
      + " left outer join " + MD_ITEM_TABLE + " mi"
      + " on am." + AU_MD_SEQ_COLUMN + " = mi." + AU_MD_SEQ_COLUMN
      + " where pl." + PLUGIN_SEQ_COLUMN + " = au." + PLUGIN_SEQ_COLUMN
      + " and au." + AU_SEQ_COLUMN + " = am." + AU_SEQ_COLUMN
      + " and am." + PROVIDER_SEQ_COLUMN + " = pv." + PROVIDER_SEQ_COLUMN
      + " group by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_SEQ_COLUMN
      + ", au." + AU_KEY_COLUMN
      + ", am." + CREATION_TIME_COLUMN
      + ", am." + MD_VERSION_COLUMN
      + ", am." + EXTRACT_TIME_COLUMN
      + ", pv." + PROVIDER_NAME_COLUMN
      + " order by pl." + PLUGIN_ID_COLUMN
      + ", au." + AU_KEY_COLUMN;

  // Query to delete an AU by its primary key and key identifier.
  private static final String DELETE_AU_BY_PK_AND_KEY_QUERY = "delete from "
      + AU_TABLE
      + " where "
      + AU_SEQ_COLUMN + " = ?"
      + " and " + AU_KEY_COLUMN + " = ?";

  // Query to find a metadata key by its name.
  private static final String FIND_MD_KEY_QUERY = "select "
      + MD_KEY_SEQ_COLUMN
      + " from " + MD_KEY_TABLE
      + " where " + KEY_NAME_COLUMN + " = ?";

  // Query to add a metadata key.
  private static final String INSERT_MD_KEY_QUERY = "insert into "
      + MD_KEY_TABLE
      + "(" + MD_KEY_SEQ_COLUMN
      + "," + KEY_NAME_COLUMN
      + ") values (default,?)";

  // Query to add a metadata key/value pair.
  private static final String INSERT_MD_QUERY = "insert into "
      + MD_TABLE
      + "(" + MD_ITEM_SEQ_COLUMN
      + "," + MD_KEY_SEQ_COLUMN
      + "," + MD_VALUE_COLUMN
      + ") values (?,?,?)";

  private DbManager dbManager;

  /**
   * Constructor.
   * 
   * @param dbManager
   *          A DbManager with the database manager.
   */
  MetadataManagerSql(DbManager dbManager) {
    this.dbManager = dbManager;
  }

  /**
   * Provides the number of publications in the metadata database.
   * 
   * @return a long with the number of publications in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getPublicationCount() throws DbException {
    final String DEBUG_HEADER = "getPublicationCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    // Get a connection to the database.
    Connection conn = dbManager.getConnection();

    try {
      rowCount = getPublicationCount(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the number of publications in the metadata database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the number of publications in the metadata database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long getPublicationCount(Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublicationCount(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    long rowCount = -1;

    PreparedStatement stmt =
	dbManager.prepareStatement(conn, COUNT_PUBLICATION_QUERY);
    ResultSet resultSet = null;

    try {
      resultSet = dbManager.executeQuery(stmt);
      resultSet.next();
      rowCount = resultSet.getLong(1);
    } catch (SQLException sqle) {
      String message = "Cannot get the count of publications";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_PUBLICATION_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides the identifier of a plugin if existing or after creating it
   * otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @param platformSeq
   *          A Long with the publishing platform identifier.
   * @param isBulkContent
   *          A boolean with the indication of bulk content for the plugin.
   * @return a Long with the identifier of the plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findOrCreatePlugin(Connection conn, String pluginId, Long platformSeq,
      boolean isBulkContent) throws DbException {
    final String DEBUG_HEADER = "findOrCreatePlugin(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pluginId = " + pluginId);
      log.debug2(DEBUG_HEADER + "platformSeq = " + platformSeq);
      log.debug2(DEBUG_HEADER + "isBulkContent = " + isBulkContent);
    }

    Long pluginSeq = findPlugin(conn, pluginId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginSeq = " + pluginSeq);

    // Check whether it is a new plugin.
    if (pluginSeq == null) {
      // Yes: Add to the database the new plugin.
      pluginSeq = addPlugin(conn, pluginId, platformSeq, isBulkContent);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "new pluginSeq = " + pluginSeq);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "pluginSeq = " + pluginSeq);
    return pluginSeq;
  }

  /**
   * Provides the identifier of a plugin.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginKey
   *          A String with the plugin key.
   * @return a Long with the identifier of the plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findPlugin(Connection conn, String pluginKey) throws DbException {
    final String DEBUG_HEADER = "findPlugin(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "pluginKey = " + pluginKey);
    Long pluginSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findPlugin =
	dbManager.prepareStatement(conn, FIND_PLUGIN_QUERY);

    try {
      findPlugin.setString(1, pluginKey);

      resultSet = dbManager.executeQuery(findPlugin);
      if (resultSet.next()) {
	pluginSeq = resultSet.getLong(PLUGIN_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find plugin";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PLUGIN_QUERY + "'.");
      log.error("pluginKey = " + pluginKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPlugin);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "pluginSeq = " + pluginSeq);
    return pluginSeq;
  }

  /**
   * Adds a plugin to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @param platformSeq
   *          A Long with the publishing platform identifier.
   * @param isBulkContent
   *          A boolean with the indication of bulk content for the plugin.
   * @return a Long with the identifier of the plugin just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long addPlugin(Connection conn, String pluginId, Long platformSeq,
      boolean isBulkContent) throws DbException {
    final String DEBUG_HEADER = "addPlugin(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pluginId = " + pluginId);
      log.debug2(DEBUG_HEADER + "platformSeq = " + platformSeq);
      log.debug2(DEBUG_HEADER + "isBulkContent = " + isBulkContent);
    }

    Long pluginSeq = null;
    ResultSet resultSet = null;

    PreparedStatement insertPlugin = dbManager.prepareStatement(conn,
	INSERT_PLUGIN_QUERY, Statement.RETURN_GENERATED_KEYS);

    try {
      // skip auto-increment key field #0
      insertPlugin.setString(1, pluginId);

      if (platformSeq != null) {
	insertPlugin.setLong(2, platformSeq);
      } else {
	insertPlugin.setNull(2, BIGINT);
      }

      insertPlugin.setBoolean(3, isBulkContent);

      dbManager.executeUpdate(insertPlugin);
      resultSet = insertPlugin.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create plugin table row.");
	return null;
      }

      pluginSeq = resultSet.getLong(1);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Added pluginSeq = " + pluginSeq);
    } catch (SQLException sqle) {
      String message = "Cannot add plugin";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_PLUGIN_QUERY + "'.");
      log.error("pluginId = " + pluginId);
      log.error("platformSeq = " + platformSeq);
      log.error("isBulkContent = " + isBulkContent);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertPlugin);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "pluginSeq = " + pluginSeq);
    return pluginSeq;
  }

  /**
   * Provides the identifier of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginSeq
   *          A Long with the identifier of the plugin.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a Long with the identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findAu(Connection conn, Long pluginSeq, String auKey)
      throws DbException {
    final String DEBUG_HEADER = "findAu(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pluginSeq = " + pluginSeq);
      log.debug2(DEBUG_HEADER + "auKey = " + auKey);
    }

    ResultSet resultSet = null;
    Long auSeq = null;

    PreparedStatement findAu = dbManager.prepareStatement(conn, FIND_AU_QUERY);

    try {
      findAu.setLong(1, pluginSeq);
      findAu.setString(2, auKey);
      resultSet = dbManager.executeQuery(findAu);
      if (resultSet.next()) {
	auSeq = resultSet.getLong(AU_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found auSeq = " + auSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find AU";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_AU_QUERY + "'.");
      log.error("pluginSeq = " + pluginSeq);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);
    return auSeq;
  }

  /**
   * Adds an Archival Unit to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginSeq
   *          A Long with the identifier of the plugin.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a Long with the identifier of the Archival Unit just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addAu(Connection conn, Long pluginSeq, String auKey) throws DbException {
    final String DEBUG_HEADER = "addAu(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pluginSeq = " + pluginSeq);
      log.debug2(DEBUG_HEADER + "auKey = " + auKey);
    }

    ResultSet resultSet = null;
    Long auSeq = null;

    PreparedStatement insertAu = dbManager.prepareStatement(conn,
	INSERT_AU_QUERY, Statement.RETURN_GENERATED_KEYS);

    try {
      // skip auto-increment key field #0
      insertAu.setLong(1, pluginSeq);
      insertAu.setString(2, auKey);
      dbManager.executeUpdate(insertAu);
      resultSet = insertAu.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create AU table row for AU key " + auKey);
	return null;
      }

      auSeq = resultSet.getLong(1);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Added auSeq = " + auSeq);
    } catch (SQLException sqle) {
      String message = "Cannot add AU";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_AU_QUERY + "'.");
      log.error("pluginSeq = " + pluginSeq);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);
    return auSeq;
  }

  /**
   * Adds an Archival Unit metadata to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the identifier of the Archival Unit.
   * @param version
   *          An int with the metadata version.
   * @param extractTime
   *          A long with the extraction time of the metadata.
   * @param creationTime
   *          A long with the creation time of the archival unit.
   * @param providerSeq
   *          A Long with the identifier of the Archival Unit provider.
   * @return a Long with the identifier of the Archival Unit metadata just
   *         added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addAuMd(Connection conn, Long auSeq, int version, long extractTime,
      long creationTime, Long providerSeq) throws DbException {
    final String DEBUG_HEADER = "addAuMd(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);
      log.debug2(DEBUG_HEADER + "version = " + version);
      log.debug2(DEBUG_HEADER + "extractTime = " + extractTime);
      log.debug2(DEBUG_HEADER + "creationTime = " + creationTime);
      log.debug2(DEBUG_HEADER + "providerSeq = " + providerSeq);
    }

    ResultSet resultSet = null;
    Long auMdSeq = null;

    PreparedStatement insertAuMd = dbManager.prepareStatement(conn,
	INSERT_AU_MD_QUERY, Statement.RETURN_GENERATED_KEYS);

    try {
      // skip auto-increment key field #0
      insertAuMd.setLong(1, auSeq);
      insertAuMd.setShort(2, (short) version);
      insertAuMd.setLong(3, extractTime);
      insertAuMd.setLong(4, creationTime);
      insertAuMd.setLong(5, providerSeq);
      dbManager.executeUpdate(insertAuMd);
      resultSet = insertAuMd.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create AU_MD table row for auSeq " + auSeq);
	return null;
      }

      auMdSeq = resultSet.getLong(1);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Added auMdSeq = " + auMdSeq);
    } catch (SQLException sqle) {
      String message = "Cannot add AU metadata";
      log.error(message, sqle);
      log.error("sql = " + INSERT_AU_MD_QUERY);
      log.error("auSeq = " + auSeq);
      log.error("version = " + version);
      log.error("extractTime = " + extractTime);
      log.error("creationTime = " + creationTime);
      log.error("providerSeq = " + providerSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertAuMd);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
    return auMdSeq;
  }

  /**
   * Adds a publication to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param parentMdItemSeq
   *          A Long with the publication parent metadata item parent
   *          identifier.
   * @param mdItemType
   *          A String with the type of publication.
   * @param title
   *          A String with the title of the publication.
   * @return a Long with the identifier of the publication just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addPublication(Connection conn, Long publisherSeq, Long parentMdItemSeq,
      String mdItemType, String title) throws DbException {
    final String DEBUG_HEADER = "addPublication(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "parentMdItemSeq = " + parentMdItemSeq);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
      log.debug2(DEBUG_HEADER + "title = " + title);
    }

    Long publicationSeq = null;

    Long mdItemTypeSeq = findMetadataItemType(conn, mdItemType);
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "mdItemTypeSeq = " + mdItemTypeSeq);

    if (mdItemTypeSeq == null) {
	log.error("Unable to find the metadata item type " + mdItemType);
	return null;
    }

    Long mdItemSeq =
	addMdItem(conn, parentMdItemSeq, mdItemTypeSeq, null, null, null, -1);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    if (mdItemSeq == null) {
	log.error("Unable to create metadata item table row.");
	return null;
    }

    addMdItemName(conn, mdItemSeq, title, PRIMARY_NAME_TYPE);

    ResultSet resultSet = null;

    PreparedStatement insertPublication = dbManager.prepareStatement(conn,
	INSERT_PUBLICATION_QUERY, Statement.RETURN_GENERATED_KEYS);

    try {
      // skip auto-increment key field #0
      insertPublication.setLong(1, mdItemSeq);
      insertPublication.setLong(2, publisherSeq);
      dbManager.executeUpdate(insertPublication);
      resultSet = insertPublication.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create publication table row.");
	return null;
      }

      publicationSeq = resultSet.getLong(1);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Added publicationSeq = " + publicationSeq);
    } catch (SQLException sqle) {
      String message = "Cannot insert publication";
      log.error(message, sqle);
      log.error("parentMdItemSeq = " + parentMdItemSeq);
      log.error("mdItemType = " + mdItemType);
      log.error("title = " + title);
      log.error("SQL = '" + INSERT_PUBLICATION_QUERY + "'.");
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      log.error("publisherSeq = '" + publisherSeq + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertPublication);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of the metadata item of a publication.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publicationSeq
   *          A Long with the identifier of the publication.
   * @return a Long with the identifier of the metadata item of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findPublicationMetadataItem(Connection conn, Long publicationSeq)
      throws DbException {
    final String DEBUG_HEADER = "findPublicationMetadataItem(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    Long mdItemSeq = null;
    PreparedStatement findMdItem =
	dbManager.prepareStatement(conn, FIND_PUBLICATION_METADATA_ITEM_QUERY);
    ResultSet resultSet = null;

    try {
      findMdItem.setLong(1, publicationSeq);

      resultSet = dbManager.executeQuery(findMdItem);
      if (resultSet.next()) {
	mdItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find publication metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PUBLICATION_METADATA_ITEM_QUERY + "'.");
      log.error("publicationSeq = " + publicationSeq + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findMdItem);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }

  /**
   * Provides the identifier of the parent of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mditemSeq
   *          A Long with the identifier of the metadata item.
   * @return a Long with the identifier of the parent of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findParentMetadataItem(Connection conn, Long mditemSeq)
      throws DbException {
    final String DEBUG_HEADER = "findParentMetadataItem(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mditemSeq = " + mditemSeq);

    Long mdParentItemSeq = null;
    PreparedStatement findParentMdItem =
        dbManager.prepareStatement(conn, FIND_PARENT_METADATA_ITEM_QUERY);
    ResultSet resultSet = null;

    try {
      findParentMdItem.setLong(1, mditemSeq);

      resultSet = dbManager.executeQuery(findParentMdItem);
      if (resultSet.next()) {
        mdParentItemSeq = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
        if (log.isDebug3())
          log.debug3(DEBUG_HEADER + "mdParentItemSeq = " + mdParentItemSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find parent metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PARENT_METADATA_ITEM_QUERY + "'.");
      log.error("mditemSeq = " + mditemSeq + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findParentMdItem);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "mdParentItemSeq = " + mdParentItemSeq);
    return mdParentItemSeq;
  }

  /**
   * Adds to the database the ISSNs of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param pIssn
   *          A String with the print ISSN of the metadata item.
   * @param eIssn
   *          A String with the electronic ISSN of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdItemIssns(Connection conn, Long mdItemSeq, String pIssn,
      String eIssn) throws DbException {
    final String DEBUG_HEADER = "addMdItemIssns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
    }

    if (pIssn == null && eIssn == null) {
      return;
    }

    PreparedStatement insertIssn =
	dbManager.prepareStatement(conn, INSERT_ISSN_QUERY);

    try {
      if (pIssn != null) {
	insertIssn.setLong(1, mdItemSeq);
	insertIssn.setString(2, pIssn);
	insertIssn.setString(3, P_ISSN_TYPE);
	int count = dbManager.executeUpdate(insertIssn);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Added PISSN = " + pIssn);
	}

	insertIssn.clearParameters();
      }

      if (eIssn != null) {
	insertIssn.setLong(1, mdItemSeq);
	insertIssn.setString(2, eIssn);
	insertIssn.setString(3, E_ISSN_TYPE);
	int count = dbManager.executeUpdate(insertIssn);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Added EISSN = " + eIssn);
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot add metadata item ISSNs";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_ISSN_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq);
      log.error("pIssn = " + pIssn);
      log.error("eIssn = " + eIssn);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertIssn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Adds to the database the ISBNs of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param pIsbn
   *          A String with the print ISBN of the metadata item.
   * @param eIsbn
   *          A String with the electronic ISBN of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdItemIsbns(Connection conn, Long mdItemSeq, String pIsbn,
      String eIsbn) throws DbException {
    final String DEBUG_HEADER = "addMdItemIsbns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
    }

    if (pIsbn == null && eIsbn == null) {
      return;
    }

    PreparedStatement insertIsbn =
	dbManager.prepareStatement(conn, INSERT_ISBN_QUERY);

    try {
      if (pIsbn != null) {
	insertIsbn.setLong(1, mdItemSeq);
	insertIsbn.setString(2, pIsbn);
	insertIsbn.setString(3, P_ISBN_TYPE);
	int count = dbManager.executeUpdate(insertIsbn);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Added PISBN = " + pIsbn);
	}

	insertIsbn.clearParameters();
      }

      if (eIsbn != null) {
	insertIsbn.setLong(1, mdItemSeq);
	insertIsbn.setString(2, eIsbn);
	insertIsbn.setString(3, E_ISBN_TYPE);
	int count = dbManager.executeUpdate(insertIsbn);

	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "count = " + count);
	  log.debug3(DEBUG_HEADER + "Added EISBN = " + eIsbn);
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot add metadata item ISBNs";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_ISBN_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq);
      log.error("pIssn = " + pIsbn);
      log.error("eIssn = " + eIsbn);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertIsbn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the ISSNs of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @return a Set<Issn> with the ISSNs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Set<Issn> getMdItemIssns(Connection conn, Long mdItemSeq) throws DbException {
    final String DEBUG_HEADER = "getMdItemIssns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    Set<Issn> issns = new HashSet<Issn>();

    PreparedStatement findIssns =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_ISSN_QUERY);

    ResultSet resultSet = null;
    Issn issn;

    try {
      // Get the metadata item ISSNs.
      findIssns.setLong(1, mdItemSeq);
      resultSet = dbManager.executeQuery(findIssns);

      // Loop through the results.
      while (resultSet.next()) {
	// Get the next ISSN.
	issn = new Issn(resultSet.getString(ISSN_COLUMN),
	    resultSet.getString(ISSN_TYPE_COLUMN));
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found " + issn);

	// Add it to the results.
	issns.add(issn);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find metadata item ISSNs";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_ISSN_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findIssns);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "issns = " + issns);
    return issns;
  }

  /**
   * Provides the proprietary identifiers of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @return A Collection<String> with the proprietary identifiers of the
   *         metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getMdItemProprietaryIds(Connection conn, Long mdItemSeq)
      throws DbException {
    final String DEBUG_HEADER = "getMdItemProprietaryIds(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    List<String> proprietaryIds = new ArrayList<String>();

    PreparedStatement findMdItemProprietaryId =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_PROPRIETARY_ID_QUERY);

    ResultSet resultSet = null;

    try {
      // Get the existing proprietary identifiers.
      findMdItemProprietaryId.setLong(1, mdItemSeq);
      resultSet = dbManager.executeQuery(findMdItemProprietaryId);

      while (resultSet.next()) {
	proprietaryIds.add(resultSet.getString(PROPRIETARY_ID_COLUMN));
      }
    } catch (SQLException sqle) {
      String message =
	  "Cannot get the proprietary identifiers of a metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_PROPRIETARY_ID_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findMdItemProprietaryId);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "proprietaryIds = " + proprietaryIds);
    return proprietaryIds;
  }

  /**
   * Provides the ISBNs of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @return a Set<Isbn> with the ISBNs.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Set<Isbn> getMdItemIsbns(Connection conn, Long mdItemSeq) throws DbException {
    final String DEBUG_HEADER = "getMdItemIsbns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    Set<Isbn> isbns = new HashSet<Isbn>();

    PreparedStatement findIsbns =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_ISBN_QUERY);

    ResultSet resultSet = null;
    Isbn isbn;

    try {
      // Get the metadata item ISBNs.
      findIsbns.setLong(1, mdItemSeq);
      resultSet = dbManager.executeQuery(findIsbns);

      // Loop through the results.
      while (resultSet.next()) {
	// Get the next ISBN.
	isbn = new Isbn(resultSet.getString(ISBN_COLUMN),
	    resultSet.getString(ISBN_TYPE_COLUMN));
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Found " + isbn);

	// Add it to the results.
	isbns.add(isbn);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find metadata item ISBNs";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_ISBN_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findIsbns);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "isbns = " + isbns);
    return isbns;
  }

  /**
   * Provides the identifier of a publication by its publisher and ISSNs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the publication.
   * @param eIssn
   *          A String with the electronic ISSN of the publication.
   * @param mdItemType
   *          A String with the type of publication to be identified.
   * @return a Long with the identifier of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findPublicationByIssns(Connection conn, Long publisherSeq, String pIssn,
      String eIssn, String mdItemType) throws DbException {
    final String DEBUG_HEADER = "findPublicationByIssns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
    }

    Long publicationSeq = null;
    ResultSet resultSet = null;
    PreparedStatement findPublicationByIssns =
	dbManager.prepareStatement(conn, FIND_PUBLICATION_BY_ISSNS_QUERY);

    try {
      findPublicationByIssns.setLong(1, publisherSeq);
      findPublicationByIssns.setString(2, pIssn);
      findPublicationByIssns.setString(3, eIssn);
      findPublicationByIssns.setString(4, mdItemType);

      resultSet = dbManager.executeQuery(findPublicationByIssns);
      if (resultSet.next()) {
	publicationSeq = resultSet.getLong(PUBLICATION_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find publication";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PUBLICATION_BY_ISSNS_QUERY + "'.");
      log.error("publisherSeq = " + publisherSeq + ".");
      log.error("pIssn = " + pIssn);
      log.error("eIssn = " + eIssn);
      log.error("mdItemType = " + mdItemType);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPublicationByIssns);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of a publication by its publisher and ISBNs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIsbn
   *          A String with the print ISBN of the publication.
   * @param eIsbn
   *          A String with the electronic ISBN of the publication.
   * @param mdItemType
   *          A String with the type of publication to be identified.
   * @return a Long with the identifier of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findPublicationByIsbns(Connection conn, Long publisherSeq, String pIsbn,
      String eIsbn, String mdItemType) throws DbException {
    final String DEBUG_HEADER = "findPublicationByIsbns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
    }

    Long publicationSeq = null;
    ResultSet resultSet = null;
    PreparedStatement findPublicationByIsbns =
	dbManager.prepareStatement(conn, FIND_PUBLICATION_BY_ISBNS_QUERY);

    try {
      findPublicationByIsbns.setLong(1, publisherSeq);
      findPublicationByIsbns.setString(2, pIsbn);
      findPublicationByIsbns.setString(3, eIsbn);
      findPublicationByIsbns.setString(4, mdItemType);

      resultSet = dbManager.executeQuery(findPublicationByIsbns);
      if (resultSet.next()) {
	publicationSeq = resultSet.getLong(PUBLICATION_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find publication";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PUBLICATION_BY_ISBNS_QUERY + "'.");
      log.error("publisherSeq = " + publisherSeq);
      log.error("pIsbn = " + pIsbn);
      log.error("eIsbn = " + eIsbn);
      log.error("mdItemType = " + mdItemType);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPublicationByIsbns);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of a publication by its title and publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param title
   *          A String with the title of the publication.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param mdItemType
   *          A String with the type of publication to be identified.
   * @return a Long with the identifier of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findPublicationByName(Connection conn, Long publisherSeq, String title,
      String mdItemType) throws DbException {
    final String DEBUG_HEADER = "findPublicationByName(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
    }

    Long publicationSeq = null;
    ResultSet resultSet = null;
    PreparedStatement findPublicationByName =
	dbManager.prepareStatement(conn, FIND_PUBLICATION_BY_NAME_QUERY);

    try {
      findPublicationByName.setLong(1, publisherSeq);
      findPublicationByName.setString(2, title);
      findPublicationByName.setString(3, mdItemType);

      resultSet = dbManager.executeQuery(findPublicationByName);
      if (resultSet.next()) {
	publicationSeq = resultSet.getLong(PUBLICATION_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find publication";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PUBLICATION_BY_NAME_QUERY + "'.");
      log.error("publisherSeq = '" + publisherSeq + "'.");
      log.error("title = " + title);
      log.error("mdItemType = " + mdItemType);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPublicationByName);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides an indication of whether a publication has ISBNs in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publicationSeq
   *          A Long with the publication identifier.
   * @return a boolean with <code>true</code> if the publication has ISBNs,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean publicationHasIsbns(Connection conn, Long publicationSeq)
      throws DbException {
    final String DEBUG_HEADER = "publicationHasIsbns(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    long rowCount = -1;
    ResultSet results = null;
    PreparedStatement countIsbns =
	dbManager.prepareStatement(conn, COUNT_PUBLICATION_ISBNS_QUERY);

    try {
      countIsbns.setLong(1, publicationSeq);

      // Find the ISBNs.
      results = dbManager.executeQuery(countIsbns);
      results.next();
      rowCount = results.getLong(1);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "rowCount = " + rowCount);
    } catch (SQLException sqle) {
      String message = "Cannot count publication ISBNs";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_PUBLICATION_ISBNS_QUERY + "'.");
      log.error("publicationSeq = " + publicationSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(countIsbns);
    }

    boolean result = rowCount > 0;
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides an indication of whether a publication has ISSNs in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publicationSeq
   *          A Long with the publication identifier.
   * @return a boolean with <code>true</code> if the publication has ISSNs,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean publicationHasIssns(Connection conn, Long publicationSeq)
      throws DbException {
    final String DEBUG_HEADER = "publicationHasIssns(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    long rowCount = -1;
    ResultSet results = null;
    PreparedStatement countIssns =
	dbManager.prepareStatement(conn, COUNT_PUBLICATION_ISSNS_QUERY);

    try {
      countIssns.setLong(1, publicationSeq);

      // Find the ISSNs.
      results = dbManager.executeQuery(countIssns);
      results.next();
      rowCount = results.getLong(1);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "rowCount = " + rowCount);
    } catch (SQLException sqle) {
      String message = "Cannot count publication ISSNs";
      log.error(message, sqle);
      log.error("SQL = '" + COUNT_PUBLICATION_ISSNS_QUERY + "'.");
      log.error("publicationSeq = " + publicationSeq);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(countIssns);
    }

    boolean result = rowCount > 0;
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the identifier of a metadata item type by its name.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param typeName
   *          A String with the name of the metadata item type.
   * @return a Long with the identifier of the metadata item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findMetadataItemType(Connection conn, String typeName)
      throws DbException {
    final String DEBUG_HEADER = "findMetadataItemType(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "typeName = " + typeName);

    Long mdItemTypeSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findMdItemType =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_TYPE_QUERY);

    try {
      findMdItemType.setString(1, typeName);

      resultSet = dbManager.executeQuery(findMdItemType);
      if (resultSet.next()) {
	mdItemTypeSeq = resultSet.getLong(MD_ITEM_TYPE_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find metadata item type";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_TYPE_QUERY + "'.");
      log.error("typeName = '" + typeName + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findMdItemType);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "mdItemTypeSeq = " + mdItemTypeSeq);
    return mdItemTypeSeq;
  }

  /**
   * Adds a metadata item to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param parentSeq
   *          A Long with the metadata item parent identifier.
   * @param auMdSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @param mdItemTypeSeq
   *          A Long with the identifier of the type of metadata item.
   * @param date
   *          A String with the publication date of the metadata item.
   * @param coverage
   *          A String with the metadata item coverage.
   * @param fetchTime
   *          A long with the fetch time of metadata item.
   * @return a Long with the identifier of the metadata item just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addMdItem(Connection conn, Long parentSeq, Long mdItemTypeSeq,
      Long auMdSeq, String date, String coverage, long fetchTime)
	  throws DbException {
    final String DEBUG_HEADER = "addMdItem(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "parentSeq = " + parentSeq);
      log.debug2(DEBUG_HEADER + "mdItemTypeSeq = " + mdItemTypeSeq);
      log.debug2(DEBUG_HEADER + "auMdSeq = " + auMdSeq);
      log.debug2(DEBUG_HEADER + "date = " + date);
      log.debug2(DEBUG_HEADER + "coverage = " + coverage);
      log.debug2(DEBUG_HEADER + "fetchTime = " + fetchTime);
    }

    PreparedStatement insertMdItem = dbManager.prepareStatement(conn,
	INSERT_MD_ITEM_QUERY, Statement.RETURN_GENERATED_KEYS);

    ResultSet resultSet = null;
    Long mdItemSeq = null;

    try {
      // skip auto-increment key field #0
      if (parentSeq != null) {
	insertMdItem.setLong(1, parentSeq);
      } else {
	insertMdItem.setNull(1, BIGINT);
      }
      insertMdItem.setLong(2, mdItemTypeSeq);
      if (auMdSeq != null) {
	insertMdItem.setLong(3, auMdSeq);
      } else {
	insertMdItem.setNull(3, BIGINT);
      }
      insertMdItem.setString(4, date);
      insertMdItem.setString(5, coverage);
      insertMdItem.setLong(6, fetchTime);
      dbManager.executeUpdate(insertMdItem);
      resultSet = insertMdItem.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create metadata item table row.");
	return null;
      }

      mdItemSeq = resultSet.getLong(1);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Added mdItemSeq = " + mdItemSeq);
    } catch (SQLException sqle) {
      String message = "Cannot insert metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_MD_ITEM_QUERY + "'.");
      log.error("parentSeq = " + parentSeq + ".");
      log.error("mdItemTypeSeq = " + mdItemTypeSeq + ".");
      log.error("auMdSeq = " + auMdSeq + ".");
      log.error("date = '" + date + "'.");
      log.error("coverage = '" + coverage + "'.");
      log.error("fetchTime = " + fetchTime);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertMdItem);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }

  /**
   * Provides the names of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @return a Map<String, String> with the names and name types of the metadata
   *         item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, String> getMdItemNames(Connection conn, Long mdItemSeq)
      throws DbException {
    final String DEBUG_HEADER = "getMdItemNames(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    Map<String, String> names = new HashMap<String, String>();
    PreparedStatement getNames =
	dbManager.prepareStatement(conn, FIND_MD_ITEM_NAME_QUERY);
    ResultSet resultSet = null;

    try {
      getNames.setLong(1, mdItemSeq);
      resultSet = dbManager.executeQuery(getNames);
      while (resultSet.next()) {
	names.put(resultSet.getString(NAME_COLUMN),
		  resultSet.getString(NAME_TYPE_COLUMN));
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Found metadata item name = '" + resultSet.getString(NAME_COLUMN)
	    + "' of type '" + resultSet.getString(NAME_TYPE_COLUMN) + "'.");
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the names of a metadata item";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_ITEM_NAME_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getNames);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "names = " + names);
    return names;
  }

  /**
   * Adds a metadata item name to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param name
   *          A String with the name of the metadata item.
   * @param type
   *          A String with the type of name of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdItemName(Connection conn, Long mdItemSeq, String name, String type)
      throws DbException {
    final String DEBUG_HEADER = "addMdItemName(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "name = " + name);
      log.debug2(DEBUG_HEADER + "type = " + type);
    }

    if (name == null || type == null) {
      return;
    }

    PreparedStatement insertMdItemName =
	dbManager.prepareStatement(conn, INSERT_MD_ITEM_NAME_QUERY);

    try {
      insertMdItemName.setLong(1, mdItemSeq);
      insertMdItemName.setString(2, name);
      insertMdItemName.setString(3, type);
      int count = dbManager.executeUpdate(insertMdItemName);

      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER + "count = " + count);
	log.debug3(DEBUG_HEADER + "Added metadata item name = " + name);
      }
    } catch (SQLException sqle) {
      String message = "Cannot add a metadata item name";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_MD_ITEM_NAME_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq + ".");
      log.error("name = " + name + ".");
      log.error("type = " + type + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemName);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Adds to the database a metadata item URL.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param feature
   *          A String with the feature of the metadata item URL.
   * @param url
   *          A String with the metadata item URL.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdItemUrl(Connection conn, Long mdItemSeq, String feature, String url)
      throws DbException {
    final String DEBUG_HEADER = "addMdItemUrl(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "feature = " + feature);
      log.debug2(DEBUG_HEADER + "url = " + url);
    }

    PreparedStatement insertMdItemUrl =
	dbManager.prepareStatement(conn, INSERT_URL_QUERY);

    try {
      insertMdItemUrl.setLong(1, mdItemSeq);
      insertMdItemUrl.setString(2, feature);
      insertMdItemUrl.setString(3, url);
      int count = dbManager.executeUpdate(insertMdItemUrl);

      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER + "count = " + count);
	log.debug3(DEBUG_HEADER + "Added URL = " + url);
      }
    } catch (SQLException sqle) {
      String message = "Cannot add a metadata item URL";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_URL_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq + ".");
      log.error("feature = " + feature + ".");
      log.error("url = " + url + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemUrl);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Adds to the database a metadata item DOI.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param doi
   *          A String with the DOI of the metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdItemDoi(Connection conn, Long mdItemSeq, String doi)
      throws DbException {
    final String DEBUG_HEADER = "addMdItemDoi(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "doi = " + doi);
    }

    if (StringUtil.isNullString(doi)) {
      return;
    }

    PreparedStatement insertMdItemDoi =
	dbManager.prepareStatement(conn, INSERT_DOI_QUERY);

    try {
      insertMdItemDoi.setLong(1, mdItemSeq);
      insertMdItemDoi.setString(2, doi);
      int count = dbManager.executeUpdate(insertMdItemDoi);

      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER + "count = " + count);
	log.debug3(DEBUG_HEADER + "Added DOI = " + doi);
      }
    } catch (SQLException sqle) {
      String message = "Cannot add a metadata item DOI";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_DOI_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq + ".");
      log.error("doi = " + doi + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemDoi);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the identifier of a platform.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param platformName
   *          A String with the platform identifier.
   * @return a Long with the identifier of the platform.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findPlatform(Connection conn, String platformName) throws DbException {
    final String DEBUG_HEADER = "findPlatform(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "platformName = " + platformName);

    Long platformSeq = null;
    ResultSet resultSet = null;

    PreparedStatement findPlatform =
	dbManager.prepareStatement(conn, FIND_PLATFORM_QUERY);

    try {
      findPlatform.setString(1, platformName);

      resultSet = dbManager.executeQuery(findPlatform);
      if (resultSet.next()) {
	platformSeq = resultSet.getLong(PLATFORM_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find platform";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PLATFORM_QUERY + "'.");
      log.error("platformName = '" + platformName + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPlatform);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "platformSeq = " + platformSeq);
    return platformSeq;
  }

  /**
   * Adds a platform to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param platformName
   *          A String with the platform name.
   * @return a Long with the identifier of the platform just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addPlatform(Connection conn, String platformName) throws DbException {
    final String DEBUG_HEADER = "addPlatform(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "platformName = " + platformName);

    Long platformSeq = null;
    ResultSet resultSet = null;
    PreparedStatement insertPlatform = dbManager.prepareStatement(conn,
	INSERT_PLATFORM_QUERY, Statement.RETURN_GENERATED_KEYS);

    try {
      // Skip auto-increment key field #0
      insertPlatform.setString(1, platformName);
      dbManager.executeUpdate(insertPlatform);
      resultSet = insertPlatform.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create platform table row.");
	return null;
      }

      platformSeq = resultSet.getLong(1);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Added platformSeq = " + platformSeq);
    } catch (SQLException sqle) {
      String message = "Cannot add platform";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_PLATFORM_QUERY + "'.");
      log.error("platformName = '" + platformName + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertPlatform);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "platformSeq = " + platformSeq);
    return platformSeq;
  }

  /**
   * Adds an Archival Unit to the table of unconfigured Archival Units.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void persistUnconfiguredAu(Connection conn, String auId) throws DbException {
    final String DEBUG_HEADER = "persistUnconfiguredAu(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    PreparedStatement insertUnconfiguredAu = null;
    String pluginKey = null;
    String auKey = null;

    try {
      insertUnconfiguredAu =
	  dbManager.prepareStatement(conn, INSERT_UNCONFIGURED_AU_QUERY);

      pluginKey = PluginManager.pluginKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginKey = " + pluginKey);
      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      insertUnconfiguredAu.setString(1, pluginKey);
      insertUnconfiguredAu.setString(2, auKey);
      int count = dbManager.executeUpdate(insertUnconfiguredAu);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
    } catch (SQLException sqle) {
      String message = "Cannot insert archival unit in unconfigured table";
      log.error(message, sqle);
      log.error("auId = " + auId);
      log.error("SQL = '" + INSERT_UNCONFIGURED_AU_QUERY + "'.");
      log.error("pluginKey = " + pluginKey);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } catch (DbException dbe) {
      String message = "Cannot insert archival unit in unconfigured table";
      log.error(message, dbe);
      log.error("auId = " + auId);
      log.error("SQL = '" + INSERT_UNCONFIGURED_AU_QUERY + "'.");
      log.error("pluginKey = " + pluginKey);
      log.error("auKey = " + auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseStatement(insertUnconfiguredAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the count of recorded unconfigured archival units.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a long with the count of recorded unconfigured archival units.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  long countUnconfiguredAus(Connection conn) throws DbException {
    final String DEBUG_HEADER = "countUnconfiguredAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    long rowCount = -1;
    ResultSet results = null;
    PreparedStatement unconfiguredAu =
	dbManager.prepareStatement(conn, UNCONFIGURED_AU_COUNT_QUERY);

    try {
      // Count the rows in the table.
      results = dbManager.executeQuery(unconfiguredAu);
      results.next();
      rowCount = results.getLong(1);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "rowCount = " + rowCount);
    } catch (SQLException sqle) {
      String message = "Cannot count unconfigured archival units";
      log.error(message, sqle);
      log.error("SQL = '" + UNCONFIGURED_AU_COUNT_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(unconfiguredAu);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "rowCount = " + rowCount);
    return rowCount;
  }

  /**
   * Provides an indication of whether an Archival Unit is in the table of
   * unconfigured Archival Units.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a boolean with <code>true</code> if the Archival Unit is in the
   *         UNCONFIGURED_AU table, <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean isAuInUnconfiguredAuTable(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "isAuInUnconfiguredAuTable(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    String pluginKey = null;
    String auKey = null;
    long rowCount = -1;
    ResultSet results = null;
    PreparedStatement unconfiguredAu =
	dbManager.prepareStatement(conn, FIND_UNCONFIGURED_AU_COUNT_QUERY);

    try {
      pluginKey = PluginManager.pluginKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginKey = " + pluginKey);
      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      unconfiguredAu.setString(1, pluginKey);
      unconfiguredAu.setString(2, auKey);

      // Find the archival unit in the table.
      results = dbManager.executeQuery(unconfiguredAu);
      results.next();
      rowCount = results.getLong(1);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "rowCount = " + rowCount);
    } catch (SQLException sqle) {
      String message = "Cannot find archival unit in unconfigured table";
      log.error(message, sqle);
      log.error("auId = " + auId);
      log.error("SQL = '" + FIND_UNCONFIGURED_AU_COUNT_QUERY + "'.");
      log.error("pluginKey = " + pluginKey);
      log.error("auKey = " + auKey);
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(results);
      DbManager.safeCloseStatement(unconfiguredAu);
    }

    boolean result = rowCount > 0;
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the names of the publishers in the database.
   * 
   * @return a Collection<String> with the publisher names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getPublisherNames() throws DbException {
    final String DEBUG_HEADER = "getPublisherNames(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<String> publisherNames = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the publisher names.
      publisherNames = getPublisherNames(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "publisherNames.size() = "
	+ publisherNames.size());
    return publisherNames;
  }

  /**
   * Provides the names of the publishers in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<String> with the publisher names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getPublisherNames(Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublisherNames(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<String> publisherNames = new ArrayList<String>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      // Get the publisher names.
      stmt = dbManager.prepareStatement(conn, GET_PUBLISHER_NAMES_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the publisher names. 
      while (resultSet.next()) {
	String publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

	publisherNames.add(publisherName);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the publisher names";
      log.error(message, sqle);
      log.error("SQL = '" + GET_PUBLISHER_NAMES_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "publisherNames.size() = "
	+ publisherNames.size());
    return publisherNames;
  }

  /**
   * Provides the DOI prefixes for the publishers in the database with multiple
   * DOI prefixes.
   * 
   * @return a Map<String, Collection<String>> with the DOI prefixes keyed by
   *         the publisher name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getPublishersWithMultipleDoiPrefixes()
      throws DbException {
    final String DEBUG_HEADER = "getPublishersWithMultipleDoiPrefixes(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> publishersDoiPrefixes = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the publisher DOI prefixes.
      publishersDoiPrefixes = getPublishersWithMultipleDoiPrefixes(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publishersDoiPrefixes.size() = " + publishersDoiPrefixes.size());
    return publishersDoiPrefixes;
  }

  /**
   * Provides the DOI prefixes for the publishers in the database with multiple
   * DOI prefixes.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the DOI prefixes keyed by
   *         the publisher name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getPublishersWithMultipleDoiPrefixes(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublishersWithMultipleDoiPrefixes(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> publishersDoiPrefixes =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;
    String sql = null;

    try {
      String previousPublisherName = null;

      // Get the publisher DOI prefixes.
      sql = GET_PUBLISHERS_MULTIPLE_DOI_PREFIXES_DERBY_QUERY;

      if (dbManager.isTypePostgresql()) {
	sql = GET_PUBLISHERS_MULTIPLE_DOI_PREFIXES_PG_QUERY;
      } else if (dbManager.isTypeMysql()) {
	sql = GET_PUBLISHERS_MULTIPLE_DOI_PREFIXES_MYSQL_QUERY;
      }

      stmt = dbManager.prepareStatement(conn, sql);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the publisher DOI prefixes. 
      while (resultSet.next()) {
	String publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

	String prefix = resultSet.getString("prefix");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "prefix = " + prefix);

	if (publisherName.equals(previousPublisherName)) {
	  publishersDoiPrefixes.get(publisherName).add(prefix);
	} else {
	  Collection<String> publisherPrefixes = new ArrayList<String>();
	  publisherPrefixes.add(prefix);
	  publishersDoiPrefixes.put(publisherName, publisherPrefixes);
	  previousPublisherName = publisherName;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the publishers DOI prefixes";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publishersDoiPrefixes.size() = " + publishersDoiPrefixes.size());
    return publishersDoiPrefixes;
  }

  /**
   * Provides the publisher names linked to DOI prefixes in the database that
   * are linked to multiple publishers.
   * 
   * @return a Map<String, Collection<String>> with the publisher names keyed by
   *         the DOI prefixes to which they are linked.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getDoiPrefixesWithMultiplePublishers()
      throws DbException {
    final String DEBUG_HEADER = "getDoiPrefixesWithMultiplePublishers(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> doiPrefixesPublishers = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the DOI prefix publishers.
      doiPrefixesPublishers = getDoiPrefixesWithMultiplePublishers(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publishersDoiPrefixes.size() = " + doiPrefixesPublishers.size());
    return doiPrefixesPublishers;
  }

  /**
   * Provides the publisher names linked to DOI prefixes in the database that
   * are linked to multiple publishers.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the publisher names keyed by
   *         the DOI prefixes to which they are linked.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getDoiPrefixesWithMultiplePublishers(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getDoiPrefixesWithMultiplePublishers(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> doiPrefixesPublishers =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;
    String sql = null;

    try {
      String previousDoiPrefix = null;

      // Get the DOI prefix publishers.
      sql = GET_DOI_PREFIXES_MULTIPLE_PUBLISHERS_DERBY_QUERY;

      if (dbManager.isTypePostgresql()) {
	sql = GET_DOI_PREFIXES_MULTIPLE_PUBLISHERS_PG_QUERY;
      } else if (dbManager.isTypeMysql()) {
	sql = GET_DOI_PREFIXES_MULTIPLE_PUBLISHERS_MYSQL_QUERY;
      }

      stmt = dbManager.prepareStatement(conn, sql);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the DOI prefix publishers.
      while (resultSet.next()) {
	String prefix = resultSet.getString("prefix");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "prefix = " + prefix);

	String publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

	if (prefix.equals(previousDoiPrefix)) {
	  doiPrefixesPublishers.get(prefix).add(publisherName);
	} else {
	  Collection<String> prefixPublishers = new ArrayList<String>();
	  prefixPublishers.add(publisherName);
	  doiPrefixesPublishers.put(prefix, prefixPublishers);
	  previousDoiPrefix = prefix;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the DOI prefixes publishers";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publishersDoiPrefixes.size() = " + doiPrefixesPublishers.size());
    return doiPrefixesPublishers;
  }

  /**
   * Provides the DOI prefixes linked to the Archival Unit identifier for the
   * Archival Units in the database with multiple DOI prefixes.
   * 
   * @return a Map<String, Collection<String>> with the DOI prefixes keyed by
   *         the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getAuIdsWithMultipleDoiPrefixes()
      throws DbException {
    final String DEBUG_HEADER = "getAuIdsWithMultipleDoiPrefixes(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> ausDoiPrefixes = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the Archival Unit DOI prefixes.
      ausDoiPrefixes = getAuIdsWithMultipleDoiPrefixes(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "ausDoiPrefixes.size() = "
	+ ausDoiPrefixes.size());
    return ausDoiPrefixes;
  }

  /**
   * Provides the DOI prefixes linked to the Archival Unit identifier for the
   * Archival Units in the database with multiple DOI prefixes.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the DOI prefixes keyed by
   *         the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getAuIdsWithMultipleDoiPrefixes(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getAuIdsWithMultipleDoiPrefixes(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> ausDoiPrefixes =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;
    String sql = null;

    try {
      String previousAuId = null;

      // Get the Archival Unit DOI prefixes.
      sql = GET_AUS_MULTIPLE_DOI_PREFIXES_DERBY_QUERY;

      if (dbManager.isTypePostgresql()) {
	sql = GET_AUS_MULTIPLE_DOI_PREFIXES_PG_QUERY;
      } else if (dbManager.isTypeMysql()) {
	sql = GET_AUS_MULTIPLE_DOI_PREFIXES_MYSQL_QUERY;
      }

      stmt = dbManager.prepareStatement(conn, sql);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the Archival Unit DOI prefixes. 
      while (resultSet.next()) {
	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	String auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

	String prefix = resultSet.getString("prefix");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "prefix = " + prefix);

	if (auId.equals(previousAuId)) {
	  ausDoiPrefixes.get(auId).add(prefix);
	} else {
	  Collection<String> auPrefixes = new ArrayList<String>();
	  auPrefixes.add(prefix);
	  ausDoiPrefixes.put(auId, auPrefixes);
	  previousAuId = auId;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the Archival Units DOI prefixes";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "ausDoiPrefixes.size() = "
	+ ausDoiPrefixes.size());
    return ausDoiPrefixes;
  }

  /**
   * Provides the ISBNs for the publications in the database with more than two
   * ISBNS.
   * 
   * @return a Map<String, Collection<Isbn>> with the ISBNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<Isbn>> getPublicationsWithMoreThan2Isbns()
      throws DbException {
    final String DEBUG_HEADER = "getPublicationsWithMoreThan2Isbns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<Isbn>> publicationsIsbns = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the publication ISBNs.
      publicationsIsbns = getPublicationsWithMoreThan2Isbns(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publicationsIsbns.size() = " + publicationsIsbns.size());
    return publicationsIsbns;
  }

  /**
   * Provides the ISBNs for the publications in the database with more than two
   * ISBNS.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<Isbn>> with the ISBNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<Isbn>> getPublicationsWithMoreThan2Isbns(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublicationsWithMoreThan2Isbns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<Isbn>> publicationsIsbns =
	new TreeMap<String, Collection<Isbn>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousPublicationName = null;

      // Get the publication ISBNs.
      stmt = dbManager.prepareStatement(conn,
	  GET_PUBLICATIONS_MORE_2_ISBNS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the publication ISBNs.
      while (resultSet.next()) {
	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	String isbn = resultSet.getString(ISBN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbn = " + isbn);

	String isbnType = resultSet.getString(ISBN_TYPE_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbnType = " + isbnType);

	if (publicationName.equals(previousPublicationName)) {
	  publicationsIsbns.get(publicationName).add(new Isbn(isbn, isbnType));
	} else {
	  Collection<Isbn> publicationIsbns = new ArrayList<Isbn>();
	  publicationIsbns.add(new Isbn(isbn, isbnType));
	  publicationsIsbns.put(publicationName, publicationIsbns);
	  previousPublicationName = publicationName;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the publication ISBNs";
      log.error(message, sqle);
      log.error("SQL = '" + GET_PUBLICATIONS_MORE_2_ISBNS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publicationsIsbns.size() = " + publicationsIsbns.size());
    return publicationsIsbns;
  }

  /**
   * Provides the ISSNs for the publications in the database with more than two
   * ISSNS.
   * 
   * @return a Map<PkNamePair, Collection<Issn>> with the ISSNs keyed by the
   *         publication PK/name pair.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<PkNamePair, Collection<Issn>> getPublicationsWithMoreThan2Issns()
      throws DbException {
    final String DEBUG_HEADER = "getPublicationsWithMoreThan2Issns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<PkNamePair, Collection<Issn>> publicationsIssns = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the publication ISSNs.
      publicationsIssns = getPublicationsWithMoreThan2Issns(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publicationsIssns.size() = " + publicationsIssns.size());
    return publicationsIssns;
  }

  /**
   * Provides the ISSNs for the publications in the database with more than two
   * ISSNS.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<PkNamePair, Collection<Issn>> with the ISSNs keyed by the
   *         publication PK/name pair.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<PkNamePair, Collection<Issn>> getPublicationsWithMoreThan2Issns(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublicationsWithMoreThan2Issns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<PkNamePair, Collection<Issn>> publicationsIssns =
	new TreeMap<PkNamePair, Collection<Issn>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      PkNamePair previousPair = null;

      // Get the publication ISSNs.
      stmt = dbManager.prepareStatement(conn,
	  GET_PUBLICATIONS_MORE_2_ISSNS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the publication ISSNs.
      while (resultSet.next()) {
	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	Long pk = resultSet.getLong(MD_ITEM_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pk = " + pk);

	PkNamePair pair = new PkNamePair(pk, publicationName);

	String issn = resultSet.getString(ISSN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issn = " + issn);

	String issnType = resultSet.getString(ISSN_TYPE_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issnType = " + issnType);

	if (pair.equals(previousPair)) {
	  publicationsIssns.get(pair).add(new Issn(issn, issnType));
	} else {
	  Collection<Issn> publicationIssns = new ArrayList<Issn>();
	  publicationIssns.add(new Issn(issn, issnType));
	  publicationsIssns.put(pair, publicationIssns);
	  previousPair = pair;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the publication ISSNs";
      log.error(message, sqle);
      log.error("SQL = '" + GET_PUBLICATIONS_MORE_2_ISSNS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publicationsIssns.size() = " + publicationsIssns.size());
    return publicationsIssns;
  }

  /**
   * Provides the publication names linked to ISBNs in the database that are
   * linked to multiple publications.
   * 
   * @return a Map<String, Collection<String>> with the publication names keyed
   *         by the ISBNs to which they are linked.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getIsbnsWithMultiplePublications()
      throws DbException {
    final String DEBUG_HEADER = "getIsbnsWithMultiplePublications(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> isbnsPublications = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the ISBN publications.
      isbnsPublications = getIsbnsWithMultiplePublications(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "isbnsPublications.size() = " + isbnsPublications.size());
    return isbnsPublications;
  }

  /**
   * Provides the publication names linked to ISBNs in the database that are
   * linked to multiple publications.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the publication names keyed
   *         by the ISBNs to which they are linked.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getIsbnsWithMultiplePublications(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getIsbnsWithMultiplePublications(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> isbnsPublications =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousIsbn = null;

      // Get the ISBN publications.
      stmt = dbManager.prepareStatement(conn,
	  GET_ISBNS_MULTIPLE_PUBLICATIONS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the ISBN publications.
      while (resultSet.next()) {
	String isbn = resultSet.getString(ISBN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbn = " + isbn);

	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	if (isbn.equals(previousIsbn)) {
	  isbnsPublications.get(isbn).add(publicationName);
	} else {
	  Collection<String> isbnPublications = new ArrayList<String>();
	  isbnPublications.add(publicationName);
	  isbnsPublications.put(isbn, isbnPublications);
	  previousIsbn = isbn;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the ISBN publications";
      log.error(message, sqle);
      log.error("SQL = '" + GET_ISBNS_MULTIPLE_PUBLICATIONS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "isbnsPublications.size() = " + isbnsPublications.size());
    return isbnsPublications;
  }

  /**
   * Provides the publication names linked to ISSNs in the database that are
   * linked to multiple publications.
   * 
   * @return a Map<String, Collection<String>> with the publication names keyed
   *         by the ISSNs to which they are linked.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getIssnsWithMultiplePublications()
      throws DbException {
    final String DEBUG_HEADER = "getIssnsWithMultiplePublications(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> issnsPublications = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the ISSN publications.
      issnsPublications = getIssnsWithMultiplePublications(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "issnsPublications.size() = " + issnsPublications.size());
    return issnsPublications;
  }

  /**
   * Provides the publication names linked to ISSNs in the database that are
   * linked to multiple publications.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the publication names keyed
   *         by the ISSNs to which they are linked.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getIssnsWithMultiplePublications(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getIssnsWithMultiplePublications(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> issnsPublications =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousIssn = null;

      // Get the ISSN publications.
      stmt = dbManager.prepareStatement(conn,
	  GET_ISSNS_MULTIPLE_PUBLICATIONS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the ISSN publications.
      while (resultSet.next()) {
	String issn = resultSet.getString(ISSN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issn = " + issn);

	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	if (issn.equals(previousIssn)) {
	  issnsPublications.get(issn).add(publicationName);
	} else {
	  Collection<String> issnPublications = new ArrayList<String>();
	  issnPublications.add(publicationName);
	  issnsPublications.put(issn, issnPublications);
	  previousIssn = issn;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the ISSN publications";
      log.error(message, sqle);
      log.error("SQL = '" + GET_ISSNS_MULTIPLE_PUBLICATIONS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "issnsPublications.size() = " + issnsPublications.size());
    return issnsPublications;
  }

  /**
   * Provides the ISSNs for books in the database.
   * 
   * @return a Map<String, Collection<String>> with the ISSNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getBooksWithIssns() throws DbException {
    final String DEBUG_HEADER = "getBooksWithIssns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> booksWithIssns = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the books with ISSNs.
      booksWithIssns = getBooksWithIssns(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "booksWithIssns.size() = "
	+ booksWithIssns.size());
    return booksWithIssns;
  }

  /**
   * Provides the ISSNs for books in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the ISSNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getBooksWithIssns(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getBooksWithIssns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> booksWithIssns =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousDisplayPublicationName = null;

      // Get the publication ISSNs.
      stmt = dbManager.prepareStatement(conn, GET_BOOKS_WITH_ISSNS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the book ISSNs.
      while (resultSet.next()) {
	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	String publicationTypeName = resultSet.getString(TYPE_NAME_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "publicationTypeName = "
	    + publicationTypeName);

	String displayPublicationName =
	    publicationName + " [" + publicationTypeName.substring(0, 1) + "]";

	String issn = resultSet.getString(ISSN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issn = " + issn);

	if (displayPublicationName.equals(previousDisplayPublicationName)) {
	  booksWithIssns.get(displayPublicationName).add(issn);
	} else {
	  Collection<String> publicationIssns = new ArrayList<String>();
	  publicationIssns.add(issn);
	  booksWithIssns.put(displayPublicationName, publicationIssns);
	  previousDisplayPublicationName = displayPublicationName;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the book ISSNs";
      log.error(message, sqle);
      log.error("SQL = '" + GET_BOOKS_WITH_ISSNS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "booksWithIssns.size() = "
	+ booksWithIssns.size());
    return booksWithIssns;
  }

  /**
   * Provides the ISBNs for periodicals in the database.
   * 
   * @return a Map<String, Collection<String>> with the ISBNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getPeriodicalsWithIsbns() throws DbException {
    final String DEBUG_HEADER = "getPeriodicalsWithIsbns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> periodicalsWithIsbns = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the periodicals with ISBNs.
      periodicalsWithIsbns = getPeriodicalsWithIsbns(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "periodicalsWithIsbns.size() = " + periodicalsWithIsbns.size());
    return periodicalsWithIsbns;
  }

  /**
   * Provides the ISBNs for periodicals in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the ISBNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getPeriodicalsWithIsbns(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getPeriodicalsWithIsbns(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> periodicalsWithIsbns =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousDisplayPublicationName = null;

      // Get the publication ISBNs.
      stmt = dbManager.prepareStatement(conn, GET_PERIODICALS_WITH_ISBNS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the periodical ISBNs.
      while (resultSet.next()) {
	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	String publicationTypeName = resultSet.getString(TYPE_NAME_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "publicationTypeName = "
	    + publicationTypeName);

	String displayPublicationName =
	    publicationName + " [" + publicationTypeName.substring(0, 1) + "]";

	String isbn = resultSet.getString(ISBN_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbn = " + isbn);

	if (displayPublicationName.equals(previousDisplayPublicationName)) {
	  periodicalsWithIsbns.get(displayPublicationName).add(isbn);
	} else {
	  Collection<String> publicationIsbns = new ArrayList<String>();
	  publicationIsbns.add(isbn);
	  periodicalsWithIsbns.put(displayPublicationName, publicationIsbns);
	  previousDisplayPublicationName = displayPublicationName;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the periodical ISBNs";
      log.error(message, sqle);
      log.error("SQL = '" + GET_PERIODICALS_WITH_ISBNS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "periodicalsWithIsbns.size() = " + periodicalsWithIsbns.size());
    return periodicalsWithIsbns;
  }

  /**
   * Provides the Archival Units in the database with an unknown provider.
   * 
   * @return a Collection<String> with the sorted Archival Unit names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getUnknownProviderAuIds() throws DbException {
    final String DEBUG_HEADER = "getUnknownProviderAus(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<String> unknownProviderAuIds = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the identifiers of the Archival Unitswith an unknown provider.
      unknownProviderAuIds = getUnknownProviderAuIds(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "unknownProviderAuIds.size() = " + unknownProviderAuIds.size());
    return unknownProviderAuIds;
  }

  /**
   * Provides the Archival Units in the database with an unknown provider.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<String> with the sorted Archival Unit names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getUnknownProviderAuIds(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getUnknownProviderAuIds(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<String> unknownProviderAuIds = new ArrayList<String>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;
    String sql = GET_UNKNOWN_PROVIDER_AUS_QUERY;

    try {
      stmt = dbManager.prepareStatement(conn, sql);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the Archival Unit DOI prefixes. 
      while (resultSet.next()) {
	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	String auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

	unknownProviderAuIds.add(auId);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the Archival Units with unknown provider";
      log.error(message, sqle);
      log.error("SQL = '" + sql + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "unknownProviderAuIds.size() = " + unknownProviderAuIds.size());
    return unknownProviderAuIds;
  }

  /**
   * Provides the journal articles in the database whose parent is not a
   * journal.
   * 
   * @return a Collection<Map<String, String>> with the mismatched journal
   *         articles sorted by Archival Unit, parent name and child name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, String>> getMismatchedParentJournalArticles()
      throws DbException {
    return getMismatchedParentChildren(
	GET_MISMATCHED_PARENT_JOURNAL_ARTICLES_QUERY);
  }

  /**
   * Provides the book chapters in the database whose parent is not a book or a
   * book series.
   * 
   * @return a Collection<Map<String, String>> with the mismatched book chapters
   *         sorted by Archival Unit, parent name and child name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, String>> getMismatchedParentBookChapters()
      throws DbException {
    return getMismatchedParentChildren(
	GET_MISMATCHED_PARENT_BOOK_CHAPTERS_QUERY);
  }

  /**
   * Provides the book volumes in the database whose parent is not a book or a
   * book series.
   * 
   * @return a Collection<Map<String, String>> with the mismatched book volumes
   *         sorted by Archival Unit, parent name and child name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, String>> getMismatchedParentBookVolumes()
      throws DbException {
    return getMismatchedParentChildren(
	GET_MISMATCHED_PARENT_BOOK_VOLUMES_QUERY);
  }

  /**
   * Provides the children in the database with a mismatched parent.
   * 
   * @param A
   *          String with the database query to be used.
   * @return a Collection<Map<String, String>> with the mismatched children
   *         sorted by Archival Unit, parent name and child name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<Map<String, String>> getMismatchedParentChildren(
      String query) throws DbException {
    final String DEBUG_HEADER = "getMismatchedParentChildren(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "query = " + query);
    Collection<Map<String, String>> mismatchedChildren = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the children in the database with a mismatched parent.
      mismatchedChildren = getMismatchedParentChildren(conn, query);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "mismatchedChildren.size() = " + mismatchedChildren.size());
    return mismatchedChildren;
  }

  /**
   * Provides the children in the database with a mismatched parent.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param A
   *          String with the database query to be used.
   * @return a Collection<Map<String, String>> with the mismatched children
   *         sorted by Archival Unit, parent name and child name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<Map<String, String>> getMismatchedParentChildren(
      Connection conn, String query) throws DbException {
    final String DEBUG_HEADER = "getMismatchedParentChildren(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "query = " + query);
    Collection<Map<String, String>> mismatchedChildren =
	new ArrayList<Map<String, String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn, query);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the mismatched children. 
      while (resultSet.next()) {
	Map<String, String> mismatchedChild = new HashMap<String, String>();

	String col1 = resultSet.getString("col1");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col1 = " + col1);

	mismatchedChild.put("col1", col1);

	String col2 = resultSet.getString("col2");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col2 = " + col2);

	mismatchedChild.put("col2", col2);

	String col3 = resultSet.getString("col3");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col3 = " + col3);

	mismatchedChild.put("col3", col3);

	String col4 = resultSet.getString("col4");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col4 = " + col4);

	mismatchedChild.put("col4", col4);

	String col5 = resultSet.getString("col5");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col5 = " + col5);

	mismatchedChild.put("col5", col5);

	mismatchedChildren.add(mismatchedChild);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the children with mismatched parents";
      log.error(message, sqle);
      log.error("SQL = '" + query + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "mismatchedChildren.size() = " + mismatchedChildren.size());
    return mismatchedChildren;
  }

  /**
   * Provides the publishers linked to the Archival Unit identifier for the
   * Archival Units in the database with multiple publishers.
   * 
   * @return a Map<String, Collection<String>> with the publishers keyed by the
   *         Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getAuIdsWithMultiplePublishers()
      throws DbException {
    final String DEBUG_HEADER = "getAuIdsWithMultiplePublishers(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> ausPublishers = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the Archival Unit publishers.
      ausPublishers = getAuIdsWithMultiplePublishers(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "ausPublishers.size() = "
	+ ausPublishers.size());
    return ausPublishers;
  }

  /**
   * Provides the publishers linked to the Archival Unit identifier for the
   * Archival Units in the database with multiple publishers.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Map<String, Collection<String>> with the publishers keyed by the
   *         Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getAuIdsWithMultiplePublishers(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getAuIdsWithMultiplePublishers(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> ausPublishers =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousAuId = null;

      // Get the Archival Unit publishers.
      stmt =
	  dbManager.prepareStatement(conn, GET_AUS_MULTIPLE_PUBLISHERS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the Archival Unit publishers. 
      while (resultSet.next()) {
	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	String auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

	String publisherName = resultSet.getString(PUBLISHER_NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publisherName = " + publisherName);

	if (auId.equals(previousAuId)) {
	  ausPublishers.get(auId).add(publisherName);
	} else {
	  Collection<String> auPublishers = new ArrayList<String>();
	  auPublishers.add(publisherName);
	  ausPublishers.put(auId, auPublishers);
	  previousAuId = auId;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the Archival Units publishers";
      log.error(message, sqle);
      log.error("SQL = '" + GET_AUS_MULTIPLE_PUBLISHERS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "ausPublishers.size() = "
	+ ausPublishers.size());
    return ausPublishers;
  }

  /**
   * Provides the metadata items in the database that have no name.
   * 
   * @return a Collection<Map<String, String>> with the unnamed metadata items
   *         sorted by publisher, parent type, parent title and item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, String>> getUnnamedItems() throws DbException {
    final String DEBUG_HEADER = "getUnnamedItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, String>> unnamedItems = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the metadata items in the database that have no name.
      unnamedItems = getUnnamedItems(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "unnamedItems.size() = " + unnamedItems.size());
    return unnamedItems;
  }

  /**
   * Provides the metadata items in the database that have no name.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<Map<String, String>> with the unnamed metadata items
   *         articles sorted by publisher, parent type, parent title and item
   *         type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<Map<String, String>> getUnnamedItems(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getUnnamedItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, String>> unnamedItems =
	new ArrayList<Map<String, String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn, GET_UNNAMED_ITEMS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the unnamed items. 
      while (resultSet.next()) {
	Map<String, String> unnamedItem = new HashMap<String, String>();

	String col1 = "" + resultSet.getInt("col1");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col1 = " + col1);

	unnamedItem.put("col1", col1);

	String col2 = resultSet.getString("col2");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col2 = " + col2);

	unnamedItem.put("col2", col2);

	String col3 = resultSet.getString("col3");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col3 = " + col3);

	unnamedItem.put("col3", col3);

	String col4 = resultSet.getString("col4");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col4 = " + col4);

	unnamedItem.put("col4", col4);

	String col5 = resultSet.getString("col5");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col5 = " + col5);

	unnamedItem.put("col5", col5);

	String col6 = resultSet.getString("col6");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col6 = " + col6);

	unnamedItem.put("col6", col6);

	String col7 = resultSet.getString("col7");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col7 = " + col7);

	unnamedItem.put("col7", col7);

	unnamedItems.add(unnamedItem);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the unnamed items";
      log.error(message, sqle);
      log.error("SQL = '" + GET_UNNAMED_ITEMS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "unnamedItems.size() = " + unnamedItems.size());
    return unnamedItems;
  }

  /**
   * Provides the earliest and latest publication dates of all the metadata
   * items included in an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a KeyPair with the earliest and latest publication dates.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  KeyPair findPublicationDateInterval(Connection conn, String pluginId,
      String auKey) throws DbException {
    final String DEBUG_HEADER = "findPublicationDateInterval(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pluginId = " + pluginId);
      log.debug2(DEBUG_HEADER + "auKey = " + auKey);
    }

    KeyPair publicationInterval = null;

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn,
	  FIND_PUBLICATION_DATE_INTERVAL_QUERY);
      stmt.setString(1, auKey);
      stmt.setString(2, pluginId);

      resultSet = dbManager.executeQuery(stmt);

      // Get the single result. 
      if (resultSet.next()) {
	String earliest = resultSet.getString("earliest");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "earliest = " + earliest);

	String latest = resultSet.getString("latest");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "latest = " + latest);

	// Handle the case where the earliest value is wider than the latest.
	if (latest.startsWith(earliest)) {
	  latest = earliest;
	}

	publicationInterval = new KeyPair(earliest, latest);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find publication date interval";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_PUBLICATION_DATE_INTERVAL_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "publicationInterval = '"
	+ publicationInterval.car + "' - '" + publicationInterval.cdr + "'");

    return publicationInterval;
  }

  /**
   * Provides the proprietary identifiers for the publications in the database
   * with multiple proprietary identifiers.
   * 
   * @return a Map<String, Collection<String>> with the proprietary identifiers
   *         keyed by the publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Collection<String>> getPublicationsWithMultiplePids()
      throws DbException {
    final String DEBUG_HEADER = "getPublicationsWithMultiplePids(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> publicationsPids = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the publication proprietary identifiers.
      publicationsPids = getPublicationsWithMultiplePids(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publicationsPids.size() = " + publicationsPids.size());
    return publicationsPids;
  }

  /**
   * Provides the proprietary identifiers for the publications in the database
   * with multiple proprietary identifiers.
   * 
   * @return a Map<String, Collection<String>> with the proprietary identifiers
   *         keyed by the publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Map<String, Collection<String>> getPublicationsWithMultiplePids(
      Connection conn) throws DbException {
    final String DEBUG_HEADER = "getPublicationsWithMultiplePids(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Map<String, Collection<String>> publicationsPids =
	new TreeMap<String, Collection<String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      String previousPublicationName = null;

      // Get the publication proprietary identifiers.
      stmt = dbManager.prepareStatement(conn,
	  GET_PUBLICATIONS_MULTIPLE_PIDS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the publication proprietary identifiers. 
      while (resultSet.next()) {
	String publicationName = resultSet.getString(NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "publicationName = " + publicationName);

	String proprietaryId = resultSet.getString(PROPRIETARY_ID_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "proprietaryId = " + proprietaryId);

	if (publicationName.equals(previousPublicationName)) {
	  publicationsPids.get(publicationName).add(proprietaryId);
	} else {
	  Collection<String> publicationPids = new ArrayList<String>();
	  publicationPids.add(proprietaryId);
	  publicationsPids.put(publicationName, publicationPids);
	  previousPublicationName = publicationName;
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the publications proprietary identifiers";
      log.error(message, sqle);
      log.error("SQL = '" + GET_PUBLICATIONS_MULTIPLE_PIDS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "publicationsPids.size() = " + publicationsPids.size());
    return publicationsPids;
  }

  /**
   * Provides the non-parent metadata items in the database that have no DOI.
   * 
   * @return a Collection<Map<String, String>> with the non-parent metadata
   *         items that have no DOI sorted by publisher, parent type, parent
   *         title and item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, String>> getNoDoiItems() throws DbException {
    final String DEBUG_HEADER = "getNoDoiItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, String>> noDoiItems = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the non-parent metadata items in the database that have no DOI.
      noDoiItems = getNoDoiItems(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "noDoiItems.size() = " + noDoiItems.size());
    return noDoiItems;
  }

  /**
   * Provides the non-parent metadata items in the database that have no DOI.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<Map<String, String>> with the non-parent metadata
   *         items that have no DOI sorted by publisher, parent type, parent
   *         title and item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<Map<String, String>> getNoDoiItems(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getNoDoiItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, String>> noDoiItems =
	new ArrayList<Map<String, String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn, GET_NO_DOI_ITEMS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the non-parent items with no DOI. 
      while (resultSet.next()) {
	Map<String, String> noDoiItem = new HashMap<String, String>();

	String col1 = resultSet.getString("col1");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col1 = " + col1);

	noDoiItem.put("col1", col1);

	String col2 = resultSet.getString("col2");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col2 = " + col2);

	noDoiItem.put("col2", col2);

	String col3 = resultSet.getString("col3");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col3 = " + col3);

	noDoiItem.put("col3", col3);

	String col4 = resultSet.getString("col4");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col4 = " + col4);

	noDoiItem.put("col4", col4);

	String col5 = resultSet.getString("col5");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col5 = " + col5);

	noDoiItem.put("col5", col5);

	String col6 = resultSet.getString("col6");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col6 = " + col6);

	noDoiItem.put("col6", col6);

	String col7 = resultSet.getString("col7");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col7 = " + col7);

	noDoiItem.put("col7", col7);

	noDoiItems.add(noDoiItem);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the non-parent items with no DOI";
      log.error(message, sqle);
      log.error("SQL = '" + GET_NO_DOI_ITEMS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "noDoiItems.size() = " + noDoiItems.size());
    return noDoiItems;
  }

  /**
   * Provides the non-parent metadata items in the database that have no Access
   * URL.
   * 
   * @return a Collection<Map<String, String>> with the non-parent metadata
   *         items that have no Access URL sorted by publisher, parent type,
   *         parent title and item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, String>> getNoAccessUrlItems() throws DbException {
    final String DEBUG_HEADER = "getNoAccessUrlItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, String>> noAccessUrlItems = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the non-parent metadata items in the database that have no Access
      // URL.
      noAccessUrlItems = getNoAccessUrlItems(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "noAccessUrlItems.size() = " + noAccessUrlItems.size());
    return noAccessUrlItems;
  }

  /**
   * Provides the non-parent metadata items in the database that have no Access
   * URL.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<Map<String, String>> with the non-parent metadata
   *         items that have no Access URL sorted by publisher, parent type,
   *         parent title and item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<Map<String, String>> getNoAccessUrlItems(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getNoAccessUrlItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, String>> noAccessUrlItems =
	new ArrayList<Map<String, String>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn, GET_NO_ACCESS_URL_ITEMS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the non-parent items with no Access URL. 
      while (resultSet.next()) {
	Map<String, String> noAccessUrlItem = new HashMap<String, String>();

	String col1 = resultSet.getString("col1");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col1 = " + col1);

	noAccessUrlItem.put("col1", col1);

	String col2 = resultSet.getString("col2");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col2 = " + col2);

	noAccessUrlItem.put("col2", col2);

	String col3 = resultSet.getString("col3");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col3 = " + col3);

	noAccessUrlItem.put("col3", col3);

	String col4 = resultSet.getString("col4");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col4 = " + col4);

	noAccessUrlItem.put("col4", col4);

	String col5 = resultSet.getString("col5");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col5 = " + col5);

	noAccessUrlItem.put("col5", col5);

	String col6 = resultSet.getString("col6");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col6 = " + col6);

	noAccessUrlItem.put("col6", col6);

	String col7 = resultSet.getString("col7");
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "col7 = " + col7);

	noAccessUrlItem.put("col7", col7);

	noAccessUrlItems.add(noAccessUrlItem);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the non-parent items with no Access URL";
      log.error(message, sqle);
      log.error("SQL = '" + GET_NO_ACCESS_URL_ITEMS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "noAccessUrlItems.size() = "
	+ noAccessUrlItems.size());
    return noAccessUrlItems;
  }

  /**
   * Deletes an ISSN linked to a publication.
   * 
   * @param mdItemSeq
   *          A Long with the publication metadata identifier.
   * @param issn
   *          A String with the ISSN.
   * @param issnType
   *          A String with the ISSN type.
   * @return a boolean with <code>true</code> if the ISSN was deleted,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean deletePublicationIssn(Long mdItemSeq, String issn, String issnType)
      throws DbException {
    final String DEBUG_HEADER = "deletePublicationIssn(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    boolean deleted = false;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Delete the ISSN.
      deleted = deletePublicationIssn(conn, mdItemSeq, issn, issnType);
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "deleted = " + deleted);
    return deleted;
  }

  /**
   * Deletes an ISSN linked to a publication.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the publication metadata identifier.
   * @param issn
   *          A String with the ISSN.
   * @param issnType
   *          A String with the ISSN type.
   * @return a boolean with <code>true</code> if the ISSN was deleted,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private boolean deletePublicationIssn(Connection conn, Long mdItemSeq,
      String issn, String issnType) throws DbException {
    final String DEBUG_HEADER = "deletePublicationIssn(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "issn = " + issn);
      log.debug2(DEBUG_HEADER + "issnType = " + issnType);
    }

    int deletedCount = -1;
    PreparedStatement deleteIssn =
	dbManager.prepareStatement(conn, DELETE_ISSN_QUERY);

    try {
      deleteIssn.setLong(1, mdItemSeq);
      deleteIssn.setString(2, issn);
      deleteIssn.setString(3, issnType);
      deletedCount = dbManager.executeUpdate(deleteIssn);
    } catch (SQLException sqle) {
      String message = "Cannot delete ISSN";
      log.error(message, sqle);
      log.error("mdItemSeq = '" + mdItemSeq + "'.");
      log.error("issn = '" + issn + "'.");
      log.error("issnType = '" + issnType + "'.");
      log.error("SQL = '" + DELETE_ISSN_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(deleteIssn);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "result = " + (deletedCount > 0));
    return deletedCount > 0;
  }

  /**
   * Provides the Archival Units in the database with no metadata items.
   * 
   * @return a Collection<String> with the sorted Archival Unit identifiers.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<String> getNoItemsAuIds() throws DbException {
    final String DEBUG_HEADER = "getNoItemsAuIds(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<String> noItemsAuIds = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the identifiers of the Archival Units with no metadata items.
      noItemsAuIds = getNoItemsAuIds(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "noItemsAuIds.size() = " + noItemsAuIds.size());
    return noItemsAuIds;
  }

  /**
   * Provides the Archival Units in the database with no metadata items.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<String> with the sorted Archival Unit identifiers.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<String> getNoItemsAuIds(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getNoItemsAuIds(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<String> noItemsAuIds = new ArrayList<String>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn, GET_NO_ITEMS_AUS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the Archival Unit DOI prefixes. 
      while (resultSet.next()) {
	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	String auId = PluginManager.generateAuId(pluginId, auKey);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

	noItemsAuIds.add(auId);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the Archival Units with no metadata items";
      log.error(message, sqle);
      log.error("SQL = '" + GET_NO_ITEMS_AUS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "noItemsAuIds.size() = " + noItemsAuIds.size());
    return noItemsAuIds;
  }

  /**
   * Provides the metadata information of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a Map<String, Object> with the metadata information of the Archival
   *         Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Map<String, Object> getAuMetadata(Connection conn, String auId)
      throws DbException {
    final String DEBUG_HEADER = "getAuMetadata(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    Map<String, Object> result = null;
    String pluginKey = null;
    String auKey = null;

    PreparedStatement getAuMetadata =
	dbManager.prepareStatement(conn, GET_AU_MD_QUERY);

    ResultSet resultSet = null;

    try {
      pluginKey = PluginManager.pluginKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginKey() = " + pluginKey);

      auKey = PluginManager.auKeyFromAuId(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      getAuMetadata.setString(1, pluginKey);
      getAuMetadata.setString(2, auKey);
      resultSet = dbManager.executeQuery(getAuMetadata);

      if (resultSet.next()) {
	result = new HashMap<String, Object>();

	Long auMdSeq = resultSet.getLong(AU_MD_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auMdSeq = " + auMdSeq);

	if (!resultSet.wasNull()) {
	  result.put(AU_MD_SEQ_COLUMN, auMdSeq);
	}

	Long auSeq = resultSet.getLong(AU_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

	if (!resultSet.wasNull()) {
	  result.put(AU_SEQ_COLUMN, auSeq);
	}

	Integer mdVersion = resultSet.getInt(MD_VERSION_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "mdVersion = " + mdVersion);

	if (!resultSet.wasNull()) {
	  result.put(MD_VERSION_COLUMN, mdVersion);
	}

	Long extractTime = resultSet.getLong(EXTRACT_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "extractTime = " + extractTime);

	if (!resultSet.wasNull()) {
	  result.put(EXTRACT_TIME_COLUMN, extractTime);
	}

	Long creationTime = resultSet.getLong(CREATION_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "creationTime = " + creationTime);

	if (!resultSet.wasNull()) {
	  result.put(CREATION_TIME_COLUMN, creationTime);
	}

	Long providerSeq = resultSet.getLong(PROVIDER_SEQ_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "providerSeq = " + providerSeq);

	if (!resultSet.wasNull()) {
	  result.put(PROVIDER_SEQ_COLUMN, providerSeq);
	}
      }
    } catch (SQLException sqle) {
      String message = "Cannot get AU extraction time";
      log.error(message, sqle);
      log.error("auId = '" + auId + "'.");
      log.error("SQL = '" + GET_AU_MD_QUERY + "'.");
      log.error("pluginKey = '" + pluginKey + "'.");
      log.error("auKey = '" + auKey + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuMetadata);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the Archival Units that exist in the database.
   * 
   * @return a Collection<Map<String, Object>> with the Archival unit data.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Collection<Map<String, Object>> getDbArchivalUnits() throws DbException {
    final String DEBUG_HEADER = "getDbArchivalUnits(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, Object>> aus = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Get the data of the archival units.
      aus = getDbArchivalUnits(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "aus.size() = " + aus.size());
    return aus;
  }

  /**
   * Provides the Archival Units that exist in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @return a Collection<Map<String, Object>> with the Archival unit data.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Collection<Map<String, Object>> getDbArchivalUnits(Connection conn)
      throws DbException {
    final String DEBUG_HEADER = "getDbArchivalUnits(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, Object>> aus = new ArrayList<Map<String, Object>>();

    PreparedStatement stmt = null;
    ResultSet resultSet = null;

    try {
      stmt = dbManager.prepareStatement(conn, GET_DB_ARCHIVAL_UNITS_QUERY);
      resultSet = dbManager.executeQuery(stmt);

      // Loop through the Archival Units. 
      while (resultSet.next()) {
	Map<String, Object> auProperties = new HashMap<String, Object>();

	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

	auProperties.put(PLUGIN_ID_COLUMN, pluginId);

	Long auSeq = resultSet.getLong(AU_SEQ_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

	auProperties.put(AU_SEQ_COLUMN, auSeq);

	String auKey = resultSet.getString(AU_KEY_COLUMN);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

	auProperties.put(AU_KEY_COLUMN, auKey);

	Long creationTime = resultSet.getLong(CREATION_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "creationTime = " + creationTime);

	auProperties.put(CREATION_TIME_COLUMN, creationTime);

	Integer mdVersion = resultSet.getInt(MD_VERSION_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "mdVersion = " + mdVersion);

	auProperties.put(MD_VERSION_COLUMN, mdVersion);

	Long extractionTime = resultSet.getLong(EXTRACT_TIME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "extractionTime = " + extractionTime);

	auProperties.put(EXTRACT_TIME_COLUMN, extractionTime);

	String providerName = resultSet.getString(PROVIDER_NAME_COLUMN);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "providerName = " + providerName);

	auProperties.put(PROVIDER_NAME_COLUMN, providerName);

	Integer itemCount = resultSet.getInt("item_count");
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "itemCount = " + itemCount);

	auProperties.put("item_count", itemCount);

	aus.add(auProperties);
      }
    } catch (SQLException sqle) {
      String message = "Cannot get the Archival Units";
      log.error(message, sqle);
      log.error("SQL = '" + GET_DB_ARCHIVAL_UNITS_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(stmt);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "aus.size() = " + aus.size());
    return aus;
  }

  /**
   * Deletes an Archival Unit and its metadata.
   * 
   * @param auSeq
   *          A Long with the Archival Unit identifier.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a boolean with <code>true</code> if the Archival Unit was deleted,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  boolean removeAu(Long auSeq, String auKey) throws DbException {
    final String DEBUG_HEADER = "removeAu(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    boolean deleted = false;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = dbManager.getConnection();

      // Delete the Archival Unit.
      deleted = removeAu(conn, auSeq, auKey);
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "deleted = " + deleted);
    return deleted;
  }

  /**
   * Deletes an Archival Unit and its metadata.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the Archival Unit identifier.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a boolean with <code>true</code> if the Archival Unit was deleted,
   *         <code>false</code> otherwise.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private boolean removeAu(Connection conn, Long auSeq, String auKey)
      throws DbException {
    final String DEBUG_HEADER = "removeAu(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "auSeq = " + auSeq);
      log.debug2(DEBUG_HEADER + "auKey = " + auKey);
    }

    int deletedCount = -1;
    PreparedStatement deleteAu =
	dbManager.prepareStatement(conn, DELETE_AU_BY_PK_AND_KEY_QUERY);

    try {
      deleteAu.setLong(1, auSeq);
      deleteAu.setString(2, auKey);
      deletedCount = dbManager.executeUpdate(deleteAu);
    } catch (SQLException sqle) {
      String message = "Cannot delete Archival Unit";
      log.error(message, sqle);
      log.error("auSeq = '" + auSeq + "'.");
      log.error("auKey = '" + auKey + "'.");
      log.error("SQL = '" + DELETE_AU_BY_PK_AND_KEY_QUERY + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(deleteAu);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "result = " + (deletedCount > 0));
    return deletedCount > 0;
  }

  /**
   * Provides the identifier of a metadata key.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param keyName
   *          A String with the key name.
   * @return a Long with the identifier of the metadata key.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long findMdKey(Connection conn, String keyName) throws DbException {
    final String DEBUG_HEADER = "findMdKey(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "keyName = " + keyName);

    Long mdKeySeq = null;
    ResultSet resultSet = null;

    PreparedStatement findMdKey =
	dbManager.prepareStatement(conn, FIND_MD_KEY_QUERY);

    try {
      findMdKey.setString(1, keyName);

      resultSet = dbManager.executeQuery(findMdKey);
      if (resultSet.next()) {
	mdKeySeq = resultSet.getLong(MD_KEY_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      String message = "Cannot find metadata key";
      log.error(message, sqle);
      log.error("SQL = '" + FIND_MD_KEY_QUERY + "'.");
      log.error("keyName = '" + keyName + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findMdKey);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdKeySeq = " + mdKeySeq);
    return mdKeySeq;
  }

  /**
   * Adds a metadata key to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param keyName
   *          A String with the key name.
   * @return a Long with the identifier of the metadata key just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  Long addMdKey(Connection conn, String keyName) throws DbException {
    final String DEBUG_HEADER = "addMdKey(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "keyName = " + keyName);

    Long mdKeySeq = null;
    ResultSet resultSet = null;
    PreparedStatement insertMdKey = dbManager.prepareStatement(conn,
	INSERT_MD_KEY_QUERY, Statement.RETURN_GENERATED_KEYS);

    try {
      // Skip auto-increment key field #0
      insertMdKey.setString(1, keyName);
      dbManager.executeUpdate(insertMdKey);
      resultSet = insertMdKey.getGeneratedKeys();

      if (!resultSet.next()) {
	log.error("Unable to create metadata key table row.");
	return null;
      }

      mdKeySeq = resultSet.getLong(1);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Added mdKeySeq = " + mdKeySeq);
    } catch (SQLException sqle) {
      String message = "Cannot add metadata key";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_MD_KEY_QUERY + "'.");
      log.error("keyName = '" + keyName + "'.");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertMdKey);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdKeySeq = " + mdKeySeq);
    return mdKeySeq;
  }

  /**
   * Adds to the database a metadata item metadata generic key/value pair.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param mdKeySeq
   *          A Long with the metadata item key/value pair key identifier.
   * @param mdValue
   *          A String with the value of the metadata item key/value pair.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  void addMdPair(Connection conn, Long mdItemSeq, Long mdKeySeq, String mdValue)
      throws DbException {
    final String DEBUG_HEADER = "addMdPair(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "mdKeySeq = " + mdKeySeq);
      log.debug2(DEBUG_HEADER + "mdValue = " + mdValue);
    }

    PreparedStatement insertMdItemMdPair =
	dbManager.prepareStatement(conn, INSERT_MD_QUERY);

    try {
      insertMdItemMdPair.setLong(1, mdItemSeq);
      insertMdItemMdPair.setLong(2, mdKeySeq);
      insertMdItemMdPair.setString(3, mdValue);
      int count = dbManager.executeUpdate(insertMdItemMdPair);

      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER + "count = " + count);
	log.debug3(DEBUG_HEADER + "Added mdValue = " + mdValue);
      }
    } catch (SQLException sqle) {
      String message = "Cannot add a metadata item generic key/value pair";
      log.error(message, sqle);
      log.error("SQL = '" + INSERT_MD_QUERY + "'.");
      log.error("mdItemSeq = " + mdItemSeq + ".");
      log.error("mdKeySeq = " + mdKeySeq + ".");
      log.error("mdValue = " + mdValue + ".");
      throw new DbException(message, sqle);
    } finally {
      DbManager.safeCloseStatement(insertMdItemMdPair);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }
}
