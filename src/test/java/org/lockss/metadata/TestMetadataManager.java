/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

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
package org.lockss.metadata;

import static org.lockss.metadata.SqlConstants.*;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import org.lockss.config.*;
import org.lockss.daemon.PluginException;
import org.lockss.extractor.ArticleMetadata;
import org.lockss.extractor.ArticleMetadataExtractor;
import org.lockss.extractor.MetadataField;
import org.lockss.extractor.MetadataTarget;
import org.lockss.plugin.*;
import org.lockss.plugin.simulated.*;
import org.lockss.util.*;
import org.lockss.test.*;

/**
 * Test class for org.lockss.metadata.MetadataManager
 *
 * @author  Philip Gust
 * @version 1.0
 */
public class TestMetadataManager extends LockssTestCase {
  static Logger log = Logger.getLogger(TestMetadataManager.class);

  private SimulatedArchivalUnit sau0, sau1, sau2, sau3, sau4;
  private MockLockssDaemon theDaemon;
  private MetadataManager metadataManager;
  private MetadataManagerSql metadataManagerSql;
  private PluginManager pluginManager;
  private MetadataDbManager dbManager;
  
  /** number of articles deleted by the MetadataManager */
  Integer[] articlesDeleted = new Integer[] {0};
  
  public void setUp() throws Exception {
    super.setUp();
    String tempDirPath = setUpDiskSpace();
    useOldRepo();

    theDaemon = getMockLockssDaemon();
    theDaemon.getAlertManager();
    pluginManager = theDaemon.getPluginManager();
    pluginManager.setLoadablePluginsReady(true);
    theDaemon.setDaemonInited(true);
    theDaemon.getCrawlManager();

    sau0 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin0.class,
                                              simAuConfig(tempDirPath + "/0"));
    sau1 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin1.class,
                                              simAuConfig(tempDirPath + "/1"));
    sau2 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin2.class,
                                              simAuConfig(tempDirPath + "/2"));
    sau3 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin3.class,
                                              simAuConfig(tempDirPath + "/3"));
    sau4 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin0.class,
                                              simAuConfig(tempDirPath + "/4"));
    PluginTestUtil.crawlSimAu(sau0);
    PluginTestUtil.crawlSimAu(sau1);
    PluginTestUtil.crawlSimAu(sau2);
    PluginTestUtil.crawlSimAu(sau3);
    PluginTestUtil.crawlSimAu(sau4);

    dbManager = getTestDbManager(tempDirPath);

    theDaemon.setMetadataManager(metadataManager);
    metadataManager.initService(theDaemon);
    metadataManager.startService();

    metadataManagerSql = metadataManager.getMetadataManagerSql();

    theDaemon.setAusStarted(true);
    
    int expectedAuCount = 5;
    assertEquals(expectedAuCount, pluginManager.getAllAus().size());

  }

  Configuration simAuConfig(String rootPath) {
    Configuration conf = ConfigManager.newConfiguration();
    conf.put("root", rootPath);
    conf.put("depth", "2");
    conf.put("branch", "1");
    conf.put("numFiles", "3");
    conf.put("fileTypes", "" + (SimulatedContentGenerator.FILE_TYPE_PDF +
                                SimulatedContentGenerator.FILE_TYPE_HTML));
    conf.put("binFileSize", "7");
    return conf;
  }


  public void tearDown() throws Exception {
    sau0.deleteContentTree();
    sau1.deleteContentTree();
    sau2.deleteContentTree();
    theDaemon.stopDaemon();
    super.tearDown();
  }

  public void testAll() throws Exception {
    runTestFindPublication();
    runMetadataMonitorTest();
    runPublicationIntervalTest();
    runMetadataControlTest();
  }

  private void runTestFindPublication() throws Exception {
    Connection conn = dbManager.getConnection();

    List<Long> journals = new ArrayList<Long>();
    List<Long> books = new ArrayList<Long>();
    Map<Long, Long> publishers = new HashMap<Long, Long>();
    Map<Long, Long> mdItems = new HashMap<Long, Long>();
    Map<Long, String> names = new HashMap<Long, String>();

    String query = "select p." + PUBLICATION_SEQ_COLUMN
	+ ", p." + PUBLISHER_SEQ_COLUMN
	+ ", p." + MD_ITEM_SEQ_COLUMN
	+ ", mt." + TYPE_NAME_COLUMN
	+ ", n." + NAME_COLUMN
	+ " from " + MD_ITEM_TYPE_TABLE + " mt"
	+ ", " + MD_ITEM_NAME_TABLE + " n"
	+ ", " + MD_ITEM_TABLE + " m"
	+ ", " + PUBLICATION_TABLE + " p"
	+ " where mt." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " = m." + MD_ITEM_TYPE_SEQ_COLUMN
	+ " and m." + MD_ITEM_SEQ_COLUMN + " = p." + MD_ITEM_SEQ_COLUMN
	+ " and n." + MD_ITEM_SEQ_COLUMN + " = p." + MD_ITEM_SEQ_COLUMN;

    PreparedStatement stmt = dbManager.prepareStatement(conn, query);
    ResultSet resultSet = dbManager.executeQuery(stmt);

    while (resultSet.next()) {
      Long publicationSeq = resultSet.getLong(PUBLICATION_SEQ_COLUMN);
      String typeName = resultSet.getString(TYPE_NAME_COLUMN);

      if (MD_ITEM_TYPE_JOURNAL.equals(typeName)) {
	journals.add(publicationSeq);
      } else if (MD_ITEM_TYPE_BOOK.equals(typeName)) {
	books.add(publicationSeq);
      }

      publishers.put(publicationSeq, resultSet.getLong(PUBLISHER_SEQ_COLUMN));
      mdItems.put(publicationSeq, resultSet.getLong(MD_ITEM_SEQ_COLUMN));
      names.put(publicationSeq, resultSet.getString(NAME_COLUMN));
    }

    Map<Long, String> pIssns = new HashMap<Long, String>();
    Map<Long, String> eIssns = new HashMap<Long, String>();

    query = "select p." + PUBLICATION_SEQ_COLUMN
	+ ", i." + ISSN_COLUMN
	+ ", i." + ISSN_TYPE_COLUMN
	+ " from " + ISSN_TABLE + " i"
	+ ", " + PUBLICATION_TABLE + " p"
	+ " where i." + MD_ITEM_SEQ_COLUMN + " = p." + MD_ITEM_SEQ_COLUMN;

    stmt = dbManager.prepareStatement(conn, query);
    resultSet = dbManager.executeQuery(stmt);

    while (resultSet.next()) {
      if (P_ISSN_TYPE.equals(resultSet.getString(ISSN_TYPE_COLUMN))) {
	pIssns.put(resultSet.getLong(PUBLICATION_SEQ_COLUMN),
	    resultSet.getString(ISSN_COLUMN));
      } else if (E_ISSN_TYPE.equals(resultSet.getString(ISSN_TYPE_COLUMN))) {
	eIssns.put(resultSet.getLong(PUBLICATION_SEQ_COLUMN),
	    resultSet.getString(ISSN_COLUMN));
      }
    }

    Map<Long, String> pIsbns = new HashMap<Long, String>();
    Map<Long, String> eIsbns = new HashMap<Long, String>();

    query = "select p." + PUBLICATION_SEQ_COLUMN
	+ ", i." + ISBN_COLUMN
	+ ", i." + ISBN_TYPE_COLUMN
	+ " from " + ISBN_TABLE + " i"
	+ ", " + PUBLICATION_TABLE + " p"
	+ " where i." + MD_ITEM_SEQ_COLUMN + " = p." + MD_ITEM_SEQ_COLUMN;

    stmt = dbManager.prepareStatement(conn, query);
    resultSet = dbManager.executeQuery(stmt);

    while (resultSet.next()) {
      if (P_ISBN_TYPE.equals(resultSet.getString(ISBN_TYPE_COLUMN))) {
	pIsbns.put(resultSet.getLong(PUBLICATION_SEQ_COLUMN),
	    resultSet.getString(ISBN_COLUMN));
      } else if (E_ISBN_TYPE.equals(resultSet.getString(ISBN_TYPE_COLUMN))) {
	eIsbns.put(resultSet.getLong(PUBLICATION_SEQ_COLUMN),
	    resultSet.getString(ISBN_COLUMN));
      }
    }

    runTestFindJournal(conn, journals, publishers, names, pIssns, eIssns,
	pIsbns, eIsbns);

    runTestFindBook(conn, books, publishers, names, pIssns, eIssns, pIsbns,
	eIsbns);

    runTestFindBookSeries(conn, journals, mdItems, publishers, names, pIssns,
	eIssns,	pIsbns, eIsbns);

    MetadataDbManager.safeRollbackAndClose(conn);
  }

  private void runTestFindJournal(Connection conn, List<Long> journals,
      Map<Long, Long> publishers, Map<Long, String> names,
      Map<Long, String> pIssns, Map<Long, String> eIssns,
      Map<Long, String> pIsbns, Map<Long, String> eIsbns) throws Exception {

    for (Long publicationSeq : journals) {
      // Exact match.
      Long matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISSNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), null,
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISSNs and and alternate name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), "Alternate Name",
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with reversed print and electronic ISSNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  eIssns.get(publicationSeq), pIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no print ISSN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  null, pIssns.get(publicationSeq), pIsbns.get(publicationSeq),
	  eIsbns.get(publicationSeq), MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no electronic ISSN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  eIssns.get(publicationSeq), null, pIsbns.get(publicationSeq),
	  eIsbns.get(publicationSeq), MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match by name and no ISSNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  null, null, pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      assertEquals(publicationSeq, matchedPublicationSeq);

      boolean existingHasIssns = pIssns.get(publicationSeq) != null
	  || eIssns.get(publicationSeq) != null;

      // No match for new print ISSN when the existing one has an ISSN even if
      // the name matches, unless the electronic ISSN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  "12345678", eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      if (existingHasIssns && eIssns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISSN when the existing one has an ISSN even
      // if the name matches, unless the print ISSN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), "98765432",
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      if (existingHasIssns && pIssns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new print ISSN when the existing one has an ISSN even if
      // the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  "12345678", null, pIsbns.get(publicationSeq),
	  eIsbns.get(publicationSeq), MD_ITEM_TYPE_JOURNAL);

      if (existingHasIssns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISBN when the existing one has an ISBN even
      // if the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null,
	  "98765432", pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      if (existingHasIssns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for different publication type.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK);

      assertNull(matchedPublicationSeq);
    }
  }

  private void runTestFindBook(Connection conn, List<Long> books,
      Map<Long, Long> publishers, Map<Long, String> names,
      Map<Long, String> pIssns, Map<Long, String> eIssns,
      Map<Long, String> pIsbns, Map<Long, String> eIsbns) throws Exception {

    for (Long publicationSeq : books) {
      // Exact match.
      Long matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISBNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), null,
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISBNs and and alternate name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), "Alternate Name",
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with reversed print and electronic ISBNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  eIsbns.get(publicationSeq), pIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no print ISBN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  null, pIsbns.get(publicationSeq), MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no electronic ISBN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  eIsbns.get(publicationSeq), null, MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match by name and no ISBNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  null, null, MD_ITEM_TYPE_BOOK);

      assertEquals(publicationSeq, matchedPublicationSeq);

      boolean existingHasIsbns = pIsbns.get(publicationSeq) != null
	  || eIsbns.get(publicationSeq) != null;

      // No match for new print ISBN when the existing one has an ISBN even if
      // the name matches, unless the electronic ISBN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  "9876543210987", eIsbns.get(publicationSeq), MD_ITEM_TYPE_BOOK);

      if (existingHasIsbns && eIsbns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISBN when the existing one has an ISBN even
      // if the name matches, unless the print ISBN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), "9876543210987", MD_ITEM_TYPE_BOOK);

      if (existingHasIsbns && pIsbns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new print ISBN when the existing one has an ISBN even if
      // the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  "9876543210987", null, MD_ITEM_TYPE_BOOK);

      if (existingHasIsbns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISBN when the existing one has an ISBN even
      // if the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  null, "9876543210987", MD_ITEM_TYPE_BOOK);

      if (existingHasIsbns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for different publication type.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_JOURNAL);

      assertNull(matchedPublicationSeq);
    }
  }

  private void runTestFindBookSeries(Connection conn, List<Long> journals,
      Map<Long, Long> mdItems, Map<Long, Long> publishers,
      Map<Long, String> names, Map<Long, String> pIssns,
      Map<Long, String> eIssns, Map<Long, String> pIsbns,
      Map<Long, String> eIsbns) throws Exception {

    for (Long publicationSeq : journals) {
      metadataManagerSql.addMdItemIsbns(conn, mdItems.get(publicationSeq),
	  "9781585623174", "9781585623177");
      pIsbns.put(publicationSeq, "9781585623174");
      eIsbns.put(publicationSeq, "9781585623177");

      String query = "update " + MD_ITEM_TABLE
  	+ " set " + MD_ITEM_TYPE_SEQ_COLUMN + " = 1"
  	+ " where " + MD_ITEM_SEQ_COLUMN + " = " + mdItems.get(publicationSeq);

      PreparedStatement stmt = dbManager.prepareStatement(conn, query);
      dbManager.executeUpdate(stmt);
    }

    for (Long publicationSeq : journals) {
      // Exact match.
      Long matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISSNs and ISBNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), null,
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISSNs and no ISBNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), null,
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq), null,null,
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISBNs and no ISSNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), null, null,null,
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISSNs and ISBNs and and alternate name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), "Alternate Name",
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISSNs and no ISBNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), "Alternate Name",
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq), null,null,
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with same ISBNs and no ISSNs and no name.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), "Alternate Name", null,null,
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with reversed print and electronic ISSNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  eIssns.get(publicationSeq), pIssns.get(publicationSeq), null, null,
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no print ISSN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null,
	  eIssns.get(publicationSeq), null, null, MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no electronic ISSN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  eIssns.get(publicationSeq), null, null, null,
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with reversed print and electronic ISBNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  eIsbns.get(publicationSeq), pIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no print ISBN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  null, eIsbns.get(publicationSeq), MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match with no electronic ISSN.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  pIsbns.get(publicationSeq), null, MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      // Match by name and no ISBNs and no ISSNs.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  null, null, MD_ITEM_TYPE_BOOK_SERIES);

      assertEquals(publicationSeq, matchedPublicationSeq);

      boolean existingHasIssns = pIssns.get(publicationSeq) != null
	  || eIssns.get(publicationSeq) != null;

      // No match for new print ISSN when the existing one has an ISSN even if
      // the name matches, unless the electronic ISSN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  "12345678", eIssns.get(publicationSeq), null, null,
	  MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIssns && eIssns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISSN when the existing one has an ISSN even
      // if the name matches, unless the print ISSN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), "98765432", null, null,
	  MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIssns && pIssns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new print ISSN when the existing one has an ISSN even if
      // the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  "12345678", null, null, null, MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIssns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISSN when the existing one has an ISSN even
      // if the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null,
	  "98765432", null, null, MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIssns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      boolean existingHasIsbns = pIsbns.get(publicationSeq) != null
	  || eIsbns.get(publicationSeq) != null;

      // No match for new print ISBN when the existing one has an ISBN even if
      // the name matches, unless the electronic ISBN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  "9876543210987", eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIsbns && eIsbns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISBN when the existing one has an ISBN even
      // if the name matches, unless the print ISBN matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  pIsbns.get(publicationSeq), "1234567890123",
	  MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIsbns && pIsbns.get(publicationSeq) == null) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new print ISBN when the existing one has an ISBN even if
      // the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  "9876543210987", null, MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIsbns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for new electronic ISBN when the existing one has an ISBN even
      // if the name matches.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq), null, null,
	  null, "1234567890123", MD_ITEM_TYPE_BOOK_SERIES);

      if (existingHasIsbns) {
	assertNull(matchedPublicationSeq);
      } else {
	assertEquals(publicationSeq, matchedPublicationSeq);
      }

      // No match for different publication type.
      matchedPublicationSeq = metadataManager.findPublication(conn,
	  publishers.get(publicationSeq), names.get(publicationSeq),
	  pIssns.get(publicationSeq), eIssns.get(publicationSeq),
	  pIsbns.get(publicationSeq), eIsbns.get(publicationSeq),
	  MD_ITEM_TYPE_BOOK);

      assertNull(matchedPublicationSeq);
    }
  }

  private void runMetadataMonitorTest() throws Exception {
    assertEquals(0,
	metadataManager.getPublishersWithMultipleDoiPrefixes().size());
    assertEquals(0,
	metadataManager.getDoiPrefixesWithMultiplePublishers().size());
    assertEquals(0, metadataManager.getAuNamesWithMultipleDoiPrefixes().size());
    assertEquals(0, metadataManager.getPublicationsWithMoreThan2Isbns().size());
    assertEquals(0, metadataManager.getPublicationsWithMoreThan2Issns().size());
    assertEquals(0, metadataManager.getIsbnsWithMultiplePublications().size());
    assertEquals(0, metadataManager.getIssnsWithMultiplePublications().size());
    assertEquals(0, metadataManager.getBooksWithIssns().size());
    assertEquals(0, metadataManager.getPeriodicalsWithIsbns().size());
    assertEquals(0, metadataManager.getUnknownProviderAuIds().size());
    assertEquals(0, metadataManager.getPublicationsWithMultiplePids().size());
  }

  private void runPublicationIntervalTest() throws Exception {
    Connection conn = dbManager.getConnection();
    
    // Get the existing AU key and plugin pairs.
    String query = "select p." + PLUGIN_ID_COLUMN
	+ ", " + AU_TABLE + "." + AU_KEY_COLUMN
	+ " from " + AU_TABLE
	+ ", " + PLUGIN_TABLE + " p"
        + " where " + AU_TABLE + "." + PLUGIN_SEQ_COLUMN
        + " = p." + PLUGIN_SEQ_COLUMN
        + " order by p." + PLUGIN_ID_COLUMN
	+ ", " + AU_TABLE + "." + AU_KEY_COLUMN;

    PreparedStatement stmt = dbManager.prepareStatement(conn, query);
    ResultSet resultSet = dbManager.executeQuery(stmt);

    while (resultSet.next()) {
      String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
      String auKey = resultSet.getString(AU_KEY_COLUMN);

      KeyPair interval =
	  metadataManagerSql.findPublicationDateInterval(conn, pluginId, auKey);
      String earliest = (String)interval.car;
      String latest = (String)interval.cdr;

      if (pluginId.endsWith("0")) {
	assertEquals("2010-Q1", earliest);
	assertEquals("2010-Q2", latest);
      } else if (pluginId.endsWith("1")) {
	assertEquals("2010-S2", earliest);
	assertEquals("2010-S3", latest);
      } else if (pluginId.endsWith("2")) {
	assertEquals("1993", earliest);
	assertEquals("1993", latest);
      } else if (pluginId.endsWith("3")) {
	assertEquals("1999", earliest);
	assertEquals("1999", latest);
      } else {
	fail("Unexpected pluginId '" + pluginId + "'");
      }
    }

    MetadataDbManager.safeRollbackAndClose(conn);
  }

  private void runMetadataControlTest() throws Exception {
    assertFalse(metadataManager.deletePublicationIssn(123456L, "Nonexistent",
	"e_issn"));
  }

  public static class MySubTreeArticleIteratorFactory
      implements ArticleIteratorFactory {
    String pat;
    public MySubTreeArticleIteratorFactory(String pat) {
      this.pat = pat;
    }
    
    /**
     * Create an Iterator that iterates through the AU's articles, pointing
     * to the appropriate CachedUrl of type mimeType for each, or to the
     * plugin's choice of CachedUrl if mimeType is null
     * @param au the ArchivalUnit to iterate through
     * @return the ArticleIterator
     */
    @Override
    public Iterator<ArticleFiles> createArticleIterator(
        ArchivalUnit au, MetadataTarget target) throws PluginException {
      Iterator<ArticleFiles> ret;
      SubTreeArticleIterator.Spec spec = 
        new SubTreeArticleIterator.Spec().setTarget(target);
      
      if (pat != null) {
       spec.setPattern(pat);
      }
      
      ret = new SubTreeArticleIterator(au, spec);
      log.debug(  "creating article iterator for au " + au.getName() 
                    + " hasNext: " + ret.hasNext());
      return ret;
    }
  }

  public static class MySimulatedPlugin extends SimulatedPlugin {
    ArticleMetadataExtractor simulatedArticleMetadataExtractor = null;
    int version = 2;
    /**
     * Returns the article iterator factory for the mime type, if any
     * @param contentType the content type
     * @return the ArticleIteratorFactory
     */
    @Override
    public ArticleIteratorFactory getArticleIteratorFactory() {
      MySubTreeArticleIteratorFactory ret =
          new MySubTreeArticleIteratorFactory(null); //"branch1/branch1");
      return ret;
    }
    @Override
    public ArticleMetadataExtractor 
      getArticleMetadataExtractor(MetadataTarget target, ArchivalUnit au) {
      return simulatedArticleMetadataExtractor;
    }

    @Override
    public String getFeatureVersion(Plugin.Feature feat) {
      if (Feature.Metadata == feat) {
	// Increment the version on every call to delete old metadata before
	// storing new metadata.
	return feat + "_" + version++;
      } else {
	return null;
      }
    }
  }

  public static class MySimulatedPlugin0 extends MySimulatedPlugin {
    public MySimulatedPlugin0() {
      simulatedArticleMetadataExtractor = new ArticleMetadataExtractor() {
        int articleNumber = 0;
        public void extract(MetadataTarget target, ArticleFiles af,
            Emitter emitter) throws IOException, PluginException {
          ArticleMetadata md = new ArticleMetadata();
          articleNumber++;

          // use provider based on au number from last digit of auid: 0 or 4
          String auid = af.getFullTextCu().getArchivalUnit().getAuId();
          String auNumber = auid.substring(auid.length()-1);
          md.put(MetadataField.FIELD_PROVIDER, "Provider "+auNumber);

          md.put(MetadataField.FIELD_PUBLISHER,"Publisher 0");
          md.put(MetadataField.FIELD_ISSN,"0740-2783");
          md.put(MetadataField.FIELD_VOLUME,"XI");
          if (articleNumber % 2 == 0) {
            md.put(MetadataField.FIELD_ISSUE,"1st Quarter");
            md.put(MetadataField.FIELD_DATE,"2010-Q1");
            md.put(MetadataField.FIELD_START_PAGE,"" + articleNumber);
          } else {
                    md.put(MetadataField.FIELD_ISSUE,"2nd Quarter");
            md.put(MetadataField.FIELD_DATE,"2010-Q2");
            md.put(MetadataField.FIELD_START_PAGE,"" + (articleNumber-9));
          }
          String doiPrefix = "10.1234/12345678";
          String doi = doiPrefix + "."
                        + md.get(MetadataField.FIELD_DATE) + "."
                        + md.get(MetadataField.FIELD_START_PAGE); 
          md.put(MetadataField.FIELD_DOI, doi);
          md.put(MetadataField.FIELD_PUBLICATION_TITLE,
                 "Journal[" + doiPrefix + "]");
          md.put(MetadataField.FIELD_ARTICLE_TITLE,"Title[" + doi + "]");
          md.put(MetadataField.FIELD_AUTHOR,"Author[" + doi + "]");
          md.put(MetadataField.FIELD_ACCESS_URL, 
                 "http://www.title0.org/plugin0/XI/"
             +  md.get(MetadataField.FIELD_DATE) 
             +"/p" + md.get(MetadataField.FIELD_START_PAGE));
          emitter.emitMetadata(af, md);
        }
      };
    }
    public ExternalizableMap getDefinitionMap() {
      ExternalizableMap map = new ExternalizableMap();
      map.putString("au_start_url", "\"%splugin0/%s\", base_url, volume");
      return map;
    }
  }
          
  public static class MySimulatedPlugin1 extends MySimulatedPlugin {
    public MySimulatedPlugin1() {
      simulatedArticleMetadataExtractor = new ArticleMetadataExtractor() {
        int articleNumber = 0;
        public void extract(MetadataTarget target, ArticleFiles af,
            Emitter emitter) throws IOException, PluginException {
          articleNumber++;
          ArticleMetadata md = new ArticleMetadata();
          md.put(MetadataField.FIELD_PUBLISHER,"Publisher One");
          md.put(MetadataField.FIELD_ISSN,"1144-875X");
          md.put(MetadataField.FIELD_EISSN, "7744-6521");
          md.put(MetadataField.FIELD_VOLUME,"42");
          if (articleNumber < 10) {
            md.put(MetadataField.FIELD_ISSUE,"Summer");
            md.put(MetadataField.FIELD_DATE,"2010-S2");
            md.put(MetadataField.FIELD_START_PAGE,"" + articleNumber);
          } else {
            md.put(MetadataField.FIELD_ISSUE,"Fall");
            md.put(MetadataField.FIELD_DATE,"2010-S3");
            md.put(MetadataField.FIELD_START_PAGE, "" + (articleNumber-9));
          }
          String doiPrefix = "10.2468/28681357";
          String doi = doiPrefix + "."
                        + md.get(MetadataField.FIELD_DATE) + "."
                        + md.get(MetadataField.FIELD_START_PAGE); 
          md.put(MetadataField.FIELD_DOI, doi);
          md.put(MetadataField.FIELD_PUBLICATION_TITLE,
                 "Journal[" + doiPrefix + "]");
          md.put(MetadataField.FIELD_ARTICLE_TITLE, "Title[" + doi + "]");
          md.put(MetadataField.FIELD_AUTHOR, "Author1[" + doi + "]");
          md.put(MetadataField.FIELD_ACCESS_URL, 
              "http://www.title1.org/plugin1/v_42/"
                +  md.get(MetadataField.FIELD_DATE) 
                +"/p" + md.get(MetadataField.FIELD_START_PAGE));
          emitter.emitMetadata(af, md);
        }
      };
    }
    public ExternalizableMap getDefinitionMap() {
      ExternalizableMap map = new ExternalizableMap();
      map.putString("au_start_url", "\"%splugin1/v_42\", base_url");
      return map;
    }
  }
  
  public static class MySimulatedPlugin2 extends MySimulatedPlugin {
    public MySimulatedPlugin2() {
      simulatedArticleMetadataExtractor = new ArticleMetadataExtractor() {
        int articleNumber = 0;
        public void extract(MetadataTarget target, ArticleFiles af,
            Emitter emitter) throws IOException, PluginException {
          org.lockss.extractor.ArticleMetadata md = new ArticleMetadata();
          articleNumber++;
          md.put(MetadataField.FIELD_PUBLISHER,"Publisher Dos");
          String doi = "10.1357/9781585623174." + articleNumber; 
          md.put(MetadataField.FIELD_DOI,doi);
          md.put(MetadataField.FIELD_ISBN,"978-1-58562-317-4");
          md.put(MetadataField.FIELD_DATE,"1993");
          md.put(MetadataField.FIELD_START_PAGE,"" + articleNumber);
          md.put(MetadataField.FIELD_PUBLICATION_TITLE,
              "Manual of Clinical Psychopharmacology");
          md.put(MetadataField.FIELD_ARTICLE_TITLE,"Title[" + doi + "]");
          md.put(MetadataField.FIELD_AUTHOR,"Author1[" + doi + "]");
          md.put(MetadataField.FIELD_AUTHOR,"Author2[" + doi + "]");
          md.put(MetadataField.FIELD_AUTHOR,"Author3[" + doi + "]");
          md.put(MetadataField.FIELD_ACCESS_URL, 
             "http://www.title2.org/plugin2/1993/p"+articleNumber);
          emitter.emitMetadata(af, md);
        }
      };
    }
    public ExternalizableMap getDefinitionMap() {
      ExternalizableMap map = new ExternalizableMap();
      map.putString("au_start_url", "\"%splugin2/1993\", base_url");
      return map;
    }
  }
  
  public static class MySimulatedPlugin3 extends MySimulatedPlugin {
    public MySimulatedPlugin3() {
      simulatedArticleMetadataExtractor = new ArticleMetadataExtractor() {
        int articleNumber = 0;
        public void extract(MetadataTarget target, ArticleFiles af,
            Emitter emitter) throws IOException, PluginException {
          ArticleMetadata md = new ArticleMetadata();
          articleNumber++;
          md.put(MetadataField.FIELD_PUBLISHER,"Publisher Trois");
          String doiPrefix = "10.0135/12345678.1999-11.12";
          String doi = doiPrefix + "." + articleNumber; 
          md.put(MetadataField.FIELD_DOI,doi);
          md.put(MetadataField.FIELD_ISBN,"976-1-58562-317-7");
          md.put(MetadataField.FIELD_DATE,"1999");
          md.put(MetadataField.FIELD_START_PAGE,"" + articleNumber);
          md.put(MetadataField.FIELD_PUBLICATION_TITLE,
                 "Journal[" + doiPrefix + "]");
          md.put(MetadataField.FIELD_ARTICLE_TITLE,"Title[" + doi + "]");
          md.put(MetadataField.FIELD_AUTHOR,"Author1[" + doi + "]");
          md.put(MetadataField.FIELD_ACCESS_URL, 
                  "http://www.title3.org/plugin3/1999/p"+articleNumber);
          emitter.emitMetadata(af, md);
        }
      };
    }
    public ExternalizableMap getDefinitionMap() {
      ExternalizableMap map = new ExternalizableMap();
      map.putString("au_start_url", "\"%splugin3/1999\", base_url");
      return map;
    }
  }
}
