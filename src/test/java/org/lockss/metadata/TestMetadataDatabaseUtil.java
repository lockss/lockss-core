/*

 Copyright (c) 2013-2018 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.metadata.SqlConstants.*;
import java.sql.Connection;
import java.util.List;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.exporter.biblio.BibliographicItem;
import org.lockss.test.*;
import org.lockss.util.Logger;

/**
 * Test class for org.lockss.metadata.MetadataDatabaseUtil. 
 * Inspired by TestAuMetadataRecorder
 * 
 * @author Philip Gust
 */
public class TestMetadataDatabaseUtil extends LockssTestCase {
  static Logger log = Logger.getLogger();

  private MetadataManager metadataManager;
  private MetadataDbManager dbManager;

  public void setUp() throws Exception {
    super.setUp();
    String tempDirPath = setUpDiskSpace();

    MockLockssDaemon theDaemon = getMockLockssDaemon();

    dbManager = getTestDbManager(tempDirPath);

    metadataManager = new MetadataManager();
    metadataManager.initService(theDaemon);
    metadataManager.startService();
  }

  /**
   * Runs all the tests.
   * <br />
   * This avoids unnecessary set up and tear down of the database.
   * 
   * @throws Exception
   */
  public void testAll() throws Exception {
    runRecordJournal1();
    cleanDB();
    runRecordBook1();
  }

  /**
   * Records a journal.
   * 
   * @throws Exception
   */
  private void runRecordJournal1() throws Exception {
    Connection conn = null;

    try {
      conn = dbManager.getConnection();

      int nTitles = 2;
      int nArticles=4;
      loadJournalMetadata("Publisher", nTitles, nArticles);
      List<BibliographicItem> items = 
          MetadataDatabaseUtil.getBibliographicItems(dbManager,  conn);
      assertEquals(nTitles, items.size());
      
      BibliographicItem previousItem = null;

      for (BibliographicItem item : items) {
        assertEquals("journal", item.getPublicationType());
        assertEquals("Publisher", item.getPublisherName());
        assertEquals("fulltext", item.getCoverageDepth());
        assertNotNull(item.getStartYear());
        assertNotNull(item.getEndYear());
        assertEquals(item.getStartYear(), item.getEndYear());
        assertNull(item.getPrintIsbn());
        assertNull(item.getEisbn());
        assertNull(item.getIsbn());
        assertNotNull(item.getPublicationTitle());
        assertNotNull(item.getPrintIssn());
        assertNotNull(item.getEissn());
        assertNotNull(item.getIssn());
        assertNull(item.getStartIssue());
        assertNull(item.getEndIssue());
        assertNotNull(item.getStartVolume());
        assertNotNull(item.getEndVolume());
        assertEquals(item.getStartVolume(), item.getEndVolume());
        if (item.getProprietaryIds() != null
            && item.getProprietaryIds().length > 0) {
          assertNull(item.getProprietaryIds()[0]);
        }
        if (item.getProprietarySeriesIds() != null
            && item.getProprietarySeriesIds().length > 0) {
          assertNull(item.getProprietarySeriesIds()[0]);
        }
        assertNull(item.getSeriesTitle());
        assertEquals("providerName", item.getProviderName());

        assertFalse(item.sameInNonProprietaryIdProperties(previousItem));
        previousItem = item;
      }
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }
  }

  private void loadJournalMetadata(String publishername, int publicationCount,
      int articleCount) throws DbException {
    Connection conn = null;

    try {
      conn = dbManager.getConnection();

      // Add the publisher.
      Long publisherSeq =
	  metadataManager.findOrCreatePublisher(conn, publishername);

      // Add the publishing platform.
      Long platformSeq = metadataManager.findOrCreatePlatform(conn, "platform");

      // Add the plugin.
      Long pluginSeq = metadataManager.findOrCreatePlugin(conn, "pluginId",
	  platformSeq, false);

      // Add the AU.
      Long auSeq = metadataManager.findOrCreateAu(conn, pluginSeq, "auKey");

      // Add the provider.
      Long providerSeq = metadataManager.findOrCreateProvider(conn,
	  "providerId", "providerName");

      // Add the AU metadata.
      Long auMdSeq =
	  metadataManager.addAuMd(conn, auSeq, 1, 0L, 123L, providerSeq);

      Long mdItemTypeSeq = metadataManager.findMetadataItemType(conn,
	  MD_ITEM_TYPE_JOURNAL_ARTICLE);

      for (int i = 1; i <= publicationCount; i++) {
	// Add the publication -- test direct method
	Long publicationSeq = metadataManager.findOrCreateJournal(conn,
	    publisherSeq, "1234567" + i, "4321765" + i, "Journal Title" + i,
	    null);

	Long parentSeq =
	    metadataManager.findPublicationMetadataItem(conn, publicationSeq);

	for (int j = 1; j <= articleCount; j++) {
	  Long mdItemSeq = metadataManager.addMdItem(conn, parentSeq,
	      mdItemTypeSeq, auMdSeq, "2012-12-0" + j, null, 1234L);

	  metadataManager.addMdItemName(conn, mdItemSeq,
	      "Article Title" + i + j, PRIMARY_NAME_TYPE);

	  metadataManager.addBibItem(conn, mdItemSeq, Integer.toString(i), null,
	      null, null, null);
	}
      }
    } finally {
      DbManager.commitOrRollback(conn, log);
      DbManager.safeCloseConnection(conn);
    }
  }

  private void cleanDB() throws DbException {
    Connection conn = null;

    try {
      conn = dbManager.getConnection();
      dbManager.executeUpdate(dbManager.prepareStatement(conn,
	  "delete from " + MD_ITEM_TABLE));
    } finally {
      DbManager.commitOrRollback(conn, log);
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Records a book.
   * 
   * @throws Exception
   */
  private void runRecordBook1() throws Exception {
    Connection conn = null;

    try {
      conn = dbManager.getConnection();

      // index without book titles to create unknown title entries
      int nTitles = 2;
      int nChapters = 3;
      loadBookMetadata("Publisher", nTitles, nChapters);
      List<BibliographicItem> items = 
          MetadataDatabaseUtil.getBibliographicItems(dbManager,  conn);
      assertEquals(nTitles, items.size());
      
      BibliographicItem previousItem = null;

      for (BibliographicItem item : items) {
        assertEquals("book", item.getPublicationType());
        assertEquals("Publisher", item.getPublisherName());
        assertEquals("fulltext", item.getCoverageDepth());
        assertNotNull(item.getStartYear());
        assertNotNull(item.getEndYear());
        assertEquals(item.getStartYear(), item.getEndYear());
        assertNotNull(item.getPrintIsbn());
        assertNotNull(item.getEisbn());
        assertNotNull(item.getIsbn());
        assertNotNull(item.getPublicationTitle());
        assertNull(item.getPrintIssn());
        assertNull(item.getEissn());
        assertNull(item.getIssn());
        assertNull(item.getStartIssue());
        assertNull(item.getEndIssue());
        assertNull(item.getStartVolume());
        assertNull(item.getEndVolume());
        if (item.getProprietaryIds() != null
            && item.getProprietaryIds().length > 0) {
          assertNull(item.getProprietaryIds()[0]);
        }
        if (item.getProprietarySeriesIds() != null
            && item.getProprietarySeriesIds().length > 0) {
          assertNull(item.getProprietarySeriesIds()[0]);
        }
        assertNull(item.getSeriesTitle());
        assertEquals("providerName", item.getProviderName());

        assertFalse(item.sameInNonProprietaryIdProperties(previousItem));
        previousItem = item;
      }
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }
  }

  private void loadBookMetadata(String publishername, int publicationCount,
      int articleCount) throws DbException {
    Connection conn = null;

    try {
      conn = dbManager.getConnection();

      // Add the publisher.
      Long publisherSeq =
	  metadataManager.findOrCreatePublisher(conn, publishername);

      // Add the publishing platform.
      Long platformSeq = metadataManager.findOrCreatePlatform(conn, "platform");

      // Add the plugin.
      Long pluginSeq = metadataManager.findOrCreatePlugin(conn, "pluginId",
	  platformSeq, false);

      // Add the AU.
      Long auSeq = metadataManager.findOrCreateAu(conn, pluginSeq, "auKey");

      // Add the provider.
      Long providerSeq = metadataManager.findOrCreateProvider(conn,
	  "providerId", "providerName");

      // Add the AU metadata.
      Long auMdSeq =
	  metadataManager.addAuMd(conn, auSeq, 1, 0L, 123L, providerSeq);

      for (int i = 1; i <= publicationCount; i++) {
	// Add the publication -- test direct method
	Long publicationSeq = metadataManager.findOrCreateBook(conn,
	    publisherSeq, null, "978012345678" + i, "978987654321" + i,
	    "Book Title" + i, null);

	Long parentSeq =
	    metadataManager.findPublicationMetadataItem(conn, publicationSeq);

	for (int j = 1; j <= articleCount; j++) {
	  metadataManager.addMdItemDoi(conn, parentSeq, "10.1000/182");

	  Long mdItemTypeSeq = metadataManager.findMetadataItemType(conn,
	      MD_ITEM_TYPE_BOOK_CHAPTER);

	  metadataManager.addMdItem(conn, parentSeq, mdItemTypeSeq, auMdSeq,
	      "2012-12-0" + j, null, 1234L);
	}
      }
    } finally {
      DbManager.commitOrRollback(conn, log);
      DbManager.safeCloseConnection(conn);
    }
  }
}
