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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.lockss.app.BaseLockssManager;
import org.lockss.db.DbException;
import org.lockss.extractor.MetadataField;
import org.lockss.plugin.ArchivalUnit;
import org.lockss.plugin.PluginManager;
import org.lockss.util.KeyPair;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;

/**
 * This class implements a manager that is responsible for managing Archival
 * Unit metadata.
 * 
 * @author Philip Gust
 * @version 1.0
 */
public class MetadataManager extends BaseLockssManager {

  private static Logger log = Logger.getLogger();

  /** Name of metadata status table */
  public static final String METADATA_STATUS_TABLE_NAME =
      "MetadataStatusTable";

  public static final String ACCESS_URL_FEATURE = "Access";

  // The number of publications currently in the metadata database
  // (-1 indicates needs recalculation)
  private long metadataPublicationCount = -1;

  // The plugin manager.
  private PluginManager pluginMgr = null;

  // The database manager.
  private MetadataDbManager dbManager = null;

  // The SQL code executor.
  private MetadataManagerSql mdManagerSql;

  /**
   * No-argument constructor.
   */
  public MetadataManager() {
  }

  /**
   * Constructor used for generating a testing database.
   *
   * @param dbManager
   *          A MetadataDbManager with the database manager to be used.
   */
  public MetadataManager(MetadataDbManager dbManager) {
    this.dbManager = dbManager;
    mdManagerSql = new MetadataManagerSql(dbManager);
  }

  /**
   * Starts the MetadataManager service.
   */
  @Override
  public void startService() {
    final String DEBUG_HEADER = "startService(): ";
    log.debug(DEBUG_HEADER + "Starting MetadataManager");

    pluginMgr = getManagerByType(PluginManager.class);
    dbManager = getManagerByType(MetadataDbManager.class);
    mdManagerSql = new MetadataManagerSql(dbManager/*, this*/);

    // Initialize the counts of articles and pending AUs from database.
    try {
      metadataPublicationCount = mdManagerSql.getPublicationCount();
    } catch (DbException dbe) {
      log.error("Cannot get publication count", dbe);
    }

    log.debug(DEBUG_HEADER + "MetadataManager service successfully started");
  }

  /**
   * Provides the number of distinct publications in the metadata database.
   * 
   * @return the number of distinct publications in the metadata database
   */
  public long getPublicationCount() {
    if (metadataPublicationCount < 0) {
      try {
        metadataPublicationCount = mdManagerSql.getPublicationCount();
      } catch (DbException ex) {
        log.error("getPublicationCount", ex);
      }
    }
    return (metadataPublicationCount < 0) ? 0 : metadataPublicationCount;
  }

  /**
   * Provides the number of distinct publications in the metadata database.
   * 
   * @return the number of distinct publications in the metadata database
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public long getPublicationCount(Connection conn) throws DbException {
    return mdManagerSql.getPublicationCount(conn);
  }

  /**
   * Resets the count of distinct publications in the metadata database, forcing
   * a recalculation the next time the count is requested.
   */
  public void resetPublicationCount() {
    metadataPublicationCount = -1;
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
  public Long findOrCreatePlugin(Connection conn, String pluginId,
      Long platformSeq, boolean isBulkContent) throws DbException {
    return mdManagerSql.findOrCreatePlugin(conn, pluginId, platformSeq,
	isBulkContent);
  }
  
  /**
   * Provides the identifier of an Archival Unit if existing or after creating
   * it otherwise.
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
  public Long findOrCreateAu(Connection conn, Long pluginSeq, String auKey)
      throws DbException {
    final String DEBUG_HEADER = "findOrCreateAu(): ";
    Long auSeq = mdManagerSql.findAu(conn, pluginSeq, auKey);
    log.debug3(DEBUG_HEADER + "auSeq = " + auSeq);

    // Check whether it is a new AU.
    if (auSeq == null) {
      // Yes: Add to the database the new AU.
      auSeq = mdManagerSql.addAu(conn, pluginSeq, auKey);
      log.debug3(DEBUG_HEADER + "new auSeq = " + auSeq);
    }

    return auSeq;
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
  public Long findAu(Connection conn, Long pluginSeq, String auKey)
      throws DbException {
    return mdManagerSql.findAu(conn, pluginSeq, auKey);
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
  public Long addAuMd(Connection conn, Long auSeq, int version,
      long extractTime, long creationTime, Long providerSeq)
	  throws DbException {
    return mdManagerSql.addAuMd(conn, auSeq, version, extractTime, creationTime,
	providerSeq);
  }

  /**
   * Provides the identifier of a publication if existing or after creating it
   * otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the publication.
   * @param eIssn
   *          A String with the electronic ISSN of the publication.
   * @param pIsbn
   *          A String with the print ISBN of the publication.
   * @param eIsbn
   *          A String with the electronic ISBN of the publication.
   * @param pubType
   *          The type of publication: "journal", "book", "bookSeries" or
   *          "proceedings".
   * @param seriesName
   *          A string with the name of the book series.
   * @param proprietarySeriesId
   *          A String with the proprietary series identifier of the publication.
   * @param pubName
   *          A String with the name of the publication.
   * @param proprietaryId
   *          A String with the proprietary identifier of the publication.
   * @return a Long with the identifier of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreatePublication(Connection conn, Long publisherSeq, 
      String pIssn, String eIssn, String pIsbn, String eIsbn, 
      String pubType, String seriesName, String proprietarySeriesId, 
      String pubName, String proprietaryId) throws DbException {
    final String DEBUG_HEADER = "findOrCreatePublication(): ";
    Long publicationSeq = null;

    // Get the title name.
    String pubTitle = null;
    log.debug3(DEBUG_HEADER + "name = " + pubName);

    if (!StringUtil.isNullString(pubName)) {
      pubTitle = 
          pubName.substring(0, Math.min(pubName.length(), MAX_NAME_COLUMN));
    }

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pubTitle = " + pubTitle);

    // Check whether it is a book series.
    if (MetadataField.PUBLICATION_TYPE_BOOKSERIES.equals(pubType)
        || !StringUtil.isNullString(seriesName)) {
      // Yes: Find or create the book series.
      log.debug3(DEBUG_HEADER + "is book series.");
      String seriesTitle = 
          seriesName.substring(
              0, Math.min(seriesName.length(), MAX_NAME_COLUMN));
      
      publicationSeq = findOrCreateBookInBookSeries(conn, publisherSeq, 
          pIssn, eIssn, pIsbn, eIsbn, seriesTitle, proprietarySeriesId, 
	  pubTitle, proprietaryId);
      // No: Check whether it is a book.
    } else if (MetadataField.PUBLICATION_TYPE_BOOK.equals(pubType)
               || isBook(pIsbn, eIsbn)) {
      // Yes: Find or create the book.
      log.debug3(DEBUG_HEADER + "is book.");
      publicationSeq = findOrCreateBook(conn, publisherSeq, null, 
          pIsbn, eIsbn, pubTitle, proprietaryId);
      // No: Check whether it is a proceedings article.
    } else if (MetadataField.PUBLICATION_TYPE_PROCEEDINGS.equals(pubType)) {
      // Yes: Find or create the proceedings publication.
      log.debug3(DEBUG_HEADER + "is proceedings.");
      publicationSeq = findOrCreateProceedings(conn, publisherSeq, 
          pIssn, eIssn, pubTitle, proprietaryId);
    } else if (MetadataField.PUBLICATION_TYPE_FILE.equals(pubType)) {
      // Yes: Find or create the file publication.
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "is file.");
      publicationSeq = findOrCreateFile(conn, publisherSeq, pubTitle,
	  proprietaryId);
    } else {
      // No, it is a journal article: Find or create the journal.
      log.debug3(DEBUG_HEADER + "is journal.");
      publicationSeq = findOrCreateJournal(conn, publisherSeq, 
          pIssn, eIssn, pubTitle, proprietaryId);
    }

    return publicationSeq;
  }

  /**
   * Provides the identifier of a book series if existing
   * or after creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the book series.
   * @param eIssn
   *          A String with the electronic ISSN of the book series.
   * @param seriesTitle
   *          A String with the name of the book series
   * @param proprietarySeriesId
   *          A String with the proprietary identifier of the book series.
   * @return a Long with the identifier of the book series.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateBookSeries(Connection conn, Long publisherSeq, 
      String pIssn, String eIssn, String seriesTitle,
      String proprietarySeriesId) throws DbException {
    final String DEBUG_HEADER = "findOrCreateBookInBookSeries(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "seriesTitle = " + seriesTitle);
      log.debug2(DEBUG_HEADER + "proprietarySeriesId = " + proprietarySeriesId);
    }

    // Construct a title for the book in the series.
    // Find the book series.
    Long bookSeriesSeq = findPublication(conn, publisherSeq, seriesTitle, 
	pIssn, eIssn, null, null, MD_ITEM_TYPE_BOOK_SERIES);
    log.debug3(DEBUG_HEADER + "bookSeriesSeq = " + bookSeriesSeq);

    // Check whether it is a new book series.
    if (bookSeriesSeq == null) {
      // Yes: Add to the database the new book series.
      bookSeriesSeq = addPublication(conn, publisherSeq, null, 
          MD_ITEM_TYPE_BOOK_SERIES, seriesTitle);
      log.debug3(DEBUG_HEADER + "new bookSeriesSeq = " + bookSeriesSeq);

      // Skip it if the new book series could not be added.
      if (bookSeriesSeq == null) {
        log.error("Title for new book series '" + seriesTitle
            + "' could not be created.");
        return null;
      }

      // Get the book series metadata item identifier.
      Long mdItemSeq = findPublicationMetadataItem(conn, bookSeriesSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the new book series ISSN values.
      mdManagerSql.addMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
      log.debug3(DEBUG_HEADER + "added title ISSNs.");

      // Add to the database the new book series proprietary identifier.
      addMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietarySeriesId));

    } else {
      // No: Get the book series metadata item identifier.
      Long mdItemSeq = findPublicationMetadataItem(conn, bookSeriesSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the book series name in the metadata as an
      // alternate, if new.
      addNewMdItemName(conn, mdItemSeq, seriesTitle);
      log.debug3(DEBUG_HEADER + "added new title name.");

      // Add to the database the ISSN values in the metadata, if new.
      addNewMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
      log.debug3(DEBUG_HEADER + "added new title ISSNs.");

      // Add to the database the proprietary identifier, if new.
      addNewMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietarySeriesId));
    }

    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "bookSeriesSeq = " + bookSeriesSeq);
    }
    return bookSeriesSeq;
  }

  /**
   * Provides the identifier of a book that belongs to a book series if existing
   * or after creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the book series.
   * @param eIssn
   *          A String with the electronic ISSN of the book series.
   * @param pIsbn
   *          A String with the print ISBN of the book.
   * @param eIsbn
   *          A String with the electronic ISBN of the book.
   * @param seriesTitle
   *          A String with the name of the book series
   * @param proprietarySeriesId
   *          A String with the proprietary identifier of the book series.
   * @param bookTitle
   *          A String with the name of the book
   * @param proprietaryId
   *          A String with the proprietary identifier of the book.
   * @return a Long with the identifier of the book.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateBookInBookSeries(Connection conn, Long publisherSeq,
      String pIssn, String eIssn, String pIsbn, String eIsbn,
      String seriesTitle, String proprietarySeriesId,
      String bookTitle, String proprietaryId) 
      throws DbException {
    final String DEBUG_HEADER = "findOrCreateBookInBookSeries(): ";

    // Find or create the book series
    Long bookSeriesSeq = findOrCreateBookSeries(conn, publisherSeq,  
        pIssn, eIssn, seriesTitle, proprietarySeriesId);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER 
                                   + "bookSeriesSeq = " + bookSeriesSeq);
    
    // Get the book series metadata item identifier.
    Long mdItemSeq = findPublicationMetadataItem(conn, bookSeriesSeq);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

    // Find or create the book in the series
    Long bookSeq = findOrCreateBook(conn, publisherSeq, mdItemSeq, 
        pIsbn, eIsbn, bookTitle, proprietaryId);

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "bookSeq = " + bookSeq);
    return bookSeq;
  }

  /**
   * Provides an indication of whether a metadata set corresponds to a book.
   * 
   * @param pIsbn
   *          A String with the print ISBN in the metadata.
   * @param eIsbn
   *          A String with the electronic ISBN in the metadata.
   * @return <code>true</code> if the metadata set corresponds to a book,
   *         <code>false</code> otherwise.
   */
  static public boolean isBook(String pIsbn, String eIsbn) {
    final String DEBUG_HEADER = "isBook(): ";

    // If there are ISBN values in the metadata, it is a book or a book series.
    boolean isBook =    !StringUtil.isNullString(pIsbn) 
                     || !StringUtil.isNullString(eIsbn);
    log.debug3(DEBUG_HEADER + "isBook = " + isBook);

    return isBook;
  }

  /**
   * Provides the identifier of a book existing or after creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param seriesMdItemSeq
   *          A Long with the publication parent metadata item parent identifier.
   * @param pIsbn
   *          A String with the print ISBN of the book.
   * @param eIsbn
   *          A String with the electronic ISBN of the book.
   * @param title
   *          A String with the name of the book.
   * @param proprietaryId
   *          A String with the proprietary identifier of the book.
   * @return a Long with the identifier of the book.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateBook(Connection conn, 
      Long publisherSeq, Long seriesMdItemSeq,  
      String pIsbn, String eIsbn, 
      String title, String proprietaryId)
      throws DbException {
    final String DEBUG_HEADER = "findOrCreateBook(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "seriesMdItemSeq = " + seriesMdItemSeq);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "proprietaryId = " + proprietaryId);
    }

    // Find the book.
   Long publicationSeq =
	findPublication(conn, publisherSeq, title, null, null, pIsbn, eIsbn,
			MD_ITEM_TYPE_BOOK);
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    // Check whether it is a new book.
    if (publicationSeq == null) {
      // Yes: Add to the database the new book.
      publicationSeq = addPublication(conn, publisherSeq, seriesMdItemSeq, 
          MD_ITEM_TYPE_BOOK, title);
      log.debug3(DEBUG_HEADER + "new publicationSeq = " + publicationSeq);

      // Skip it if the new book could not be added.
      if (publicationSeq == null) {
	log.error("Publication for new book '" + title
	    + "' could not be created.");
	return publicationSeq;
      }

      // Get the book metadata item identifier.
      Long mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the new book ISBN values.
      mdManagerSql.addMdItemIsbns(conn, mdItemSeq, pIsbn, eIsbn);
      log.debug3(DEBUG_HEADER + "added title ISBNs.");

      // Add to the database the new book proprietary identifier.
      addMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    } else {
      // No: Get the book metadata item identifier.
      Long mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the book name in the metadata as an alternate,
      // if new.
      addNewMdItemName(conn, mdItemSeq, title);
      log.debug3(DEBUG_HEADER + "added new title name.");

      // Add to the database the ISBN values in the metadata, if new.
      addNewMdItemIsbns(conn, mdItemSeq, pIsbn, eIsbn);
      log.debug3(DEBUG_HEADER + "added new title ISBNs.");

      // Add to the database the proprietary identifier, if not there already.
      addNewMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of a journal if existing or after creating it
   * otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the journal.
   * @param eIssn
   *          A String with the electronic ISSN of the journal.
   * @param title
   *          A String with the name of the journal.
   * @param proprietaryId
   *          A String with the proprietary identifier of the journal.
   * @return a Long with the identifier of the journal.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateJournal(Connection conn, Long publisherSeq, 
      String pIssn, String eIssn, String title, String proprietaryId)
      throws DbException {
    final String DEBUG_HEADER = "findOrCreateJournal(): ";
    Long publicationSeq = null;
    Long mdItemSeq = null;

    // Skip it if it no title name or ISSNs, as it will not be possible to
    // find the journal to which it belongs in the database.
    if (StringUtil.isNullString(title) && pIssn == null && eIssn == null) {
      log.error("Title for article cannot be created as it has no name or ISSN "
	  + "values.");
      return publicationSeq;
    }

    // Find the journal.
    publicationSeq =
	findPublication(conn, publisherSeq, title, pIssn, eIssn, null, null,
			MD_ITEM_TYPE_JOURNAL);
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    // Check whether it is a new journal.
    if (publicationSeq == null) {
      // Yes: Add to the database the new journal.
      publicationSeq = addPublication(conn, publisherSeq, null, 
          MD_ITEM_TYPE_JOURNAL, title);
      log.debug3(DEBUG_HEADER + "new publicationSeq = " + publicationSeq);

      // Skip it if the new journal could not be added.
      if (publicationSeq == null) {
	log.error("Publication for new journal '" + title
	    + "' could not be created.");
	return publicationSeq;
      }

      // Get the journal metadata item identifier.
      mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the new journal ISSN values.
      mdManagerSql.addMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
      log.debug3(DEBUG_HEADER + "added title ISSNs.");

      // Add to the database the new book proprietary identifier.
      addMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    } else {
      // No: Get the journal metadata item identifier.
      mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // No: Add to the database the journal name in the metadata as an
      // alternate, if new.
      addNewMdItemName(conn, mdItemSeq, title);
      log.debug3(DEBUG_HEADER + "added new title name.");

      // Add to the database the ISSN values in the metadata, if new.
      addNewMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
      log.debug3(DEBUG_HEADER + "added new title ISSNs.");

      // Add to the database the proprietary identifier, if not there already.
      addNewMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    }

    return publicationSeq;
  }

  /**
   * Provides the identifier of a proceedings publication if existing or after
   * creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the proceedings publication.
   * @param eIssn
   *          A String with the electronic ISSN of the proceedings publication.
   * @param title
   *          A String with the name of the proceedings publication.
   * @param proprietaryId
   *          A String with the proprietary identifier of the proceedings
   *          publication.
   * @return a Long with the identifier of the proceedings publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateProceedings(Connection conn, Long publisherSeq, 
      String pIssn, String eIssn, String title, String proprietaryId)
      throws DbException {
    final String DEBUG_HEADER = "findOrCreateProceedings(): ";
    Long publicationSeq = null;
    Long mdItemSeq = null;

    // Skip it if it no title name or ISSNs, as it will not be possible to
    // find the proceedings publication to which it belongs in the database.
    if (StringUtil.isNullString(title) && pIssn == null && eIssn == null) {
      log.error("Title for article cannot be created as it has no name or ISSN "
	  + "values.");
      return publicationSeq;
    }

    // Find the proceedings publication.
    publicationSeq =
	findPublication(conn, publisherSeq, title, pIssn, eIssn, null, null,
			MD_ITEM_TYPE_PROCEEDINGS);
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    // Check whether it is a new proceedings publication.
    if (publicationSeq == null) {
      // Yes: Add to the database the new proceedings publication.
      publicationSeq = addPublication(conn, publisherSeq, null, 
	  MD_ITEM_TYPE_PROCEEDINGS, title);
      log.debug3(DEBUG_HEADER + "new publicationSeq = " + publicationSeq);

      // Skip it if the new proceedings publication could not be added.
      if (publicationSeq == null) {
	log.error("Publication for new proceedings publication '" + title
	    + "' could not be created.");
	return publicationSeq;
      }

      // Get the proceedings publication metadata item identifier.
      mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the new proceedings publication ISSN values.
      mdManagerSql.addMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
      log.debug3(DEBUG_HEADER + "added title ISSNs.");

      // Add to the database the new proceedings publication proprietary
      // identifier.
      addMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    } else {
      // No: Get the proceedings publication metadata item identifier.
      mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // No: Add to the database the proceedings publication name in the
      // metadata as an alternate, if new.
      addNewMdItemName(conn, mdItemSeq, title);
      log.debug3(DEBUG_HEADER + "added new title name.");

      // Add to the database the ISSN values in the metadata, if new.
      addNewMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
      log.debug3(DEBUG_HEADER + "added new title ISSNs.");

      // Add to the database the proprietary identifier, if not there already.
      addNewMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    }

    return publicationSeq;
  }

  /**
   * Provides the identifier of a file existing or after creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param title
   *          A String with the name of the file.
   * @param proprietaryId
   *          A String with the proprietary identifier of the publication.
   * @return a Long with the identifier of the file.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateFile(Connection conn, Long publisherSeq,
      String title, String proprietaryId) throws DbException {
    final String DEBUG_HEADER = "findOrCreateFile(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "proprietaryId = " + proprietaryId);
    }

    // Find the file.
    Long publicationSeq =
	findPublication(conn, publisherSeq, title, null, null, null, null,
			MD_ITEM_TYPE_FILE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    // Check whether it is a new file.
    if (publicationSeq == null) {
      // Yes: Add to the database the new file.
      publicationSeq = addPublication(conn, publisherSeq, null,
	  MD_ITEM_TYPE_FILE, title);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "new publicationSeq = " + publicationSeq);

      // Skip it if the new file could not be added.
      if (publicationSeq == null) {
	log.error("Publication for new file '" + title
	    + "' could not be created.");
	return publicationSeq;
      }

      // Get the file metadata item identifier.
      Long mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the new file publication proprietary identifier.
      addMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    } else {
      // No: Get the file metadata item identifier.
      Long mdItemSeq = findPublicationMetadataItem(conn, publicationSeq);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);

      // Add to the database the file publication name in the metadata as an
      // alternate, if new.
      addNewMdItemName(conn, mdItemSeq, title);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "added new title name.");

      // Add to the database the proprietary identifier, if not there already.
      addNewMdItemProprietaryIds(conn, mdItemSeq,
	  Collections.singleton(proprietaryId));
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of a publication by its title, publisher, ISSNs
   * and/or ISBNs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param title
   *          A String with the title of the publication.
   * @param pIssn
   *          A String with the print ISSN of the publication.
   * @param eIssn
   *          A String with the electronic ISSN of the publication.
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
  public Long findPublication(Connection conn, Long publisherSeq, String title,
      String pIssn, String eIssn, String pIsbn, String eIsbn, String mdItemType)
      throws DbException {
    final String DEBUG_HEADER = "findPublication(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
    }

    Long publicationSeq = null;
    boolean hasIssns = pIssn != null || eIssn != null;
    log.debug3(DEBUG_HEADER + "hasIssns = " + hasIssns);
    boolean hasIsbns = pIsbn != null || eIsbn != null;
    log.debug3(DEBUG_HEADER + "hasIsbns = " + hasIsbns);
    boolean hasName = !StringUtil.isNullString(title);
    log.debug3(DEBUG_HEADER + "hasName = " + hasName);

    if (!hasIssns && !hasIsbns && !hasName) {
      log.debug3(DEBUG_HEADER + "Cannot find publication with no name, ISSNs"
	  + " or ISBNs.");
      return null;
    }

    if (hasIssns && hasIsbns && hasName) {
      publicationSeq = findPublicationByIssnsOrIsbnsOrName(conn, title,
	  publisherSeq, pIssn, eIssn, pIsbn, eIsbn, mdItemType);
    } else if (hasIssns && hasName) {
      publicationSeq = findPublicationByIssnsOrName(conn, publisherSeq, title,
	  pIssn, eIssn, mdItemType);
    } else if (hasIsbns && hasName) {
      publicationSeq = findPublicationByIsbnsOrName(conn, publisherSeq, title,
	  pIsbn, eIsbn, mdItemType, false);
    } else if (hasIssns) {
      publicationSeq = mdManagerSql.findPublicationByIssns(conn, publisherSeq,
	  pIssn, eIssn, mdItemType);
    } else if (hasIsbns) {
      publicationSeq = mdManagerSql.findPublicationByIsbns(conn, publisherSeq,
	  pIsbn, eIsbn, mdItemType);
    } else if (hasName) {
      publicationSeq = mdManagerSql.findPublicationByName(conn, publisherSeq,
	  title, mdItemType);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Adds a publication to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param parentMdItemSeq
   *          A Long with the publication parent metadata item parent identifier.
   * @param mdItemType
   *          A String with the type of publication.
   * @param title
   *          A String with the title of the publication.
   * @return a Long with the identifier of the publication just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addPublication(Connection conn, Long publisherSeq,
      Long parentMdItemSeq, String mdItemType, String title)
	  throws DbException {
    return mdManagerSql.addPublication(conn, publisherSeq, parentMdItemSeq,
	mdItemType, title);
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
  public Long findPublicationMetadataItem(Connection conn, Long publicationSeq)
      throws DbException {
    return mdManagerSql.findPublicationMetadataItem(conn, publicationSeq);
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
  public Long findParentMetadataItem(Connection conn, Long mditemSeq)
      throws DbException {
    return mdManagerSql.findParentMetadataItem(conn, mditemSeq);
  }

  /**
   * Adds to the database the name of a metadata item, if it does not exist
   * already.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param mdItemName
   *          A String with the name to be added, if new.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void addNewMdItemName(Connection conn, Long mdItemSeq,
      String mdItemName) throws DbException {
    final String DEBUG_HEADER = "addNewMdItemName(): ";

    if (mdItemName == null) {
      return;
    }

    Map<String, String> titleNames =
	mdManagerSql.getMdItemNames(conn, mdItemSeq);

    for (String name : titleNames.keySet()) {
      if (name.equals(mdItemName)) {
	log.debug3(DEBUG_HEADER + "Title name = " + mdItemName
	    + " already exists.");
	return;
      }
    }

    addMdItemName(conn, mdItemSeq, mdItemName, NOT_PRIMARY_NAME_TYPE);
  }

  /**
   * Adds to the database the ISSNs of a metadata item, if they do not exist
   * already.
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
  public void addNewMdItemIssns(Connection conn, Long mdItemSeq, String pIssn,
      String eIssn) throws DbException {
    final String DEBUG_HEADER = "addNewMdItemIssns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
    }

    if (pIssn == null && eIssn == null) {
      return;
    }

    String issnType;
    String issnValue;

    // Find the existing ISSNs for the current metadata item.
    Set<Issn> issns = mdManagerSql.getMdItemIssns(conn, mdItemSeq);

    // Loop through all the ISSNs found.
    for (Issn issn : issns) {
      // Get the ISSN value.
      issnValue = issn.getValue();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issnValue = " + issnValue);

      // Get the ISSN type.
      issnType = issn.getType();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "issnType = " + issnType);

      // Check whether this ISSN matches the passed print ISSN.
      if (pIssn != null
	  && pIssn.equals(issnValue)
	  && P_ISSN_TYPE.equals(issnType)) {
	// Yes: Skip it as it is already stored.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Skipped storing already existing pIssn = " + pIssn);
	pIssn = null;
	continue;
      }

      // Check whether this ISSN matches the passed electronic ISSN.
      if (eIssn != null
	  && eIssn.equals(issnValue)
	  && E_ISSN_TYPE.equals(issnType)) {
	// Yes: Skip it as it is already stored.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Skipped storing already existing eIssn = " + eIssn);
	eIssn = null;
      }
    }

    mdManagerSql.addMdItemIssns(conn, mdItemSeq, pIssn, eIssn);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Adds to the database publication proprietary identifiers, if not already
   * there.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param proprietaryIds
   *          A Collection<String> with the proprietary identifiers of the
   *          metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void addNewMdItemProprietaryIds(Connection conn, Long mdItemSeq,
      Collection<String> proprietaryIds) throws DbException {
    final String DEBUG_HEADER = "addNewMdItemProprietaryIds(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "proprietaryIds = " + proprietaryIds);
    }

    // Initialize the collection of proprietary identifiers to be added.
    ArrayList<String> newProprietaryIds = new ArrayList<String>(proprietaryIds);

    Collection<String> oldProprietaryIds =
	mdManagerSql.getMdItemProprietaryIds(conn, mdItemSeq);

    // Remove them from the collection to be added.
    newProprietaryIds.removeAll(oldProprietaryIds);

    // Add the proprietary identifiers that are new.
    addMdItemProprietaryIds(conn, mdItemSeq, newProprietaryIds);
  }

  /**
   * Adds to the database the proprietary identifiers of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param proprietaryIds
   *          A Collection<String> with the proprietary identifiers of the
   *          metadata item.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void addMdItemProprietaryIds(Connection conn, Long mdItemSeq,
      Collection<String> proprietaryIds) throws DbException {
    final String DEBUG_HEADER = "addMdItemProprietaryIds(): ";

    if (proprietaryIds == null || proprietaryIds.size() == 0) {
      return;
    }

    for (String proprietaryId : proprietaryIds) {
      dbManager.addMdItemProprietaryId(conn, mdItemSeq, proprietaryId);

      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "Added proprietary identifier = " + proprietaryId);
    }
  }

  /**
   * Adds to the database the ISBNs of a metadata item, if they do not exist
   * already.
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
  public void addNewMdItemIsbns(Connection conn, Long mdItemSeq, String pIsbn,
      String eIsbn) throws DbException {
    final String DEBUG_HEADER = "addNewMdItemIsbns(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
    }

    if (pIsbn == null && eIsbn == null) {
      return;
    }

    String isbnType;
    String isbnValue;

    // Find the existing ISBNs for the current metadata item.
    Set<Isbn> isbns = mdManagerSql.getMdItemIsbns(conn, mdItemSeq);

    // Loop through all the ISBNs found.
    for (Isbn isbn : isbns) {
      // Get the ISBN value.
      isbnValue = isbn.getValue();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbnValue = " + isbnValue);

      // Get the ISBN type.
      isbnType = isbn.getType();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "isbnType = " + isbnType);

      // Check whether this ISBN matches the passed print ISBN.
      if (pIsbn != null
	  && pIsbn.equals(isbnValue)
	  && P_ISBN_TYPE.equals(isbnType)) {
	// Yes: Skip it as it is already stored.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Skipped storing already existing pIsbn = " + pIsbn);
	pIsbn = null;
      }

      // Check whether this ISBN matches the passed electronic ISBN.
      if (eIsbn != null
	  && eIsbn.equals(isbnValue)
	  && E_ISBN_TYPE.equals(isbnType)) {
	// Yes: Skip it as it is already stored.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Skipped storing already existing eIsbn = " + eIsbn);
	eIsbn = null;
      }
    }

    mdManagerSql.addMdItemIsbns(conn, mdItemSeq, pIsbn, eIsbn);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the identifier of a publication by its title, publisher, ISSNs and
   * ISBNs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param title
   *          A String with the title of the publication.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the publication.
   * @param eIssn
   *          A String with the electronic ISSN of the publication.
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
  private Long findPublicationByIssnsOrIsbnsOrName(Connection conn,
      String title, Long publisherSeq, String pIssn, String eIssn, String pIsbn,
      String eIsbn, String mdItemType) throws DbException {
    final String DEBUG_HEADER = "findPublicationByIssnsOrIsbnsOrName(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
    }

    Long publicationSeq = mdManagerSql.findPublicationByIssns(conn,
	publisherSeq, pIssn, eIssn, mdItemType);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    if (publicationSeq == null) {
      publicationSeq = findPublicationByIsbnsOrName(conn, publisherSeq, title,
	  pIsbn, eIsbn, mdItemType, true);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of a publication by its publisher and title or
   * ISSNs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param title
   *          A String with the title of the publication.
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
  private Long findPublicationByIssnsOrName(Connection conn, Long publisherSeq, 
      String title, String pIssn, String eIssn, String mdItemType)
	  throws DbException {
    final String DEBUG_HEADER = "findPublicationByIssnsOrName(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "pIssn = " + pIssn);
      log.debug2(DEBUG_HEADER + "eIssn = " + eIssn);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
    }

    Long publicationSeq = mdManagerSql.findPublicationByIssns(conn,
	publisherSeq, pIssn, eIssn, mdItemType);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    if (publicationSeq == null) {
      publicationSeq = mdManagerSql.findPublicationByName(conn, publisherSeq,
	  title, mdItemType);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

      // Disqualify this matched publication if it already has some ISSN.
      if (publicationSeq != null && publicationHasIssns(conn, publicationSeq)) {
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "Potential match of publicationSeq = "
	      + publicationSeq + " disqualified - publicationHasIssns = "
	      + publicationHasIssns(conn, publicationSeq));
	publicationSeq = null;
      }
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "publicationSeq = " + publicationSeq);
    return publicationSeq;
  }

  /**
   * Provides the identifier of a publication by its publisher and title or
   * ISBNs.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param title
   *          A String with the title of the publication.
   * @param pIsbn
   *          A String with the print ISBN of the publication.
   * @param eIsbn
   *          A String with the electronic ISBN of the publication.
   * @param mdItemType
   *          A String with the type of publication to be identified.
   * @param newHasIssns
   *          A boolean with the indication of whether the data of the
   *          publication to be matched contains also some ISSN.
   * @return a Long with the identifier of the publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findPublicationByIsbnsOrName(Connection conn, Long publisherSeq, 
      String title, String pIsbn, String eIsbn, String mdItemType,
      boolean newHasIssns) throws DbException {
    final String DEBUG_HEADER = "findPublicationByIsbnsOrName(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "publisherSeq = " + publisherSeq);
      log.debug2(DEBUG_HEADER + "title = " + title);
      log.debug2(DEBUG_HEADER + "pIsbn = " + pIsbn);
      log.debug2(DEBUG_HEADER + "eIsbn = " + eIsbn);
      log.debug2(DEBUG_HEADER + "mdItemType = " + mdItemType);
      log.debug2(DEBUG_HEADER + "newHasIssns = " + newHasIssns);
    }

    Long publicationSeq = mdManagerSql.findPublicationByIsbns(conn,
	publisherSeq, pIsbn, eIsbn, mdItemType);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    if (publicationSeq == null) {
      publicationSeq = mdManagerSql.findPublicationByName(conn, publisherSeq,
	  title, mdItemType);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

      // Disqualify this matched publication if it already has some ISBN or if
      // it has some ISSN and the incoming data also contained some ISSN.
      if (publicationSeq != null
	  && (publicationHasIsbns(conn, publicationSeq)
	      || (newHasIssns && publicationHasIssns(conn, publicationSeq)))) {
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "Potential match of publicationSeq = "
	      + publicationSeq + " disqualified - publicationHasIsbns = "
	      + publicationHasIsbns(conn, publicationSeq) + ", newHasIssns = "
	      + newHasIssns + ", publicationHasIssns = "
	      + publicationHasIssns(conn, publicationSeq));
	publicationSeq = null;
      }
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
  public boolean publicationHasIsbns(Connection conn, Long publicationSeq)
      throws DbException {
    return mdManagerSql.publicationHasIsbns(conn, publicationSeq);
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
  public boolean publicationHasIssns(Connection conn, Long publicationSeq)
      throws DbException {
    return mdManagerSql.publicationHasIssns(conn, publicationSeq);
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
  public Long findMetadataItemType(Connection conn, String typeName)
      throws DbException {
    return mdManagerSql.findMetadataItemType(conn, typeName);
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
  public Long addMdItem(Connection conn, Long parentSeq, Long mdItemTypeSeq,
      Long auMdSeq, String date, String coverage, long fetchTime)
	  throws DbException {
    return mdManagerSql.addMdItem(conn, parentSeq, mdItemTypeSeq, auMdSeq,
	date, coverage, fetchTime);
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
  public void addMdItemName(Connection conn, Long mdItemSeq, String name,
      String type) throws DbException {
    mdManagerSql.addMdItemName(conn, mdItemSeq, name, type);
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
  public void addMdItemUrl(Connection conn, Long mdItemSeq, String feature,
      String url) throws DbException {
    mdManagerSql.addMdItemUrl(conn, mdItemSeq, feature, url);
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
  public void addMdItemDoi(Connection conn, Long mdItemSeq, String doi)
      throws DbException {
    mdManagerSql.addMdItemDoi(conn, mdItemSeq, doi);
  }

  /**
   * Adds to the database a generic metadata key/value pair of a metadata item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param key
   *          A String with the key of the metadata item key/value pair.
   * @param value
   *          A String with the value of the metadata item key/value pair.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void addMdItemMdPair(Connection conn, Long mdItemSeq, String key,
      String value) throws DbException {
    final String DEBUG_HEADER = "addMdItemMdPair(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "key = " + key);
      log.debug2(DEBUG_HEADER + "value = " + value);
    }

    Long mdKeySeq = findOrCreateMdKey(conn, key);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdKeySeq = " + mdKeySeq);

    mdManagerSql.addMdPair(conn, mdItemSeq, mdKeySeq, value);
  }

  /**
   * Adds to the database the generic metadata key/value pairs of a metadata
   * item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param mdMap
   *          A Map<String, String> with the metadata key/value pairs to be
   *          added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public void addMdItemMd(Connection conn, Long mdItemSeq,
      Map<String, String> mdMap) throws DbException {
    final String DEBUG_HEADER = "addMdItemMd(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
      log.debug2(DEBUG_HEADER + "mdMap = " + mdMap);
    }

    // Loop through all the generic metadata key/value pairs.
    for (String key : mdMap.keySet()) {
      // Add the key/value pair.
      addMdItemMdPair(conn, mdItemSeq, key, mdMap.get(key));
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Added key = " + key
	  + ", value = " + mdMap.get(key));
    }
  }

  /**
   * Provides the identifier of a publishing platform if existing or after
   * creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param platformName
   *          A String with the platform name.
   * @return a Long with the identifier of the publishing platform.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreatePlatform(Connection conn, String platformName)
      throws DbException {
    final String DEBUG_HEADER = "findOrCreatePlatform(): ";
    log.debug3(DEBUG_HEADER + "platformName = " + platformName);
    
    if (platformName == null) {
      platformName = NO_PLATFORM;
    }

    Long platformSeq = findPlatform(conn, platformName);
    log.debug3(DEBUG_HEADER + "platformSeq = " + platformSeq);

    // Check whether it is a new platform.
    if (platformSeq == null) {
      // Yes: Add to the database the new platform.
      platformSeq = mdManagerSql.addPlatform(conn, platformName);
      log.debug3(DEBUG_HEADER + "new platformSeq = " + platformSeq);
    }

    return platformSeq;
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
  public Long findPlatform(Connection conn, String platformName)
      throws DbException {
    return mdManagerSql.findPlatform(conn, platformName);
  }

  /**
   * Provides the publication identifier of an existing book series, 
   * null otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param pIssn
   *          A String with the print ISSN of the series.
   * @param eIssn
   *          A String with the electronic ISSN of the series.
   * @param seriesName
   *          A String with the name of the series.
   * @return a Long with the identifier of the series publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findBookSeries(Connection conn, Long publisherSeq,
      String pIssn, String eIssn, String seriesName) throws DbException {
    Long seriesSeq =
        findPublication(conn, publisherSeq, seriesName, 
                        pIssn, eIssn, null, null, MD_ITEM_TYPE_BOOK_SERIES);
    return seriesSeq;
  }
  
  /**
   * Provides the identifier of an existing journal, null otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pIssn
   *          A String with the print ISSN of the journal.
   * @param eIssn
   *          A String with the electronic ISSN of the journal.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param title
   *          A String with the name of the journal.
   * @return a Long with the identifier of the journal.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findJournal(Connection conn, Long publisherSeq, 
      String pIssn, String eIssn, String title) throws DbException {
    final String DEBUG_HEADER = "findJournal(): ";
    Long publicationSeq = null;

    // Skip it if it no title name or ISSNs, as it will not be possible to
    // find the journal to which it belongs in the database.
    if (StringUtil.isNullString(title) && pIssn == null && eIssn == null) {
      log.error("Title for article cannot be created as it has no name or ISSN "
	  + "values.");
      return publicationSeq;
    }

    // Find the journal.
    publicationSeq =
	findPublication(conn, publisherSeq, 
	                title, pIssn, eIssn, null, null, MD_ITEM_TYPE_JOURNAL);
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    return publicationSeq;
  }
  
  /**
   * Provides the identifier of an existing proceedings publication, null
   * otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pIssn
   *          A String with the print ISSN of the proceedings publication.
   * @param eIssn
   *          A String with the electronic ISSN of the proceedings publication.
   * @param publisherSeq
   *          A Long with the publisher identifier.
   * @param title
   *          A String with the name of the proceedings publication.
   * @return a Long with the identifier of the proceedings publication.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findProceedings(Connection conn, Long publisherSeq, 
      String pIssn, String eIssn, String title) throws DbException {
    final String DEBUG_HEADER = "findProceedings(): ";
    Long publicationSeq = null;

    // Skip it if it no title name or ISSNs, as it will not be possible to
    // find the proceedings publication to which it belongs in the database.
    if (StringUtil.isNullString(title) && pIssn == null && eIssn == null) {
      log.error("Title for article cannot be created as it has no name or ISSN "
	  + "values.");
      return publicationSeq;
    }

    // Find the proceedings publication.
    publicationSeq = findPublication(conn, publisherSeq, title, pIssn, eIssn,
	null, null, MD_ITEM_TYPE_PROCEEDINGS);
    log.debug3(DEBUG_HEADER + "publicationSeq = " + publicationSeq);

    return publicationSeq;
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
  public void persistUnconfiguredAu(Connection conn, String auId)
      throws DbException {
    mdManagerSql.persistUnconfiguredAu(conn, auId);
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
  public long countUnconfiguredAus(Connection conn) throws DbException {
    return mdManagerSql.countUnconfiguredAus(conn);
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
  public boolean isAuInUnconfiguredAuTable(Connection conn, String auId)
      throws DbException {
    return mdManagerSql.isAuInUnconfiguredAuTable(conn, auId);
  }

  /**
   * Provides the SQL code executor.
   * 
   * @return a MetadataManagerSql with the SQL code executor.
   */
  MetadataManagerSql getMetadataManagerSql() {
    return mdManagerSql;
  }

  /**
   * Provides the names of the publishers in the database.
   * 
   * @return a Collection<String> with the publisher names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<String> getPublisherNames() throws DbException {
    return mdManagerSql.getPublisherNames();
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
  public Map<String, Collection<String>> getPublishersWithMultipleDoiPrefixes()
      throws DbException {
    return mdManagerSql.getPublishersWithMultipleDoiPrefixes();
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
  public Map<String, Collection<String>> getDoiPrefixesWithMultiplePublishers()
      throws DbException {
    return mdManagerSql.getDoiPrefixesWithMultiplePublishers();
  }

  /**
   * Provides the DOI prefixes linked to the Archival Unit name for the Archival
   * Units in the database with multiple DOI prefixes.
   * 
   * @return a Map<String, Collection<String>> with the DOI prefixes keyed by
   *         the Archival Unit name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Collection<String>> getAuNamesWithMultipleDoiPrefixes()
      throws DbException {
    final String DEBUG_HEADER = "getAuNamesWithMultipleDoiPrefixes(): ";

    // The Archival Units that have multiple DOI prefixes, sorted by name.
    Map<String, Collection<String>> auNamesWithPrefixes =
	new TreeMap<String, Collection<String>>();

    // Get the DOI prefixes linked to the Archival Units.
    Map<String, Collection<String>> ausDoiPrefixes =
	getAuIdsWithMultipleDoiPrefixes();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "ausDoiPrefixes.size() = " + ausDoiPrefixes.size());

    // Loop through the Archival Units.
    for (String auId : ausDoiPrefixes.keySet()) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

      ArchivalUnit au = pluginMgr.getAuFromIdIfExists(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "au = " + au);

      if (au != null) {
	auNamesWithPrefixes.put(au.getName(), ausDoiPrefixes.get(auId));
      } else {
	auNamesWithPrefixes.put(auId, ausDoiPrefixes.get(auId));
      }
    }

    return auNamesWithPrefixes;
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
  public Map<String, Collection<String>> getAuIdsWithMultipleDoiPrefixes()
      throws DbException {
    return mdManagerSql.getAuIdsWithMultipleDoiPrefixes();
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
  public Map<String, Collection<Isbn>> getPublicationsWithMoreThan2Isbns()
      throws DbException {
    return mdManagerSql.getPublicationsWithMoreThan2Isbns();
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
  public Map<PkNamePair, Collection<Issn>> getPublicationsWithMoreThan2Issns()
      throws DbException {
    return mdManagerSql.getPublicationsWithMoreThan2Issns();
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
  public Map<String, Collection<String>> getIsbnsWithMultiplePublications()
      throws DbException {
    return mdManagerSql.getIsbnsWithMultiplePublications();
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
  public Map<String, Collection<String>> getIssnsWithMultiplePublications()
      throws DbException {
    return mdManagerSql.getIssnsWithMultiplePublications();
  }

  /**
   * Provides the ISSNs for books in the database.
   * 
   * @return a Map<String, Collection<String>> with the ISSNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Collection<String>> getBooksWithIssns()
      throws DbException {
    return mdManagerSql.getBooksWithIssns();
  }

  /**
   * Provides the ISBNs for periodicals in the database.
   * 
   * @return a Map<String, Collection<String>> with the ISBNs keyed by the
   *         publication name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Collection<String>> getPeriodicalsWithIsbns()
      throws DbException {
    return mdManagerSql.getPeriodicalsWithIsbns();
  }

  /**
   * Provides the Archival Units in the database with an unknown provider.
   * 
   * @return a Collection<String> with the sorted Archival Unit names.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<String> getUnknownProviderAuIds() throws DbException {
    return mdManagerSql.getUnknownProviderAuIds();
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
  public Collection<Map<String, String>> getMismatchedParentJournalArticles()
      throws DbException {
    return mdManagerSql.getMismatchedParentJournalArticles();
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
  public Collection<Map<String, String>> getMismatchedParentBookChapters()
      throws DbException {
    return mdManagerSql.getMismatchedParentBookChapters();
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
  public Collection<Map<String, String>> getMismatchedParentBookVolumes()
      throws DbException {
    return mdManagerSql.getMismatchedParentBookVolumes();
  }

  /**
   * Provides the publishers linked to the Archival Unit name for the Archival
   * Units in the database with multiple publishers.
   * 
   * @return a Map<String, Collection<String>> with the publishers keyed by
   *         the Archival Unit name.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Collection<String>> getAuNamesWithMultiplePublishers()
      throws DbException {
    final String DEBUG_HEADER = "getAuNamesWithMultiplePublishers(): ";

    // The Archival Units that have multiple publishers, sorted by name.
    Map<String, Collection<String>> auNamesWithPublishers =
	new TreeMap<String, Collection<String>>();

    // Get the publishers linked to the Archival Units.
    Map<String, Collection<String>> ausPublishers =
	getAuIdsWithMultiplePublishers();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "ausPublishers.size() = "
	+ ausPublishers.size());

    // Loop through the Archival Units.
    for (String auId : ausPublishers.keySet()) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auId = " + auId);

      ArchivalUnit au = pluginMgr.getAuFromIdIfExists(auId);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "au = " + au);

      if (au != null) {
	auNamesWithPublishers.put(au.getName(), ausPublishers.get(auId));
      } else {
	auNamesWithPublishers.put(auId, ausPublishers.get(auId));
      }
    }

    return auNamesWithPublishers;
  }

  /**
   * Provides the publishers linked to the Archival Unit identifier for the
   * Archival Units in the database with multiple publishers.
   * 
   * @return a Map<String, Collection<String>> with the publishers keyed by
   *         the Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Collection<String>> getAuIdsWithMultiplePublishers()
      throws DbException {
    return mdManagerSql.getAuIdsWithMultiplePublishers();
  }

  /**
   * Provides the metadata items in the database that have no name.
   * 
   * @return a Collection<Map<String, String>> with the unnamed metadata items
   *         sorted by publisher, parent type, parent title and item type.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<Map<String, String>> getUnnamedItems() throws DbException {
    return mdManagerSql.getUnnamedItems();
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
  public Map<String, Collection<String>> getPublicationsWithMultiplePids()
      throws DbException {
    return mdManagerSql.getPublicationsWithMultiplePids();
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
  public Collection<Map<String, String>> getNoDoiItems() throws DbException {
    return mdManagerSql.getNoDoiItems();
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
  public Collection<Map<String, String>> getNoAccessUrlItems()
      throws DbException {
    return mdManagerSql.getNoAccessUrlItems();
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
  public boolean deletePublicationIssn(Long mdItemSeq, String issn,
      String issnType) throws DbException {
    return mdManagerSql.deletePublicationIssn(mdItemSeq, issn, issnType);
  }

  /**
   * Provides the Archival Units in the database with no metadata items.
   * 
   * @return a Collection<String> with the sorted Archival Unit identifiers.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<String> getNoItemsAuIds() throws DbException {
    return mdManagerSql.getNoItemsAuIds();
  }

  /**
   * Provides the metadata information of an Archival Unit.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a Map<String, Object> with the metadata information of the Archival
   *         Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Object> getAuMetadata(String auId) throws DbException {
    final String DEBUG_HEADER = "getAuMetadata(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

    Map<String, Object> result = null;
    Connection conn = null;

    try {
      conn = dbManager.getConnection();

      if (conn == null) {
	String message = "Cannot get metadata for AU '" + auId
	    + "' - Cannot connect to database";
	log.error(message);
	throw new DbException(message);
      }

      result = mdManagerSql.getAuMetadata(conn, auId);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "result = '" + result + "'");
    } catch (DbException dbe) {
      String message = "Cannot get metadata for AU '" + auId + "'";
      log.error(message, dbe);
      throw dbe;
    } finally {
      MetadataDbManager.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = '" + result + "'");
    return result;
  }

  /**
   * Provides the Archival Units that exist in the database but that have been
   * deleted from the daemon.
   * 
   * @return a Collection<Map<String, Object>> with the Archival unit data.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<Map<String, Object>> getDbArchivalUnitsDeletedFromDaemon()
      throws DbException {
    final String DEBUG_HEADER = "getDbArchivalUnitsDeletedFromDaemon(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");
    Collection<Map<String, Object>> deletedAus =
	new ArrayList<Map<String, Object>>();

    Collection<Map<String, Object>> aus = mdManagerSql.getDbArchivalUnits();

    for (Map<String, Object> auProperties : aus) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "auProperties = " + auProperties);

      String pluginId = (String)auProperties.get(PLUGIN_ID_COLUMN);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pluginId = " + pluginId);

      String auKey = (String)auProperties.get(AU_KEY_COLUMN);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "auKey = " + auKey);

      String auId = PluginManager.generateAuId(pluginId, auKey);

      // Check whether the daemon does not have this AU and this AU is not
      // restarting due to a plugin update and it has not been manually
      // deactivated.
      if (pluginMgr.isNotConfiguredAndNotInactive(auId)) {
	// Yes: It is an AU that can be safely deleted.
	deletedAus.add(auProperties);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Deleted");
      } else {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Not Deleted");
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "deletedAus = " + deletedAus);
    return deletedAus;
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
  public boolean deleteDbAu(Long auSeq, String auKey) throws DbException {
    return mdManagerSql.removeAu(auSeq, auKey);
  }

  /**
   * Provides the identifier of a publisher if existing or after creating it
   * otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherName
   *          A String with the publisher name.
   * @return a Long with the identifier of the publisher.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreatePublisher(Connection conn, String publisherName)
      throws DbException {
    return dbManager.findOrCreatePublisher(conn, publisherName);
  }

  /**
   * Provides the identifier of a publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherName
   *          A String with the publisher name.
   * @return a Long with the identifier of the publisher.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findPublisher(Connection conn, String publisherName)
      throws DbException {
    return dbManager.findPublisher(conn, publisherName);
  }

  /**
   * Provides the identifier of a provider if existing or after creating it
   * otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param providerLid
   *          A String with the provider LOCKSS identifier.
   * @param providerName
   *          A String with the provider name.
   * @return a Long with the identifier of the provider.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findOrCreateProvider(Connection conn, String providerLid,
      String providerName) throws DbException {
    return dbManager.findOrCreateProvider(conn, providerLid, providerName);
  }

  /**
   * Provides the identifier of a provider.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param providerLid
   *          A String with the provider LOCKSS identifier.
   * @param providerName
   *          A String with the provider name.
   * @return a Long with the identifier of the provider.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findProvider(Connection conn, String providerLid,
      String providerName) throws DbException {
    return dbManager.findProvider(conn, providerLid, providerName);
  }

  /**
   * Adds to the database a bibliographic item.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param mdItemSeq
   *          A Long with the metadata item identifier.
   * @param volume
   *          A String with the bibliographic volume.
   * @param issue
   *          A String with the bibliographic issue.
   * @param startPage
   *          A String with the bibliographic starting page.
   * @param endPage
   *          A String with the bibliographic ending page.
   * @param itemNo
   *          A String with the bibliographic item number.
   * @return an int with the number of database rows inserted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public int addBibItem(Connection conn, Long mdItemSeq, String volume,
      String issue, String startPage, String endPage, String itemNo)
      throws DbException {
    return dbManager.addBibItem(conn, mdItemSeq, volume, issue, startPage,
	endPage, itemNo);
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
  public Long findPlugin(Connection conn, String pluginKey) throws DbException {
    return mdManagerSql.findPlugin(conn, pluginKey);
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
  public Set<Issn> getMdItemIssns(Connection conn, Long mdItemSeq)
      throws DbException {
    return mdManagerSql.getMdItemIssns(conn, mdItemSeq);
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
  public Collection<String> getMdItemProprietaryIds(Connection conn,
      Long mdItemSeq) throws DbException {
    return mdManagerSql.getMdItemProprietaryIds(conn, mdItemSeq);
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
  public Set<Isbn> getMdItemIsbns(Connection conn, Long mdItemSeq)
      throws DbException {
    return mdManagerSql.getMdItemIsbns(conn, mdItemSeq);
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
  public Map<String, String> getMdItemNames(Connection conn, Long mdItemSeq)
      throws DbException {
    return mdManagerSql.getMdItemNames(conn, mdItemSeq);
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
  public KeyPair findPublicationDateInterval(Connection conn, String pluginId,
      String auKey) throws DbException {
    return mdManagerSql.findPublicationDateInterval(conn, pluginId, auKey);
  }

  /**
   * Adds a publisher.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param publisherName
   *          A String with the publisher name.
   * @return a Long with the identifier of the publisher just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addPublisher(Connection conn, String publisherName)
      throws DbException {
    return dbManager.addPublisher(conn, publisherName);
  }

  /**
   * Provides the identifier of a key in a generic metadata key/value pair if
   * existing or after creating it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param keyName
   *          A String with the key name.
   * @return a Long with the identifier of the key.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findOrCreateMdKey(Connection conn, String keyName)
      throws DbException {
    final String DEBUG_HEADER = "findOrCreateMdKey(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "keyName = " + keyName);
    
    if (StringUtil.isNullString(keyName)) {
      throw new DbException("Invalid metadata key '" + keyName + "'");
    }

    Long mdKeySeq = findMdKey(conn, keyName);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "mdKeySeq = " + mdKeySeq);

    // Check whether it is a new key.
    if (mdKeySeq == null) {
      // Yes: Add to the database the new key.
      mdKeySeq = mdManagerSql.addMdKey(conn, keyName);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "new mdKeySeq = " + mdKeySeq);
    }

    return mdKeySeq;
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
  public Long findMdKey(Connection conn, String keyName)
      throws DbException {
    return mdManagerSql.findMdKey(conn, keyName);
  }
}
