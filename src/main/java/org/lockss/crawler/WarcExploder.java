/*
 * $Id$
 */

/*

Copyright (c) 2007-2015 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.crawler;

import java.util.*;
import java.io.*;

import org.archive.io.*;
import org.archive.io.warc.*;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;
import org.lockss.util.urlconn.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.daemon.Crawler.CrawlerFacade;
import org.lockss.plugin.*;
import org.lockss.plugin.base.*;
import org.lockss.plugin.exploded.*;
import org.lockss.state.*;

/**
 * An Exploder that ingests Internet Archive WARC files, and behaves
 * as if it had ingested each file in the WARC file directly from its
 * original source.
 *
 * @author  David S. H. Rosenthal
 * @author  Felix Ostrowski
 * @version 0.0
 */

public class WarcExploder extends Exploder {

  private static Logger logger = Logger.getLogger();
  protected InputStream arcStream;
  protected CIProperties arcProps;

  /**
   * Constructor
   * @param toExplode  url data of for the archive to explode
   * @param crawlFacade  facade for crawler performing crawl
   * @param helper helper for exploding archive
   */
  public WarcExploder(FetchedUrlData toExplode, CrawlerFacade crawlFacade,
      ExploderHelper helper) {
    super(toExplode, crawlFacade, helper);
    arcStream = toExplode.input;
    arcProps = toExplode.headers;
  }

  /**
   * Explode the archive into its constituent elements
   */
  public void explode() throws CacheException {
    CachedUrl cachedUrl = null;
    int goodEntries = 0;
    int badEntries = 0;
    int ignoredEntries = 0;
    int entriesBetweenSleep = 0;
    ArchiveReader arcReader = null;

    logger.info((storeArchive ? "Storing" : "Fetching") + " WARC file: " +
        origUrl + " will explode");
     try {
      if (storeArchive) {
        UrlCacher uc = au.makeUrlCacher(
            new UrlData(arcStream, arcProps, fetchUrl));
        BitSet bs = new BitSet();
        bs.set(UrlCacher.DONT_CLOSE_INPUT_STREAM_FLAG);
        uc.setFetchFlags(bs);
        uc.storeContent();
        archiveData.resetInputStream();
        arcStream = archiveData.input;
      }
      // Wrap it in an ArchiveReader
      logger.debug3("About to wrap stream");
      arcReader = wrapStream(fetchUrl, arcStream);
      logger.debug3("wrapStream() returns " +
          (arcReader == null ? "null" : "non-null"));
      // Explode it
      if (arcReader == null) {
        throw new CacheException.ExploderException("no WarcReader for " +
            origUrl);
      }
      ArchivalUnit au = crawlFacade.getAu();
      Set stemSet = new HashSet();
      logger.debug("Exploding " + fetchUrl);
      // Iterate through the elements in the WARC file, except the first
      Iterator i = arcReader.iterator();
      // Skip first record
      for (i.next(); i.hasNext(); ) {
        // XXX probably not necessary
        helper.pokeWDog();
        if ((++entriesBetweenSleep % sleepAfter) == 0) {
          long pauseTime =
            CurrentConfig.getTimeIntervalParam(PARAM_RETRY_PAUSE,
                DEFAULT_RETRY_PAUSE);
          Deadline pause = Deadline.in(pauseTime);
          logger.debug3("Sleeping for " +
              TimeUtil.timeIntervalToString(pauseTime));
          while (!pause.expired()) {
            try {
              pause.sleep();
            } catch (InterruptedException ie) {
              // no action
            }
          }
        }
        ArchiveRecord element = (ArchiveRecord)i.next();
        // Each element is a URL to be cached in a suitable AU
        ArchiveRecordHeader elementHeader = element.getHeader();
        String elementUrl = elementHeader.getUrl();
        String elementMimeType = elementHeader.getMimetype();
        long elementLength = elementHeader.getLength();
        logger.debug2("WARC url " + elementUrl + " mime " + elementMimeType);
        if (elementUrl.startsWith("http:")) {
          ArchiveEntry ae =
            new ArchiveEntry(elementUrl,
                elementLength,
                0, // XXX need to convert getDate string to long
                element, // ArchiveRecord extends InputStream
                this,
                fetchUrl);
          ae.setHeaderFields(makeCIProperties(elementHeader));
          long bytesStored = elementLength;
          logger.debug3("ArchiveEntry: " + ae.getName()
              + " bytes "  + bytesStored);
          try {
            helper.process(ae);
          } catch (PluginException ex) {
            throw new CacheException.ExploderException("helper.process() threw",
						       ex);
          }
          if (ae.getBaseUrl() != null) {
            if (ae.getRestOfUrl() != null &&
                ae.getHeaderFields() != null) {
              storeEntry(ae);
              handleAddText(ae);
              goodEntries++;
              crawlFacade.getCrawlerStatus().addContentBytesFetched(bytesStored);
            } else {
              ignoredEntries++;
            }
          } else {
            badEntries++;
            logger.debug2("Can't map " + elementUrl + " from "
        	+ getArchiveUrl());
          }
        }
      }
    } catch (IOException ex) {
      throw new CacheException.ExploderException(ex);
    } finally {
      if (arcReader != null) try {
        arcReader.close();
        arcReader = null;
      } catch (IOException ex) {
        throw new CacheException.ExploderException(ex);
      }
      if (cachedUrl != null) {
        cachedUrl.release();
      }
      IOUtil.safeClose(arcStream);
    }
    if (badEntries == 0 && goodEntries > 0) {
        // Make it look like a new crawl finished on each AU to which
        // URLs were added.
        for (Iterator it = touchedAus.iterator(); it.hasNext(); ) {
          ArchivalUnit au = (ArchivalUnit)it.next();
          logger.debug3(getArchiveUrl() + " touching " + au.toString());
          AuUtil.getAuState(au).newCrawlFinished(Crawler.STATUS_SUCCESSFUL, null);
        }
    } else {
      ArchivalUnit au = crawlFacade.getAu();
      String msg = getArchiveUrl() + ": " + badEntries + "/" +
        goodEntries + " bad entries";
      throw new CacheException.UnretryableException(msg);
    }
  }

  protected CIProperties makeCIProperties(ArchiveRecordHeader elementHeader)
    throws IOException {
      CIProperties ret = new CIProperties();
      Set elementHeaderFieldKeys = elementHeader.getHeaderFieldKeys();
      for (Iterator i = elementHeaderFieldKeys.iterator(); i.hasNext(); ) {
        String key = (String) i.next();
        try {

          Object valueObject = elementHeader.getHeaderValue(key);

          if (valueObject == null) {
            logger.warning("Ignoring null value for key '" + key + "'.");
          } else {
            String value = valueObject.toString();
            logger.debug3(key + ": " + value);
            ret.put(key, value);
          }

        } catch (ClassCastException ex) {
          logger.error("makeCIProperties: " + key + " threw ", ex);
          throw new CacheException.ExploderException(ex);
        }
      }
      return (ret);
    }

  protected ArchiveReader wrapStream(String url, InputStream arcStream) throws IOException {
    ArchiveReader ret = null;
    logger.debug3("Getting an ArchiveReader");
    ret = ArchiveReaderFactory.get(url, arcStream, true);
    return (ret);
  }
}
