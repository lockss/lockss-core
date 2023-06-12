/*

Copyright (c) 2023 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated handlecumentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to handle so, subject to the following conditions:

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

import org.lockss.log.L4JLogger;

public interface CrawlEventHandler {
  /**
   * Called when a crawl has started.
   *
   * @param event The description of the crawl event.
   */
  void newContentStarted(CrawlEvent event);

  /**
   * Called when a crawl has completed.
   *
   * @param event The description of the crawl event.
   */
  void newContentCompleted(CrawlEvent event);

  /**
   * Called when a repair crawl has started.
   *
   * @param event The description of the crawl event.
   */
  void repairStarted(CrawlEvent event);

  /**
   * Called when a repair crawl has completed.
   *
   * @param event The description of the crawl event.
   */
  void repairCompleted(CrawlEvent event);


  class Base implements CrawlEventHandler {
    private static final L4JLogger log =
      L4JLogger.getLogger("CrawlEventHandler.Base");

    @Override
    public void newContentStarted(CrawlEvent event) {
      log.debug("Received crawl started event.");
      handleNewContentStarted(event);
    }

    @Override
    public void newContentCompleted(CrawlEvent event) {
      log.debug("Received crawl completed event.");
      handleNewContentCompleted(event);
    }

    @Override
    public void repairStarted(CrawlEvent event) {
      log.debug("Received repair crawl started event.");
      handleRepairStarted(event);
    }

    @Override
    public void repairCompleted(CrawlEvent event) {
      log.debug("Received repair completed event.");
      handleRepairCompleted(event);
    }
    // --------------------------------------------------------------------------
    //  Null Crawl Event handlers. Override Base class and implement to actually
    //  handle any supported crawl events.
    // --------------------------------------------------------------------------

    /**
     * handler for a crawl has started.
     *
     * @param event The description of the crawl event.
     */
    protected void handleNewContentStarted(CrawlEvent event) {}
    /**
     * handler for a crawl which has completed.
     *
     * @param event The description of the crawl event.
     */
    protected void handleNewContentCompleted(CrawlEvent event) {}
    /**
     * handler for a repair crawl which has started.
     *
     * @param event The description of the crawl event.
     */
    protected void handleRepairStarted(CrawlEvent event) {}
    /**
     * handler for a repair crawl which has completed.
     *
     * @param event The description of the crawl event.
     */
    protected void handleRepairCompleted(CrawlEvent event) {}
  }
}
