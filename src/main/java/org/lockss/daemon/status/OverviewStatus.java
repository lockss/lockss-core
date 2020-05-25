/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.daemon.status;

import java.util.*;

import org.lockss.app.*;
import org.lockss.daemon.status.*;
import org.lockss.daemon.status.StatusTable.ForeignOverview;
import org.lockss.util.*;
import org.lockss.state.*;
import org.lockss.poller.*;
import org.lockss.poller.v3.*;
import org.lockss.crawler.*;
import org.lockss.repository.*;

/** Display overview of each table that has registered an
 * OverviewAccessor */
public class OverviewStatus extends BaseLockssDaemonManager {
  static Logger log = Logger.getLogger();
  final static String OVERVIEW_STATUS_TABLE = "OverviewStatus";

  public OverviewStatus() {
  }

  public void startService() {
    super.startService();
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.registerStatusAccessor(OVERVIEW_STATUS_TABLE,
				      new SummAcc(getDaemon()));
  }

  public void stopService() {
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.unregisterStatusAccessor(OVERVIEW_STATUS_TABLE);
  }

  static class SummAcc implements StatusAccessor {

    private LockssDaemon daemon;

    SummAcc(LockssDaemon daemon) {
      this.daemon = daemon;
    }

    public String getDisplayName() {
      return "Overview";
    }

    public boolean requiresKey() {
      return false;
    }

    public void populateTable(StatusTable table) {
      StatusService statusServ = daemon.getStatusService();
      table.setSummaryInfo(getSummaryInfo(statusServ, table));
    }


    static final String[] overviewTableNames = {
      org.lockss.state.ArchivalUnitStatus.SERVICE_STATUS_TABLE_NAME,
      org.lockss.repository.LockssRepositoryStatus.SERVICE_STATUS_TABLE_NAME,
      org.lockss.crawler.CrawlManagerImpl.CRAWL_STATUS_TABLE_NAME,
      org.lockss.metadata.MetadataManager.METADATA_STATUS_TABLE_NAME,
      V3PollStatus.POLLER_STATUS_TABLE_NAME,
      V3PollStatus.VOTER_STATUS_TABLE_NAME,
      org.lockss.hasher.HashSvcSchedImpl.HASH_STATUS_TABLE,
    };

    private List getSummaryInfo(StatusService statusServ, StatusTable table) {
      // Request overview lines from other components, wait briefly
      statusServ.requestOverviews(table.getOptions());
      List res = new ArrayList();

      // Iterate over locally registered overviews
      for (String overviewTableName : overviewTableNames) {

	// Foreign overview (null if no component has registered a global
	// overview accessor for that table, or if no overview value has
	// been received sufficiently recently)
	ForeignOverview fo =
	  statusServ.getForeignOverview(overviewTableName);
	if (fo != null) {
	  StatusTable.SummaryInfo summ =
	    new StatusTable.SummaryInfo(null,
					ColumnDescriptor.TYPE_STRING,
					fo.getValue());
	  res.add(summ);
	}
	// Local overview (null if no local overview accessor for that table)
	Object v = statusServ.getOverview(overviewTableName,
					 table.getOptions());

	if (v != null) {
	  // May be or contain a Reference to an overview accessor that's
	  // registered globally, but this one should always be local
	  // because it came from our local overview table
	  StatusTable.setLocal(v, true);

	  if (fo == null || !statusServ.isGlobalOnlyTable(overviewTableName)) {
	    if (fo != null) {
	      // If we have both a local and global overview for this table,
	      // label the local one
	      if (v instanceof List) {
		v = ListUtil.append((List)v, ListUtil.list(" (local)"));
	      } else {
		v = ListUtil.list(v, " (local)");
	      }
	    }
	    StatusTable.SummaryInfo summ =
	      new StatusTable.SummaryInfo(null,
					  ColumnDescriptor.TYPE_STRING,
					  v);
	    res.add(summ);
	  }
	}
      }
      return res;
    }
  }
}
