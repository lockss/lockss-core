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

import java.io.*;
import java.util.*;
import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.daemon.status.*;
import org.lockss.daemon.status.StatusService.NoSuchTableException;
import org.lockss.util.*;
import org.lockss.util.time.TimeUtil;
import org.lockss.util.rest.status.ApiStatus;
import org.lockss.config.*;
import org.lockss.plugin.*;

/** Config status table */
public class RestServiceStatus {
  static Logger log = Logger.getLogger();

  final static String SERVICE_STATUS_TABLE = "ServiceStatus";

  public static final String PREFIX = Configuration.PREFIX + "serviceStatus.";


  public RestServiceStatus() {
  }

  public static void registerAccessors(LockssApp app) {
    StatusService statusServ = app.getStatusService();
    statusServ.registerStatusAccessor(SERVICE_STATUS_TABLE,
				      new ServiceStatus(app));
  }

  public static void unregisterAccessors(LockssApp app) {
    StatusService statusServ = app.getStatusService();
    statusServ.unregisterStatusAccessor(SERVICE_STATUS_TABLE);
  }

  static class ServiceStatus implements StatusAccessor.DebugOnly {
    private LockssApp app;
    RestServicesManager svcsMgr;

    ServiceStatus(LockssApp app) {
      this.app = app;
      svcsMgr = app.getManagerByType(RestServicesManager.class);
    }

    protected final List colDescs =
      ListUtil.list(new ColumnDescriptor("name", "Service",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("status", "Status",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("reason", "Reason",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("up", "Up",
					 ColumnDescriptor.TYPE_DATE),
		    new ColumnDescriptor("version", "Version",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("release", "Release",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("resturl", "REST Url",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("adminurl", "Admin Url",
					 ColumnDescriptor.TYPE_STRING)
// 		    new ColumnDescriptor("lasttrans", "Noticed at",
// 					 ColumnDescriptor.TYPE_DATE),
		    );

    public String getDisplayName() {
      return "Service Status";
    }

    public boolean requiresKey() {
      return false;
    }

    public void populateTable(StatusTable table) {
      table.setColumnDescriptors(colDescs);
      table.setSummaryInfo(getSummaryInfo());
      table.setRows(getRows(table.getOptions(),
			    ConfigManager.getCurrentConfig()));
    }

    protected List getRows(BitSet options, Configuration config) {
      List rows = new ArrayList();
      for (ServiceDescr descr : app.getAllServiceDescrs()) {
	ServiceBinding binding = app.getServiceBinding(descr);
	RestServicesManager.ServiceStatus stat =
	  svcsMgr.getServiceStatus(binding);
	Map row = new HashMap();
	row.put("name", descr.getName());

	if (binding.hasRestPort()) {
	  row.put("resturl", binding.getRestStem());
	}
	if (binding.hasUiPort()) {
	  row.put("adminurl", binding.getUiStem("http"));
	}
	if (stat != null) {
	  row.put("status", stat.isReady() ? "Ready" : "Not Ready");
	  row.put("reason", stat.getReason());
          ApiStatus apiStat = stat.getApiStatus();
          if (apiStat != null) {
            row.put("version", apiStat.getComponentVersion());
            row.put("release", apiStat.getLockssVersion());
          }
	  if (stat.getLastTransition() > 0) {
	    row.put("lasttrans", stat.getLastTransition());
	  }
	  if (stat.getReadyTime() > 0) {
	    row.put("up",
                    TimeUtil.timeIntervalToString(TimeBase.msSince(stat.getReadyTime())));
	  }
	}
	rows.add(row);
      }
      return rows;
    }
    
    protected List getSummaryInfo() {
      List res = new ArrayList();
      return res;
    }

    void addSum(List lst, String head, String val) {
      if (val != null) {
	lst.add(new StatusTable.SummaryInfo(head,
					    ColumnDescriptor.TYPE_STRING,
					    val));
      }
    }
  }

}
