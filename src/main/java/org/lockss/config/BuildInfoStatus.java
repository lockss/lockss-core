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

package org.lockss.config;

import java.util.*;

import org.lockss.app.*;
import org.lockss.daemon.status.*;
import org.lockss.util.*;
import static org.lockss.util.BuildInfo.*;
import static org.lockss.config.ConfigManager.*;

/** Display build info for all components */
public class BuildInfoStatus extends BaseLockssManager {
  static Logger log = Logger.getLogger();
  public final static String BUILD_INFO_TABLE = "BuildInfo";

  public BuildInfoStatus() {
  }

  public void startService() {
    super.startService();
    StatusService statusServ = getApp().getStatusService();
    statusServ.registerStatusAccessor(BUILD_INFO_TABLE, new BIStatus());
  }

  public void stopService() {
    StatusService statusServ = getApp().getStatusService();
    statusServ.unregisterStatusAccessor(BUILD_INFO_TABLE);
  }

  static class BIStatus implements StatusAccessor {

    private final List colDescs =
      ListUtil.list(new ColumnDescriptor(BUILD_NAME, "Name",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor(BUILD_VERSION, "Version",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor(BUILD_PARENT_VERSION, "Parent",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor(BUILD_TIMESTAMP, "Build Time",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor(BUILD_HOST, "Build Host",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor(BUILD_GIT_BRANCH, "Git Branch",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor(BUILD_GIT_COMMIT, "Git Commit",
					 ColumnDescriptor.TYPE_STRING)
		    );

    public String getDisplayName() {
      return "Build Info";
    }

    public boolean requiresKey() {
      return false;
    }

    public void populateTable(StatusTable table) {
      Configuration config = ConfigManager.getCurrentConfig();
      table.setDefaultSortRules(Collections.EMPTY_LIST);
      table.setColumnDescriptors(colDescs);
      table.setRows(getRows(table.getOptions()));
    }

    public List getRows(BitSet options) {
      List rows = new ArrayList();
      for (BuildInfo bi : BuildInfo.getAllBuildInfo()) {
	Map row = new HashMap(bi.getBuildPropertiesInst());
	rows.add(row);
	if (Boolean.valueOf((String)row.get(BUILD_GIT_DIRTY))) {
	  String commit = (String)row.get(BUILD_GIT_COMMIT);
	  if (commit != null) {
	    row.put(BUILD_GIT_COMMIT, commit + " (D)");
	  }
	}
      }
      return rows;
    }
  }
}
