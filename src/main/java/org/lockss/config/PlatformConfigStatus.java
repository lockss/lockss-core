/*

Copyright (c) 2000-2021 Board of Trustees of Leland Stanford Jr. University,
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
import java.io.*;

import org.lockss.app.*;
import org.lockss.daemon.status.*;
import org.lockss.servlet.*;
import org.lockss.util.*;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.TimeBase;

import static org.lockss.config.ConfigManager.*;

/** Display Platform Configuration */
public class PlatformConfigStatus extends BaseLockssDaemonManager {
  static Logger log = Logger.getLogger();
  final static String PLATFORM_STATUS_TABLE = "PlatformStatus";
  final static String OS_RELEASE_TABLE = "OsRelease";

  public PlatformConfigStatus() {
  }

  public void startService() {
    super.startService();
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.registerStatusAccessor(PLATFORM_STATUS_TABLE,
				      new PCStatus(getDaemon()));
    // Register OS release info table iff its file exists
    PropFileStatusAccessor osrs = new OsReleaseStatus();
    if (osrs.canRead()) {
      statusServ.registerStatusAccessor(OS_RELEASE_TABLE, osrs);
    }
  }

  public void stopService() {
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.unregisterStatusAccessor(PLATFORM_STATUS_TABLE);
  }

  static class PCStatus implements StatusAccessor {

    private final ColumnDescriptor COL_DESC_URL =
      new ColumnDescriptor("URL", "Config URL",
                           ColumnDescriptor.TYPE_STRING);
    private final ColumnDescriptor COL_DESC_LAST =
      new ColumnDescriptor("LastModified", "Last Modified",
                           ColumnDescriptor.TYPE_DATE);
    private final ColumnDescriptor COL_DESC_LOADED =
      new ColumnDescriptor("ActualUrl", "Loaded From",
                           ColumnDescriptor.TYPE_STRING);

    private static final List sortRules =
      ListUtil.list(new StatusTable.SortRule("IX", true));

    private LockssDaemon daemon;

    PCStatus(LockssDaemon daemon) {
      this.daemon = daemon;
    }

    public String getDisplayName() {
      return "Platform Configuration";
    }

    public boolean requiresKey() {
      return false;
    }

    public void populateTable(StatusTable table) {
      ConfigManager mgr = ConfigManager.getConfigManager();
      Configuration config = ConfigManager.getCurrentConfig();
      table.setDefaultSortRules(sortRules);
      if (mgr.getSpecUrlList().equals(mgr.getLoadedUrlList())) {
        table.setColumnDescriptors(ListUtil.list(COL_DESC_URL, COL_DESC_LAST));
      } else {
        table.setColumnDescriptors(ListUtil.list(COL_DESC_URL, COL_DESC_LAST,
                                                 COL_DESC_LOADED));
      }
      table.setRows(getRows(table.getOptions()));
      table.setSummaryInfo(getSummaryInfo(config));
    }

    public List getRows(BitSet options) {
      ConfigManager mgr = ConfigManager.getConfigManager();
      List rows = new ArrayList();
      List<String> specUrls = mgr.getSpecUrlList();
      List<String> loadedUrls = mgr.getLoadedUrlList();
      if (specUrls != null) {
        for (int ix = 0; ix < specUrls.size(); ix++) {
          String url = specUrls.get(ix);
          String lUrl = ix < loadedUrls.size() ? loadedUrls.get(ix) : null;
	  // This links to ListObjects to display the text of the file, but
	  // doesn't quite work yet.
// 	  Object val =
// 	    new StatusTable.SrvLink(url,
// 				    AdminServletManager.SERVLET_LIST_OBJECTS,
// 				    PropUtil.fromArgs("type", "configfile",
// 						      "url", url));
	  Object val =
	    new StatusTable.Reference(url,
				      ConfigStatus.CONFIG_FILE_STATUS_TABLE,
				      "cf:" + url);

	  Map row = MapUtil.map("URL", val, "IX", ix);
          if (!url.equals(lUrl)) {
            row.put("ActualUrl", lUrl);
          }
	  ConfigFile cf = mgr.getConfigCache().get(url);
	  if (cf != null) {
	    // Compensate for FileConfigFile's numeric Last-Modified
	    // headers
	    String last = DateTimeUtil.gmtDateOf(cf.getLastModified());
	    row.put("LastModified", last);
	    if (cf.isLoadedFromFailover()) {
              row.put("ActualUrl", "Failover");
	    }
	  }
	  rows.add(row);
	}
      }
      return rows;
    }

    void addSum(List lst, String head, String val) {
      if (val != null) {
	lst.add(new StatusTable.SummaryInfo(head,
					    ColumnDescriptor.TYPE_STRING,
					    val));
      }
    }

    String seplist(Collection c) {
      return StringUtil.separatedString(c, ", ");
    }

    private List getSummaryInfo(Configuration config) {
      List res = new ArrayList();
      addSum(res, "Hostname", config.get(PARAM_PLATFORM_FQDN));
      addSum(res, "IP Address", config.get(PARAM_PLATFORM_IP_ADDRESS));
      if (daemon.isClockss()) {
	addSum(res, "IP Address",
	       config.get(PARAM_PLATFORM_SECOND_IP_ADDRESS));
      }
      List<String> groups = config.getPlatformGroupList();
      if (groups != null) {
	if (groups.size() == 1) {
	  addSum(res, "Group", groups.get(0));
	} else {
	  addSum(res, "Groups", seplist(groups));
	}
      }
      addSum(res, "Project", config.get(PARAM_PLATFORM_PROJECT));
      addSum(res, "V3 Identity", config.get(PARAM_PLATFORM_LOCAL_V3_IDENTITY));
      String smtpHost = config.get(PARAM_PLATFORM_SMTP_HOST);
      if (smtpHost != null) {
	int smtpPort =
	  config.getInt(PARAM_PLATFORM_SMTP_PORT,
			org.lockss.mail.SmtpMailService.DEFAULT_SMTPPORT);
	addSum(res, "Mail Relay", smtpHost + ":" + smtpPort);
      }
      addSum(res, "Admin Email", config.get(PARAM_PLATFORM_ADMIN_EMAIL));
      addSum(res, "Local Storage",
	     seplist(config.getList(PARAM_PLATFORM_DISK_SPACE_LIST)));

      res.add(new StatusTable.SummaryInfo("Current Time", 
					  ColumnDescriptor.TYPE_DATE, TimeBase.nowMs()));
      res.add(new StatusTable.SummaryInfo("Uptime", 
					  ColumnDescriptor.TYPE_TIME_INTERVAL, 
					  TimeBase.msSince(daemon.getStartDate().getTime())));
      addSum(res, "Daemon Version", 
	     ConfigManager.getDaemonVersion().displayString());

      addSum(res, "Java Version",
	     System.getProperty("java.specification.version"));
      addSum(res, "Java Runtime", daemon.getJavaVersionInfo());

      // The configuration may not be set in development environments
      PlatformVersion version = Configuration.getPlatformVersion();
      if (version != null) {
	addSum(res, "Platform", version.displayString());
      }
      
      addSum(res, "Cwd",
	     PlatformUtil.getInstance().getCwd());
      ConfigManager mgr = ConfigManager.getConfigManager();
      String restUrl = mgr.getRestConfigServiceUrl();
      if (restUrl != null) {
	addSum(res, "Config Service", UrlUtil.obfuscatePassword(restUrl));
      }
//       List propsUrls = mgr.getSpecUrlList();
//       List loadedUrls = mgr.getLoadedUrlList();
//       if (propsUrls != null) {
// 	addSum(res, "Props", StringUtil.separatedString(propsUrls, ", "));
// 	if (!propsUrls.equals(loadedUrls)) {
// 	  addSum(res, "Loaded from local failover",
// 		 StringUtil.separatedString(loadedUrls, ", "));
// 	}
//       }
      return res;
    }
    
  }

  static class OsReleaseStatus extends PropFileStatusAccessor {

    OsReleaseStatus() {
      super(new File("/etc/os-release"));
      setStripQuotes(true);
    }

    public String getDisplayName() {
      return "OS Release Info";
    }
  }
}
