/*
 * $Id$
 */

/*

Copyright (c) 2000-2009 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.util.*;
import org.lockss.app.*;
import org.lockss.daemon.status.*;
import org.lockss.daemon.status.StatusService.NoSuchTableException;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.plugin.*;

/** Config status table */
public class ConfigStatus extends BaseLockssDaemonManager {
  static Logger log = Logger.getLogger();

  final static String CONFIG_STATUS_TABLE = "ConfigStatus";
  final static String CONFIG_FILE_STATUS_TABLE = "ConfigFileStatus";

  public static final String PREFIX = Configuration.PREFIX + "configStatus.";

  /** Truncate displayed values to this length */
  static final String PARAM_MAX_DISPLAY_VAL_LEN = PREFIX + "maxDisplayValLen";
  static final int DEFAULT_MAX_DISPLAY_VAL_LEN = 1000;

  ConfigManager configMgr;

  public ConfigStatus() {
  }

  public void startService() {
    super.startService();
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.registerStatusAccessor(CONFIG_STATUS_TABLE, new Status());
    statusServ.registerStatusAccessor(CONFIG_FILE_STATUS_TABLE, new OneFile());
    configMgr = getDaemon().getConfigManager();
  }

  public void stopService() {
    StatusService statusServ = getDaemon().getStatusService();
    statusServ.unregisterStatusAccessor(CONFIG_STATUS_TABLE);
    statusServ.unregisterStatusAccessor(CONFIG_FILE_STATUS_TABLE);
  }

  /** Base class for status tables displaying a Configuration */
  abstract class BaseStatus implements StatusAccessor.DebugOnly {

    protected final List colDescs =
      ListUtil.list(new ColumnDescriptor("name", "Name",
					 ColumnDescriptor.TYPE_STRING),
		    new ColumnDescriptor("value", "Value",
					 ColumnDescriptor.TYPE_STRING)
		    );

    public abstract String getDisplayName();

    public abstract boolean requiresKey();

    public abstract void populateTable(StatusTable table)
	throws NoSuchTableException;

    protected List getRows(BitSet options, Configuration config) {
      List rows = new ArrayList();

      int maxLen = config.getInt(PARAM_MAX_DISPLAY_VAL_LEN,
				 DEFAULT_MAX_DISPLAY_VAL_LEN);
      for (Iterator iter = config.keySet().iterator(); iter.hasNext(); ) {
	String key = (String)iter.next();
	if (ConfigManager.shouldParamBeLogged(key)) {
	  Map row = new HashMap();
	  row.put("name", key);
	  row.put("value",
		  StringUtil.elideMiddleToMaxLen(config.get(key), maxLen));
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
  }

  /** Global Configuration status table */
  class Status extends BaseStatus {

    public String getDisplayName() {
      return "Configuration File";
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

    protected List getSummaryInfo() {
      List res = new ArrayList();
      res.add(new StatusTable.SummaryInfo("Last Reload",
					  ColumnDescriptor.TYPE_DATE,
					  configMgr.getLastUpdateTime()));
      return res;
    }
  }

  /** Individual ConfigFile status table */
  class OneFile extends BaseStatus {

    public String getDisplayName() {
      return "Configuration File:";
    }

    public boolean requiresKey() {
      return true;
    }

    public void populateTable(StatusTable table) throws NoSuchTableException {
      String key = table.getKey();
      if (key != null) {
        if (!key.startsWith("cf:")) {
          throw new StatusService.NoSuchTableException("Unknown selector: "
						       + key);
        }
        String[] foo = org.apache.commons.lang3.StringUtils.split(key, ":", 2);
        if (foo.length < 2 || StringUtil.isNullString(foo[1])) {
          throw new StatusService.NoSuchTableException("Empty config file url: "
						       + key);
        }
        String url = foo[1];
	ConfigFile cf = configMgr.getConfigCache().get(url);
	if (cf != null) {
	  try {
	    Configuration config = cf.getConfiguration();
	    table.setTitle("Config File: " + url);
	    table.setColumnDescriptors(colDescs);
	    table.setSummaryInfo(getSummaryInfo(cf));
	    table.setRows(getRows(table.getOptions(), config));
	  } catch (IOException e) {
	    log.error("Couldn't get config for: " + cf, e);
	    throw new StatusService.NoSuchTableException("Couldn't get config for: " + cf,
							 e);
	  }
	}
      }
    }

    protected List getSummaryInfo(ConfigFile cf) {
      List res = new ArrayList();
      // Compensate for FileConfigFile's numeric Last-Modified headers
      String last = DateTimeUtil.gmtDateOf(cf.getLastModified());
      res.add(new StatusTable.SummaryInfo("Last Modified",
					  ColumnDescriptor.TYPE_DATE,
					  last));
      return res;
    }
  }
}

