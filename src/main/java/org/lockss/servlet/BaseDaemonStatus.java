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

package org.lockss.servlet;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import javax.servlet.*;

import org.mortbay.html.*;

import org.lockss.config.*;
import org.lockss.daemon.status.*;
import org.lockss.plugin.PluginManager;
import org.lockss.util.*;


/**
 * Base class for DaemonStatus servlets, contains common methods
 */
public abstract class BaseDaemonStatus extends LockssServlet {

  private static final Logger log = Logger.getLogger();

  /** Supported output formats */
  protected static final int OUTPUT_HTML = 1;
  protected static final int OUTPUT_TEXT = 2;
  protected static final int OUTPUT_XML = 3;
  protected static final int OUTPUT_CSV = 4;

  protected static BitSet debugOptions = new BitSet();
  static {
    debugOptions.set(StatusTable.OPTION_DEBUG_USER);
  }

  protected PluginManager pluginMgr;
  protected StatusService statSvc;
  protected String tableName;
  protected String tableKey;

  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    statSvc = getLockssDaemon().getStatusService();
    pluginMgr = getLockssDaemon().getPluginManager();
  }

  protected String htmlEncode(String s) {
    if (s == null) {
      return null;
    }
    return HtmlUtil.htmlEncode(s);
  }

  protected static final Image UPARROW1 =
    ServletUtil.image("uparrow1blue.gif", 16, 16, 0,
		      "Primary sort column, ascending");
  protected static final Image UPARROW2 =
    ServletUtil.image("uparrow2blue.gif", 16, 16, 0,
		      "Secondary sort column, ascending");
  protected static final Image DOWNARROW1 =
    ServletUtil.image("downarrow1blue.gif", 16, 16, 0,
		      "Primary sort column, descending");
  protected static final Image DOWNARROW2 =
    ServletUtil.image("downarrow2blue.gif", 16, 16, 0,
		      "Secondary sort column, descending");


  abstract protected String getDisplayString(Object val, int type);
  abstract protected String getDisplayString1(Object val, int type);

  // turn References into html links
  protected String getRefString(StatusTable.Reference ref, int type) {
    StringBuilder sb = new StringBuilder();
    sb.append("table=");
    sb.append(ref.getTableName());
    String key = ref.getKey();
    if (!StringUtil.isNullString(key)) {
      sb.append("&key=");
      sb.append(urlEncode(key));
    }
    Properties refProps = ref.getProperties();
    if (refProps != null) {
      for (Iterator iter = refProps.entrySet().iterator(); iter.hasNext();) {
	Map.Entry ent = (Map.Entry)iter.next();
	sb.append("&");
	sb.append(ent.getKey());
	sb.append("=");
	sb.append(urlEncode((String)ent.getValue()));
      }
    }
    if (ref.getPeerId() != null) {
      return srvAbsLink(ref.getPeerId(),
			myServletDescr(),
			getDisplayString(ref.getValue(), type),
			sb.toString());
    }
    if (!ref.isLocal()) {
      StatusTable.ForeignTable ft = statSvc.getForeignTable(ref.getTableName());
      if (ft != null) {
	return srvAbsLink(ft.getStem(),
			  myServletDescr(),
			  getDisplayString(ref.getValue(), type),
			  sb.toString());
      }
    }
    String disp = getDisplayString(ref.getValue(), type);
    if (ref.isLabelLocal()) disp += " (local)";
    return srvLink(myServletDescr(),
		   disp,
		   sb.toString());
  }

  // turn UrlLink into html link
  protected String getSrvLinkString(StatusTable.SrvLink link, int type) {
    return srvLink(link.getServletDescr(),
		   getDisplayString1(link.getValue(), type),
		   link.getArgs());
  }

  // Patterm to parse foreign table key: <table>:<svc_abbrev>:<svc_stem>
  Pattern FOREIGN_TABLE_PAT = Pattern.compile("(\\w+):(\\w+):(.*)");

  /** If the table param is a foreign table spec, redirect to that foreign
   * table */
  protected boolean handleForeignRedirect()
      throws IOException {
    if (StringUtil.isNullString(tableName)) {
      tableName = null;
      return false;
    }
    Matcher m1 = FOREIGN_TABLE_PAT.matcher(tableName);
    if (m1.matches()) {
      String table = m1.group(1);
      String stem = m1.group(3);
      String redir =
	srvURL(stem, myServletDescr(), modifyParams("table", table));
      log.debug("Redirecting to: " + redir);
      resp.sendRedirect(redir);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Build a form with a select box that fetches a named table
   * @return the Composite object
   */
  protected Composite getSelectTableForm() {
    try {
      StatusTable statTable =
        statSvc.getTable(StatusService.ALL_TABLES_TABLE, null,
			 isDebugUser() ? debugOptions : null);
      java.util.List colList = statTable.getColumnDescriptors();
      ColumnDescriptor cd = (ColumnDescriptor)colList.get(0);
      Select sel = new Select("table", false);
      sel.attribute("onchange", "this.form.submit()");
      boolean foundIt = false;
      for (Map rowMap : statTable.getSortedRows()) {
        Object val = rowMap.get(cd.getColumnName());
        String display = StatusTable.getActualValue(val).toString();
        if (val instanceof StatusTable.Reference) {
          StatusTable.Reference ref = (StatusTable.Reference)val;
          String key = ref.getTableName();
	  String stem = ref.getServiceStem();
          boolean isThis = false;
	  if (!StringUtil.isNullString(stem)) {
	    key += ":" + ref.getServiceName() + ":" + stem;
// 	    display += " (" + ref.getServiceName() + ")";
	  } else {
	    // select the currently displayed table (always local)
	    isThis = (tableKey == null) && tableName.equals(ref.getTableName());
	    foundIt = foundIt || isThis;
	  }
          sel.add(display, isThis, key);
        } else {
          sel.add(display, false);
        }
      }
      // if not currently displaying a table in the list, select a blank entry
      if (!foundIt) {
        sel.add(" ", true, "");
      }
      Form frm = new Form(srvURL(myServletDescr()));
      // use GET so user can refresh in browser
      frm.method("GET");
      frm.add(sel);
      Input submit = new Input(Input.Submit, "foo", "Go");
      submit.attribute("id", "dsSelectBox");
      frm.add(submit);
      return frm;
    } catch (Exception e) {
      // if this fails for any reason, just don't include this form
      log.warning("Failed to build status table selector", e);
      return new Composite();
    }
  }

}
