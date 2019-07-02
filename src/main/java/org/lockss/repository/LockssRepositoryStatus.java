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

package org.lockss.repository;

import java.io.*;
import java.util.*;
import org.apache.commons.collections4.*;

import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.daemon.status.*;
import org.lockss.state.ArchivalUnitStatus;
import org.lockss.util.*;
import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;

/**
 * Status tables for V2 LockssRepository: collections, auids, artifacts
 */
public class LockssRepositoryStatus {
  private static Logger log = Logger.getLogger("RepositoryStatus");

  public static final String PREFIX = Configuration.PREFIX + "repoStatus.";

  /**
   * The default maximum number of artifacts to display in a single page.
   */
  public static final String PARAM_MAX_ARTIFACTS_TO_DISPLAY =
      PREFIX + "artifactsPerPage";
  static final int DEFAULT_MAX_ARTIFACTS_TO_DISPLAY = 200;


  public static final String SERVICE_STATUS_TABLE_NAME = "RepositoryTable";
  public static final String AUIDS_STATUS_TABLE_NAME = "CollectionTable";
  public static final String ARTIFACTS_STATUS_TABLE_NAME = "ArtifactsTable";
//   public static final String SPACE_TABLE_NAME = "RepositorySpace";

  public static final String AU_STATUS_TABLE_NAME =
    ArchivalUnitStatus.AU_STATUS_TABLE_NAME;

  // Base class
  static abstract class AbstractRepoStatusAccessor implements StatusAccessor {
    protected LockssDaemon daemon;
    protected RepositoryManager repoMgr;

    AbstractRepoStatusAccessor(LockssDaemon daemon) {
      this.daemon = daemon;
      repoMgr = daemon.getRepositoryManager();
    }
  }

  static class RepoCollsStatusAccessor extends AbstractRepoStatusAccessor {

    private static final List columnDescriptors = ListUtil.list
      (
       new ColumnDescriptor("type", "Type", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("path", "URL / Path", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("coll", "Collection", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("aus", "AUs", ColumnDescriptor.TYPE_INT)
       );

    private static final List sortRules =
      ListUtil.list(new StatusTable.SortRule("type", true),
		    new StatusTable.SortRule("path", true),
		    new StatusTable.SortRule("coll", true));

    RepoCollsStatusAccessor(LockssDaemon daemon) {
      super(daemon);
    }

    public String getDisplayName() {
      return "Repositories and Collections";
    }

    public void populateTable(StatusTable table)
        throws StatusService.NoSuchTableException {
      table.setColumnDescriptors(columnDescriptors);
      table.setDefaultSortRules(sortRules);
      table.setRows(getRows());
    }

    public boolean requiresKey() {
      return false;
    }

    private List getRows() {
      List rows = new ArrayList();
      for (RepoSpec rs : repoMgr.getV2RepositoryList()) {
	LockssRepository repo = rs.getRepository();
	try {
	  for (String coll : repo.getCollectionIds()) {
	    Map row = new HashMap();
	    row.put("type", rs.getType());
	    if (!StringUtil.isNullString(rs.getPath())) {
	      row.put("path", rs.getPath());
	    }
	    row.put("coll",
		    new StatusTable.Reference(rs.getCollection(),
					      AUIDS_STATUS_TABLE_NAME,
					      rs.getSpec()));
	    try {
	      row.put("aus",
		      new StatusTable.Reference(IterableUtils.size(repo.getAuIds(coll)),
						AUIDS_STATUS_TABLE_NAME,
						rs.getSpec()));
	    } catch (IOException e) {
	      log.warning("Couldn't get AU count", e);
	    }
	    rows.add(row);
	  }
	} catch (IOException e) {
	  log.warning("Couldn't get collection IDs from: " + rs.getSpec(),
		      e);
	}
      }
      return rows;
    }

    protected String getTitle(String key) {
      return "Repository Collections";
    }
  }


  static class CollectionAuidsStatusAccessor
    extends AbstractRepoStatusAccessor {

    private static final List columnDescriptors = ListUtil.list
      (new ColumnDescriptor("auid", "AUID", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("size", "Size", ColumnDescriptor.TYPE_INT)
       );

    private static final List sortRules =
      ListUtil.list(new StatusTable.SortRule("auid", true));

    CollectionAuidsStatusAccessor(LockssDaemon daemon) {
      super(daemon);
    }

    public String getDisplayName() {
      return "AUIDs in Collection";
    }

    public void populateTable(StatusTable table)
        throws StatusService.NoSuchTableException {
      String key = table.getKey();
      RepoSpec rs = repoMgr.getV2Repository(key);
      table.setColumnDescriptors(columnDescriptors);
      table.setDefaultSortRules(sortRules);
      table.setRows(getRows(table, rs));
      table.setSummaryInfo(getSummaryInfo(rs));
    }

    public boolean requiresKey() {
      return true;
    }

    private List getRows(StatusTable table, RepoSpec rs) {
      LockssRepository repo = rs.getRepository();
      List rows = new ArrayList();
      try {
	for (String auid : repo.getAuIds(rs.getCollection())) {
	  Map row = new HashMap();
	  StatusTable.Reference auidRef =
	    new StatusTable.Reference(auid,
				      ARTIFACTS_STATUS_TABLE_NAME,
				      rs.getSpec())
	    .setProperty("auid", auid);
	  row.put("auid", auidRef);
	  try {
	    row.put("size", repo.auSize(rs.getCollection(), auid));
	  } catch (IOException e) {
	    log.warning("Couldn't get AU size", e);
	  }
	  rows.add(row);
	}
      } catch (IOException e) {
	log.warning("Couldn't get AU list for collection: " + rs.getSpec(), e);
      }	
      return rows;
    }

    private List getSummaryInfo(RepoSpec rs) {
      List res = new ArrayList();
      res.add(new StatusTable.SummaryInfo("Collection",
					  ColumnDescriptor.TYPE_STRING,
					  rs.getSpec()));
      return res;
    }
  }

  static class AuidArtifactsStatusAccessor extends AbstractRepoStatusAccessor {
    private static final List columnDescriptors = ListUtil.list
      (new ColumnDescriptor("url", "URL", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("version", "Version", ColumnDescriptor.TYPE_INT),
       new ColumnDescriptor("size", "Size", ColumnDescriptor.TYPE_INT),
       new ColumnDescriptor("collected", "Collected at",
			    ColumnDescriptor.TYPE_DATE)
       );

    private static final List sortRules =
        ListUtil.list(new StatusTable.SortRule("sort", true));

    AuidArtifactsStatusAccessor(LockssDaemon daemon) {
      super(daemon);
    }

    public String getDisplayName() {
      return "Artifacts in AU";
    }

 public void populateTable(StatusTable table)
        throws StatusService.NoSuchTableException {
      String key = table.getKey();
      RepoSpec rs = repoMgr.getV2Repository(key);
      String auid = table.getProperty("auid");
      table.setTitle(getDisplayName());
      table.setColumnDescriptors(columnDescriptors);
      table.setDefaultSortRules(sortRules);
      table.setRows(getRows(table, rs, auid));
      table.setSummaryInfo(getSummaryInfo(rs, auid));
    }

    public boolean requiresKey() {
      return true;
    }

    private List getRows(StatusTable table, RepoSpec rs, String auid) {
      LockssRepository repo = rs.getRepository();
      int defaultNumRows =
	CurrentConfig.getIntParam(PARAM_MAX_ARTIFACTS_TO_DISPLAY,
				  DEFAULT_MAX_ARTIFACTS_TO_DISPLAY);
      int startRow = table.getStartRow();
      int numRows = table.getNumRows(defaultNumRows);

      // Adds the additional "auid" prop to the Next & Prev References
      java.util.function.Consumer refModifier =
	new java.util.function.Consumer<StatusTable.Reference>() {
	  public void accept(StatusTable.Reference ref) {
	    ref.setProperty("auid", auid);
	  }};

      List rows = new ArrayList();
      table.addPrevRowsLink(rows, "url", startRow, numRows, refModifier);

      Iterator<Artifact> artIter;
      try {
	artIter = repo.getArtifacts(rs.getCollection(), auid).iterator();
      } catch (IOException e) {
	throw new RuntimeException("Error getting Artifact Iterator", e);
      }
      int endRow1 = startRow + numRows; // end row + 1
      for (int curRow = 0; (curRow < endRow1) && artIter.hasNext(); curRow++) {
        Artifact art = artIter.next();
        if (curRow < startRow) {
          continue;
        }
	Map row = makeRow(rs, art);
	row.put("sort", new Integer(curRow));
	rows.add(row);
      }
      if (artIter.hasNext()) {
        // add 'next'
        rows.add(table.makeOtherRowsLink("url", true, endRow1, numRows,
					 refModifier));
      }
      return rows;
    }

    Map makeRow(RepoSpec rs, Artifact art) {
      Map row = new HashMap();
      row.put("url", art.getUri());
      row.put("size", art.getContentLength());
      row.put("version", art.getVersion());
      row.put("collected", art.getCollectionDate());
      return row;
    }

    private List getSummaryInfo(RepoSpec rs, String auid) {
      List res = new ArrayList();
      res.add(new StatusTable.SummaryInfo("AUID",
					  ColumnDescriptor.TYPE_STRING,
					  auid));
      res.add(new StatusTable.SummaryInfo("Collection",
					  ColumnDescriptor.TYPE_STRING,
					  rs.getSpec()));
      return res;
    }
  }

}
