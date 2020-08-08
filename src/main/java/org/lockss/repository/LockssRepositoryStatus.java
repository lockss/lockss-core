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
import org.apache.commons.collections4.iterators.*;

import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.daemon.status.*;
import org.lockss.state.ArchivalUnitStatus;
import org.lockss.util.*;
import org.lockss.util.os.*;
import org.lockss.util.storage.*;
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


  public static final String SERVICE_STATUS_TABLE_NAME = "RepositoriesTable";
  public static final String REPO_STATUS_TABLE_NAME = "RepositoryTable";
  public static final String AUIDS_STATUS_TABLE_NAME = "CollectionTable";
  public static final String ARTIFACTS_STATUS_TABLE_NAME = "ArtifactsTable";
  //  public static final String SPACE_TABLE_NAME = "RepositorySpace";

  public static final String AU_STATUS_TABLE_NAME =
    ArchivalUnitStatus.AU_STATUS_TABLE_NAME;

  static void registerAccessors(LockssDaemon daemon, StatusService statusServ) {
    statusServ.registerStatusAccessor(SERVICE_STATUS_TABLE_NAME,
				      new RepoCollsStatusAccessor(daemon));
    statusServ.registerStatusAccessor(REPO_STATUS_TABLE_NAME,
				      new RepoDetailStatusAccessor(daemon));
    statusServ.registerStatusAccessor(AUIDS_STATUS_TABLE_NAME,
				      new CollectionAuidsStatusAccessor(daemon));
    statusServ.registerStatusAccessor(ARTIFACTS_STATUS_TABLE_NAME,
				      new AuidArtifactsStatusAccessor(daemon));
    statusServ.registerOverviewAccessor(SERVICE_STATUS_TABLE_NAME,
					new Overview(daemon));

  }

  static void unregisterAccessors(StatusService statusServ) {
    statusServ.unregisterStatusAccessor(SERVICE_STATUS_TABLE_NAME);
    statusServ.unregisterStatusAccessor(REPO_STATUS_TABLE_NAME);
    statusServ.unregisterStatusAccessor(AUIDS_STATUS_TABLE_NAME);
    statusServ.unregisterStatusAccessor(ARTIFACTS_STATUS_TABLE_NAME);
    statusServ.unregisterOverviewAccessor(SERVICE_STATUS_TABLE_NAME);
  }

  // Base class
  static abstract class AbstractRepoStatusAccessor implements StatusAccessor {
    protected LockssDaemon daemon;
    protected RepositoryManager repoMgr;

    AbstractRepoStatusAccessor(LockssDaemon daemon) {
      this.daemon = daemon;
      repoMgr = daemon.getRepositoryManager();
    }
  }

  /** Display list of all Collections on all known repositories */
  static class RepoCollsStatusAccessor extends AbstractRepoStatusAccessor {

    private static final List columnDescriptors = ListUtil.list
      (
       new ColumnDescriptor("type", "Type", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("path", "URL / Path", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("size", "Size", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("free", "Free", ColumnDescriptor.TYPE_STRING),
       new ColumnDescriptor("full", "%Full", ColumnDescriptor.TYPE_STRING),
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

    // This is awkward because RepoSpec is a {repo, collection} pair but
    // we're showing both repo status and collection status.  Logicially it
    // should be two different tables but that would be more trouble for
    // users.
    private List getRows() {
      List rows = new ArrayList();
      for (RepoSpec rs : repoMgr.getV2RepositoryList()) {
	LockssRepository repo = rs.getRepository();
	PlatformUtil.DF repoDf = repoMgr.getRepositoryDF(rs.getSpec());
	String NO_COLLS = " (none) ";
	try {
	  Iterator<String> collsIter = repo.getCollectionIds().iterator();
	  if (!collsIter.hasNext()) {
	    collsIter = ListUtil.list(NO_COLLS).iterator();
	  }
	  for (String coll : new IteratorIterable<String>(collsIter)) {
	    Map row = new HashMap();
	    if (repoDf != null) {
	      row.put("size", StringUtil.sizeKBToString(repoDf.getSize()));
	      row.put("free", StringUtil.sizeKBToString(repoDf.getAvail()));
	      row.put("full", repoDf.getPercentString());
	      repoDf = null;
	    }
	    row.put("type", rs.getType());
	    if (!StringUtil.isNullString(rs.getPath())) {
	      StatusTable.Reference path =
		new StatusTable.Reference(rs.getPath(),
					  REPO_STATUS_TABLE_NAME,
					  rs.getSpec());
	      row.put("path", path);
	    }
	    if (NO_COLLS.equals(coll)) {
	      row.put("coll", coll);
	    } else {
	      row.put("coll",
		      new StatusTable.Reference(coll,
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

  /** Display scalar info about a LockssRepository */
  static class RepoDetailStatusAccessor extends AbstractRepoStatusAccessor {

    private static final List columnDescriptors = Collections.EMPTY_LIST;

    RepoDetailStatusAccessor(LockssDaemon daemon) {
      super(daemon);
    }

    public String getDisplayName() {
      return "Repository Info";
    }

    public void populateTable(StatusTable table)
        throws StatusService.NoSuchTableException {
      String key = table.getKey();
      RepoSpec rs = repoMgr.getV2Repository(key);
//       table.setColumnDescriptors(columnDescriptors);
//       table.setDefaultSortRules(sortRules);
//       table.setRows(getRows(table, rs));
      table.setSummaryInfo(getSummaryInfo(table, rs));
    }

    public boolean requiresKey() {
      return true;
    }


    private String storageStats(StorageInfo si) {
      StringBuilder sb = new StringBuilder();
      sb.append(StringUtil.sizeToString(si.getSize()));
      sb.append(", ");
      sb.append(StringUtil.sizeToString(si.getAvail()));
      sb.append(" free, ");
      sb.append(StringUtil.sizeToString(si.getUsed()));
      sb.append(" (");
      sb.append(si.getPercentUsedString());
      sb.append(")");
      sb.append(" used");
      if (!StringUtil.isNullString(si.getPath())) {
	sb.append(", at ");
	sb.append(si.getPath());
      }
      return sb.toString();
    }

    private List getSummaryInfo(StatusTable table, RepoSpec rs) {
      List res = new ArrayList();
      res.add(new StatusTable.SummaryInfo("Spec",
					  ColumnDescriptor.TYPE_STRING,
					  rs.getSpec()));
      LockssRepository repo = rs.getRepository();
      try {
	RepositoryInfo ri = repo.getRepositoryInfo();
	StorageInfo dsi = ri.getStoreInfo();
	res.add(new StatusTable.SummaryInfo("Datastore",
					    ColumnDescriptor.TYPE_STRING,
					    ( dsi.getType() + ": " +
					      storageStats(dsi))));
	if (dsi.getComponents() != null) {
	  for (StorageInfo csi : dsi.getComponents()) {
	    res.add(new StatusTable.SummaryInfo("Basedir",
						ColumnDescriptor.TYPE_STRING,
						storageStats(csi)).setIndent(2));
	  }
	}
	StorageInfo isi = ri.getIndexInfo();
	res.add(new StatusTable.SummaryInfo("Index",
					    ColumnDescriptor.TYPE_STRING,
					    ( isi.getType() + ": " +
					      storageStats(isi))));
      } catch (IOException e) {
	log.error("Coudln't get RepositoryInfo for " + rs, e);
      }

      int refetchedForContent = -1;
      if (repo instanceof RestLockssRepository) {
	RestLockssRepository rrepo = (RestLockssRepository)repo;
	ArtifactCache artCache = rrepo.getArtifactCache();
	if (artCache.isEnabled()) {
	  ArtifactCache.Stats stats = artCache.getStats();
	  refetchedForContent = stats.getRefetchedForContent();
	  String artStats =
	    String.format("%d hits, %d iter hits, %d misses, %d stores, %d invalidates",
			  stats.getCacheHits(),
			  stats.getCacheIterHits(),
			  stats.getCacheMisses(),
			  stats.getCacheStores(),
			  stats.getCacheInvalidates());
	  res.add(new StatusTable.SummaryInfo("Artifact cache",
					      ColumnDescriptor.TYPE_STRING,
					      artStats));
	  if (artCache.isInstrumented()) {
	    String hist = makeHist(stats.getArtHist(),
				   stats.getMaxArtSize());
	    res.add(new StatusTable.SummaryInfo("Histogram",
						ColumnDescriptor.TYPE_STRING,
						hist));
	    String iterHist = makeHist(stats.getArtIterHist(),
				       stats.getMaxArtSize());
	    res.add(new StatusTable.SummaryInfo("Iter hist",
						ColumnDescriptor.TYPE_STRING,
						iterHist));
	  }
	  String dataStats =
	    String.format("%d hits, %d misses, %d stores",
			  stats.getDataCacheHits(),
			  stats.getDataCacheMisses(),
			  stats.getDataCacheStores());
	  res.add(new StatusTable.SummaryInfo("ArtifactData cache",
					      ColumnDescriptor.TYPE_STRING,
					      dataStats));
	  if (artCache.isInstrumented()) {
	    String hist = makeHist(stats.getArtDataHist(),
				   stats.getMaxArtDataSize());
	    res.add(new StatusTable.SummaryInfo("Histogram",
						ColumnDescriptor.TYPE_STRING,
						hist));
	  }
	} else if (rrepo.isArtifactCacheEnabled()) {
	  res.add(new StatusTable.SummaryInfo("Artifact cache",
					      ColumnDescriptor.TYPE_STRING,
					      "enabling (waiting for confirmation from repository service)"));
	}
	if (table.getOptions().get(StatusTable.OPTION_DEBUG_USER)) {
	  ArtifactData.Stats adStats = ArtifactData.getStats();
	  StringBuilder sb = new StringBuilder();
	  sb.append(adStats.getTotalAllocated());
	  sb.append(" total, ");
	  sb.append(adStats.getWithContent());
	  sb.append(" w/ content, ");
	  if (refetchedForContent >= 0) {
	    sb.append(refetchedForContent);
	    sb.append(" refetched for content, ");
	  }
	  sb.append(StringUtil.numberOfUnits(adStats.getInputUsed(), "InputStream"));
	  sb.append(" used, ");
	  sb.append(adStats.getInputUnused() + " unused");
	  if (adStats.getUnreleased() > 0) {
	    sb.append(", ");
	    sb.append(adStats.getUnreleased() + " unreleased");
	  }
	  res.add(new StatusTable.SummaryInfo("ArtifactData",
					      ColumnDescriptor.TYPE_STRING,
					      sb.toString()));
	}
      }
      return res;
    }
  }

  private static String makeHist(int[] hist, int max) {
    return String.format("%d-%d by %ds: %s", 1, max, max / hist.length,
			 StringUtil.separatedString(hist, ", "));
  }

  /** Display list of AUIDs in a Collection */
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
					  rs.getCollection()));
      return res;
    }
  }

  /** Display list of Artifacts in a an AUID (in a Collection) */
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
					  rs.getCollection()));
      return res;
    }
  }

  static class Overview implements OverviewAccessor {

    private LockssDaemon daemon;
    private RepositoryManager repoMgr;

    public Overview(LockssDaemon daemon) {
      this.daemon = daemon;
      repoMgr = daemon.getRepositoryManager();
    }

    public Object getOverview(String tableName, BitSet options) {
      Map<String,PlatformUtil.DF> repos = repoMgr.getRepositoryDFMap();
      List res = new ArrayList();
      if (repos.size() == 1) {
	res.add("Repository: ");
      } else {
	res.add(repos.size() + " Repositories: ");
      }
      for (Iterator<Map.Entry<String,PlatformUtil.DF>> iter =
	     repos.entrySet().iterator();
	   iter.hasNext(); ) {
	PlatformUtil.DF df = iter.next().getValue();
	if (df != null) {
	  StringBuilder sb = new StringBuilder();
	  sb.append(StringUtil.sizeKBToString(df.getSize()));
	  sb.append(" (");
	  sb.append(df.getPercentString());
	  sb.append(" full, ");
	  sb.append(StringUtil.sizeKBToString(df.getAvail()));
	  sb.append(" free)");
	  Object s = sb.toString();
 	  if (df.isFullerThan(repoMgr.getDiskFullThreshold())) {
	    s = new StatusTable.DisplayedValue(s)
	      .setColor(Constants.COLOR_RED);
	  } else if (df.isFullerThan(repoMgr.getDiskWarnThreshold())) {
	    s = new StatusTable.DisplayedValue(s)
	      .setColor(Constants.COLOR_ORANGE);
	  }
	  res.add(s);
	} else {
	  res.add("???");
	}
	if (iter.hasNext()) {
	  res.add(", ");
	}
      }
      return new StatusTable.Reference(res, SERVICE_STATUS_TABLE_NAME);
    }
  }
}
