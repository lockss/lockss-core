/*

Copyright (c) 2000-2020 Board of Trustees of Leland Stanford Jr. University,
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

import javax.servlet.*;
import java.io.*;
import java.util.*;
import org.mortbay.html.*;
import org.lockss.app.*;
import org.lockss.daemon.RestServicesManager;
import org.lockss.metadata.extractor.RestMetadataExtractorClient;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;
import org.lockss.poller.*;
import org.lockss.crawler.*;
import org.lockss.state.*;
import org.lockss.config.*;
import org.lockss.remote.*;
import org.lockss.plugin.*;
import org.lockss.account.*;

/** UI to invoke various daemon actions
 */
@SuppressWarnings("serial")
public class DebugPanel extends LockssServlet {

  public static final String PREFIX = Configuration.PREFIX + "debugPanel.";

  /**
   * Priority for crawls started from the debug panel
   */
  public static final String PARAM_CRAWL_PRIORITY = 
    PREFIX + "crawlPriority";
  public static final int DEFAULT_CRAWL_PRIORITY = 10;

  /**
   * Priority for crawls started from the debug panel
   */
  public static final String PARAM_ENABLE_DEEP_CRAWL = 
    PREFIX + "deepCrawlEnabled";
  private static final boolean DEFAULT_ENABLE_DEEP_CRAWL = false;

  /**
   * Allow Delete URL
   */
  public static final String PARAM_DELETE_ENABLED = PREFIX + "deleteEnabled";
  private static final boolean DEFAULT_DELETE_ENABLED = false;

  static final String KEY_ACTION = "action";
  static final String KEY_MSG = "msg";
  static final String KEY_NAME_SEL = "name_sel";
  static final String KEY_NAME_TYPE = "name_type";
  static final String KEY_AUID = "auid";
  static final String KEY_URL = "url";
  static final String KEY_REFETCH_DEPTH = "depth";
  static final String KEY_TIME = "time";

  static final String ACTION_MAIL_BACKUP = "Mail Backup File";
  static final String ACTION_THROW_IOEXCEPTION = "Throw IOException";
  static final String ACTION_FIND_URL = "Find Preserved URL";

  public static final String ACTION_REINDEX_METADATA = "Reindex Metadata";
  public static final String ACTION_FORCE_REINDEX_METADATA =
      "Force Reindex Metadata";
  public static final String ACTION_START_V3_POLL = "Start V3 Poll";
  static final String ACTION_FORCE_START_V3_POLL = "Force V3 Poll";
  public static final String ACTION_START_CRAWL = "Start Crawl";
  public static final String ACTION_FORCE_START_CRAWL = "Force Start Crawl";
  public static final String ACTION_START_DEEP_CRAWL = "Deep Crawl";
  public static final String ACTION_FORCE_START_DEEP_CRAWL = "Force Deep Crawl";
  public static final String ACTION_CHECK_SUBSTANCE = "Check Substance";
  public static final String ACTION_VALIDATE_FILES = "Validate Files";
  public static final String ACTION_DELETE_URL = "Delete File";
  static final String ACTION_CRAWL_PLUGINS = "Crawl Plugins";
  static final String ACTION_RELOAD_CONFIG = "Reload Config";
  static final String ACTION_SLEEP = "Sleep";

  /** Set of actions for which audit alerts shouldn't be generated */
  public static final Set noAuditActions = SetUtil.set(ACTION_FIND_URL);


  static final String COL2 = "colspan=2";
  static final String COL2CENTER = COL2 + " align=center";

  static Logger log = Logger.getLogger();

  private LockssDaemon daemon;
  private PluginManager pluginMgr;
  private RestServicesManager svcsMgr;
  private PollManager pollManager;
  private CrawlManager crawlMgr;
  private ConfigManager cfgMgr;
  private ServiceBinding mdxServiceBinding = null;
  private RemoteApi rmtApi;

  boolean showResult;
  boolean showForcePoll;
  boolean showForceCrawl;
  boolean showForceReindexMetadata;

  String formAuid;
  String formDepth = "100";

  protected void resetLocals() {
    resetVars();
    super.resetLocals();
  }

  void resetVars() {
    formAuid = null;
    errMsg = null;
    statusMsg = null;
    showForcePoll = false;
    showForceCrawl = false;
    showForceReindexMetadata = false;
  }

  public void init(ServletConfig config) throws ServletException {
    super.init(config);
    daemon = getLockssDaemon();
    pluginMgr = daemon.getPluginManager();
    svcsMgr = daemon.getManagerByType(RestServicesManager.class);
    try {
      pollManager = daemon.getPollManager();
    } catch (IllegalArgumentException e) {
      log.debug("No poll manager, some functions nonfunctional");
      pollManager = null;
    }
    try {
      crawlMgr = daemon.getCrawlManager();
    } catch (IllegalArgumentException e) {
      log.debug("No crawl manager, some functions nonfunctional");
      crawlMgr = null;
    }
    cfgMgr = daemon.getConfigManager();
    try {
      rmtApi = daemon.getRemoteApi();
    } catch (IllegalArgumentException e) {
      log.debug("No RemoteApi, some functions nonfunctional");
      rmtApi = null;
    }
    mdxServiceBinding = daemon.getServiceBinding(ServiceDescr.SVC_MDX);
    if (mdxServiceBinding == null ||
        !svcsMgr.isServiceReady(mdxServiceBinding)) {
      mdxServiceBinding = null;
      log.debug("No MDX Service binding, some functions nonfunctional");
    }
  }

  public void lockssHandleRequest() throws IOException {
    resetVars();
    boolean showForm = true;
    String action = getParameter(KEY_ACTION);

    if (!StringUtil.isNullString(action)) {

      formAuid = getParameter(KEY_AUID);
      formDepth = getParameter(KEY_REFETCH_DEPTH);

      UserAccount acct = getUserAccount();
      if (acct != null && !noAuditActions.contains(action)) {
	acct.auditableEvent("used debug panel action: " + action +
			    " AU ID: " + formAuid);
      }
    }

    if (ACTION_MAIL_BACKUP.equals(action)) {
      doMailBackup();
    }
    if (ACTION_RELOAD_CONFIG.equals(action)) {
      doReloadConfig();
    }
    if (ACTION_SLEEP.equals(action)) {
      doSleep();
    }
    if (ACTION_THROW_IOEXCEPTION.equals(action)) {
      doThrow();
    }
    if (ACTION_START_V3_POLL.equals(action)) {
      doV3Poll();
    }
    if (ACTION_FORCE_START_V3_POLL.equals(action)) {
      forceV3Poll();
    }
    if (ACTION_START_CRAWL.equals(action)) {
      doCrawl(false, false);
    }
    if (ACTION_FORCE_START_CRAWL.equals(action)) {
      doCrawl(true, false);
    }
    if (ACTION_START_DEEP_CRAWL.equals(action)) {
      doCrawl(false, true);
    }
    if (ACTION_FORCE_START_DEEP_CRAWL.equals(action)) {
      doCrawl(true, true);
    }
    if (ACTION_CHECK_SUBSTANCE.equals(action)) {
      doCheckSubstance();
    }
    if (ACTION_DELETE_URL.equals(action)) {
      doDeleteUrl();
    }
    if (ACTION_VALIDATE_FILES.equals(action)) {
      doValidateFiles();
    }
    if (ACTION_CRAWL_PLUGINS.equals(action)) {
      crawlPluginRegistries();
    }
    if (ACTION_FIND_URL.equals(action)) {
      showForm = doFindUrl();
    }
    if (ACTION_REINDEX_METADATA.equals(action)) {
      doReindexMetadata();
    }
    if (ACTION_FORCE_REINDEX_METADATA.equals(action)) {
      forceReindexMetadata();
    }
    if (showForm) {
      displayPage();
    }
  }

  private void doMailBackup() {
    try {
      rmtApi.createConfigBackupFile(RemoteApi.BackupFileDisposition.Mail);
    } catch (Exception e) {
      errMsg = "Error: " + e.getMessage();
    }
  }

  private void doReloadConfig() {
    cfgMgr.requestReload();
  }

  private void doThrow() throws IOException {
    String msg = getParameter(KEY_MSG);
    throw new IOException(msg != null ? msg : "Test message");
  }

  private void doSleep() throws IOException {
    String timestr = getParameter(KEY_TIME);
    try {
      long time = StringUtil.parseTimeInterval(timestr);
      Deadline.in(time).sleep();
      statusMsg = "Slept for " + TimeUtil.timeIntervalToString(time);
    } catch (NumberFormatException e) {
      errMsg = "Illegal duration: " + e;
    } catch (InterruptedException e) {
      errMsg = "Interrupted: " + e;
    }
  }

  private void doReindexMetadata() {
    ArchivalUnit au = getAu();
    if (au == null) return;
    try {
      startReindexingMetadata(au, false);
    } catch (RuntimeException e) {
      log.error("Can't reindex metadata", e);
      errMsg = "Error: " + e.toString();
    }
  }

  private void forceReindexMetadata() {
    ArchivalUnit au = getAu();
    if (au == null) return;
    try {
      startReindexingMetadata(au, true);
    } catch (RuntimeException e) {
      log.error("Can't reindex metadata", e);
      errMsg = "Error: " + e.toString();
    }
  }

  private void doCrawl(boolean force, boolean deep) {
    ArchivalUnit au = getAu();
    if (au == null) return;
    try {
      startCrawl(au, force, deep);
    } catch (CrawlManagerImpl.NotEligibleException.RateLimiter e) {
      errMsg = "AU has crawled recently (" + e.getMessage()
	+ ").  Click again to override.";
      showForceCrawl = true;
      return;
    } catch (CrawlManagerImpl.NotEligibleException e) {
      errMsg = "Can't enqueue crawl: " + e.getMessage();
    }
  }

  private void crawlPluginRegistries() {
    StringBuilder sb = new StringBuilder();
    for (ArchivalUnit au : pluginMgr.getAllRegistryAus()) {
      sb.append(au.getName());
      sb.append(": ");
      try {
	startCrawl(au, true, false);
	sb.append("Queued.");
      } catch (CrawlManagerImpl.NotEligibleException e) {
	sb.append("Failed: ");
	sb.append(e.getMessage());
      }
      sb.append("\n");
    }
    statusMsg = sb.toString();
  }

  private boolean startCrawl(ArchivalUnit au, boolean force, boolean deep)
      throws CrawlManagerImpl.NotEligibleException {
    CrawlManagerImpl cmi = (CrawlManagerImpl)crawlMgr;
    if (force) {
      RateLimiter limit = cmi.getNewContentRateLimiter(au);
      if (!limit.isEventOk()) {
	limit.unevent();
      }
    }
    cmi.checkEligibleToQueueNewContentCrawl(au);
    String delayMsg = "";
    String deepMsg = "";
    try {
      cmi.checkEligibleForNewContentCrawl(au);
    } catch (CrawlManagerImpl.NotEligibleException e) {
      if (e.isWrongService()) {
        errMsg = "Can't start that crawl in this service: " + e.getMessage();
        return false;
      }
      if (!e.isTemporary()) {
        errMsg = "Can't start that crawl: " + e.getMessage();
        return false;
      }
      delayMsg = ", Start delayed due to: " + e.getMessage();
    }
    Configuration config = ConfigManager.getCurrentConfig();
    int pri = config.getInt(PARAM_CRAWL_PRIORITY, DEFAULT_CRAWL_PRIORITY);

    CrawlReq req;
    try {
      req = new CrawlReq(au, new CrawlerStatus(au, au.getStartUrls(), null));
      req.setPriority(pri);
      if (deep) {
	int d = Integer.parseInt(formDepth);
	if (d < 0) {
	  errMsg = "Illegal refetch depth: " + d;
	  return false;
	}
	req.setRefetchDepth(d);
	deepMsg = "Deep (" + req.getRefetchDepth() + ") ";

      }
    } catch (NumberFormatException e) {
      errMsg = "Illegal refetch depth: " + formDepth;
      return false;
    } catch (RuntimeException e) {
      log.error("Couldn't create CrawlReq: " + au, e);
      errMsg = "Couldn't create CrawlReq: " + e.toString();
      return false;
    }
    cmi.startNewContentCrawl(req);
    statusMsg = deepMsg + "Crawl requested for " + au.getName() + delayMsg;
    return true;
  }

  private void doCheckSubstance() {
    ArchivalUnit au = getAu();
    if (au == null) return;
    try {
      checkSubstance(au);
    } catch (RuntimeException e) {
      log.error("Error in SubstanceChecker", e);
      errMsg = "Error in SubstanceChecker; " + e.toString();
    }
  }

  private void checkSubstance(ArchivalUnit au) {
    SubstanceChecker subChecker = new SubstanceChecker(au);
    if (!subChecker.isEnabled()) {
      errMsg = "No substance patterns defined for plugin.";
      return;
    }
    AuState auState = AuUtil.getAuState(au);
    SubstanceChecker.State oldState = auState.getSubstanceState();
    SubstanceChecker.State newState = subChecker.findSubstance();
    String chtxt = (newState == oldState
		    ? "(unchanged)"
		    : "(was " + oldState.toString() + ")");
    switch (newState) {
    case Unknown:
      log.error("Shouldn't happen: SubstanceChecker returned Unknown");
      errMsg = "Error in SubstanceChecker; see log.";
      break;
    case Yes:
      statusMsg = "AU has substance " + chtxt + ": " + au.getName();
      auState.setSubstanceState(SubstanceChecker.State.Yes);
      break;
    case No:
      statusMsg = "AU has no substance " + chtxt + ": " + au.getName();
      auState.setSubstanceState(SubstanceChecker.State.No);
      break;
    }
  }

  private void doValidateFiles() throws IOException {
    ArchivalUnit au = getAu();
    if (au == null) return;
    if (!AuUtil.hasContentValidator(au)) {
      errMsg = au.getPlugin().getPluginName() +
	" does not supply a content validator";
      return;
    }
    String redir =
      srvURL(AdminServletManager.SERVLET_LIST_OBJECTS,
	     PropUtil.fromArgs("type", "auvalidate",
			       "auid", au.getAuId()));

    resp.setContentLength(0);
    resp.sendRedirect(redir);
  }

  private boolean startReindexingMetadata(ArchivalUnit au, boolean force) {
    if (mdxServiceBinding == null) {
      errMsg = "Metadata Extraction Service is not accessible.";
      return false;
    }

    if (!force) {
      if (!AuUtil.hasCrawled(au)) {
        errMsg = "Au has never crawled. Click again to reindex metadata";
        showForceReindexMetadata = true;
        return false;
      }

      AuState auState = AuUtil.getAuState(au);
      switch (auState.getSubstanceState()) {
      case No:
        errMsg = "Au has no substance. Click again to reindex metadata";
        showForceReindexMetadata = true;
        return false;
      case Unknown:
      case Yes:
	// fall through
      }
    }

    if (au != null) {
      try {
	// Schedule the metadata reindexing.
	RestMetadataExtractorClient client =
	    new RestMetadataExtractorClient(mdxServiceBinding.getRestStem());
	String result = client.scheduleMetadataExtraction(au.getAuId(), true);
	log.debug2("result = " + result);
	return true;
      } catch (Exception e) {
	log.error("Cannot schedule reindex metadata for " + au.getName(), e);
      }
    }

    if (force) {
      errMsg = "Still cannot reindex metadata for " + au.getName();
    } else {
      errMsg = "Cannot reindex metadata for " + au.getName();
    }
    return false;
  }

  private void doDeleteUrl() {
    ArchivalUnit au = getAu();
    if (au == null) return;
    String url = getParameter(KEY_URL);
    if (url == null) {
      errMsg = "Enter a URL in the URL field";
      return;
    }
    try {
      deleteUrl(au, url);
    } catch (RuntimeException e) {
      log.error("Error deleting URL", e);
      errMsg = "Error deleting URL; see log.";
    }
  }

  private void deleteUrl(ArchivalUnit au, String url) {
    org.lockss.laaws.rs.core.LockssRepository v2Repo =
      daemon.getRepositoryManager().getV2Repository().getRepository();
    String ns =
      daemon.getRepositoryManager().getV2Repository().getNamespace();
    if (v2Repo == null) {
      errMsg = "Can't delete, not using V2 repository";
      return;
    }
    try {
      if (v2Repo.getArtifact(ns, au.getAuId(), url) == null) {
	errMsg = "No such file: " + url + " in " + au.getName();
	return;
      }
      int cnt = 0;
      for (org.lockss.laaws.rs.model.Artifact art :
	     v2Repo.getArtifactsAllVersions(ns, au.getAuId(), url)) {
	log.debug2("deleting: " + art);
	v2Repo.deleteArtifact(art);
	cnt++;
      }
      org.lockss.laaws.rs.model.Artifact delArt =
	v2Repo.getArtifact(ns, au.getAuId(), url);
      if (delArt == null) {
	statusMsg ="Deleted " + StringUtil.numberOfUnits(cnt, "version") +
	  " of " + url;
      } else {
	log.debug2("Delete failed: " + delArt);
	errMsg = "File was not deleted: " + url;
	return;
      }
    } catch (IOException e) {
      errMsg = "Error obtaining artifact: " + url + ": " + e.toString();
      return;
    }
  }

  private void doV3Poll() {
    ArchivalUnit au = getAu();
    if (au == null) return;
    try {
      callV3ContentPoll(au);
    } catch (PollManager.NotEligibleException e) {
      errMsg = "AU is not eligible for poll: " + e.getMessage();
//       errMsg = "Ineligible: " + e.getMessage() +
// 	"<br>Click again to force new poll.";
//       showForcePoll = true;
      return;
    } catch (Exception e) {
      log.error("Can't start poll", e);
      errMsg = "Error: " + e.toString();
    }
  }

  private void forceV3Poll() {
    ArchivalUnit au = getAu();
    if (au == null) return;
    try {
      callV3ContentPoll(au);
    } catch (Exception e) {
      log.error("Can't start poll", e);
      errMsg = "Error: " + e.toString();
    }
  }

  private void callV3ContentPoll(ArchivalUnit au)
      throws PollManager.NotEligibleException {
    log.debug("Enqueuing a V3 Content Poll on " + au.getName());
    PollSpec spec = new PollSpec(au.getAuCachedUrlSet(), Poll.V3_POLL);
    pollManager.enqueueHighPriorityPoll(au, spec);
    statusMsg = "Enqueued V3 poll for " + au.getName();
  }

  private boolean doFindUrl() throws IOException {
    
    String url = getParameter(KEY_URL);
    if (StringUtil.isNullString(url)) {
      errMsg = "Enter a URL in the URL field";
      return true;
    }

    String redir =
      srvURL(AdminServletManager.SERVLET_DAEMON_STATUS,
	     PropUtil.fromArgs("table",
			       ArchivalUnitStatus.AUS_WITH_URL_TABLE_NAME,
			       "key", url));

    resp.setContentLength(0);
//     resp.sendRedirect(resp.encodeRedirectURL(redir));
    resp.sendRedirect(redir);
    return false;
  }

  ArchivalUnit getAu() {
    if (StringUtil.isNullString(formAuid)) {
      errMsg = "Select an AU";
      return null;
    }
    ArchivalUnit au = pluginMgr.getAuFromId(formAuid);
    if (au == null) {
      errMsg = "No such AU.  Select an AU";
      return null;
    }
    return au;
  }

  private void displayPage() throws IOException {
    Page page = newPage();
    layoutErrorBlock(page);
    ServletUtil.layoutExplanationBlock(page, "Debug Actions");
    page.add(makeForm());
    page.add("<br>");
    endPage(page);
  }

  private Element makeForm() {
    Composite comp = new Composite();
    Form frm = new Form(srvURL(myServletDescr()));
    frm.method("POST");


    frm.add("<br><center>");
    Input reload = new Input(Input.Submit, KEY_ACTION, ACTION_RELOAD_CONFIG);
    setTabOrder(reload);
    frm.add(reload);
    frm.add(" ");
    Input backup = new Input(Input.Submit, KEY_ACTION, ACTION_MAIL_BACKUP);
    setTabOrder(backup);
    frm.add(backup);
    frm.add(" ");
    Input crawlplug = new Input(Input.Submit, KEY_ACTION, ACTION_CRAWL_PLUGINS);
    if (crawlMgr != null && daemon.getCrawlMode().isCrawlPlugins()) {
      setTabOrder(crawlplug);
    } else {
      disableButton(crawlplug);
    }
    frm.add(crawlplug);
    frm.add("</center>");
    ServletDescr d1 = AdminServletManager.SERVLET_HASH_CUS;
    if (isServletRunnable(d1)) {
      frm.add("<br><center>"+srvLink(d1, d1.heading)+"</center>");
    }
    ServletDescr d2 = AdminServletManager.SERVLET_GENERATE_LCAP_KEYS;
    if (isServletRunnable(d2)) {
      frm.add("<br><center>"+srvLink(d2, d2.heading)+"</center>");
    }
    Input findUrl = new Input(Input.Submit, KEY_ACTION, ACTION_FIND_URL);
    Input findUrlText = new Input(Input.Text, KEY_URL);
    findUrlText.setSize(50);
    setTabOrder(findUrl);
    setTabOrder(findUrlText);
    frm.add("<br><center>URL:&nbsp;" + findUrlText + "</center>");
    frm.add("<br><center>");
    frm.add(findUrl);
    if (CurrentConfig.getBooleanParam(PARAM_DELETE_ENABLED, DEFAULT_DELETE_ENABLED)) {
      Input delUrl = new Input(Input.Submit, KEY_ACTION, ACTION_DELETE_URL);
      frm.add(" ");
      frm.add(delUrl);
    }
    frm.add("</center>");

    Input thrw = new Input(Input.Submit, KEY_ACTION, ACTION_THROW_IOEXCEPTION);
    Input thmsg = new Input(Input.Text, KEY_MSG);
    setTabOrder(thrw);
    setTabOrder(thmsg);
    frm.add("<br><center>"+thrw+" " + thmsg + "</center>");

    frm.add("<br><center>AU Actions: select AU</center>");
    Composite ausel = ServletUtil.layoutSelectAu(this, KEY_AUID, formAuid);
    frm.add("<br><center>"+ausel+"</center>");
    setTabOrder(ausel);

    Input v3Poll = new Input(Input.Submit, KEY_ACTION,
			     ( showForcePoll
			       ? ACTION_FORCE_START_V3_POLL
			       : ACTION_START_V3_POLL));
    // Don't check whether poller is enabled as it's possible to start
    // polls manually when it's disabled.
    if (pollManager == null) {
      disableButton(v3Poll);
    }
    Input crawl = new Input(Input.Submit, KEY_ACTION,
			    ( showForceCrawl
			      ? ACTION_FORCE_START_CRAWL
			      : ACTION_START_CRAWL));
    if (crawlMgr == null || !crawlMgr.isCrawlerEnabled()) {
      disableButton(crawl);
    }

    frm.add("<br><center>");
    frm.add(v3Poll);
    frm.add(" ");
    frm.add(crawl);
    if (CurrentConfig.getBooleanParam(PARAM_ENABLE_DEEP_CRAWL,
				      DEFAULT_ENABLE_DEEP_CRAWL)) {
      Input deepCrawl = new Input(Input.Submit, KEY_ACTION,
				  ( showForceCrawl
				    ? ACTION_FORCE_START_DEEP_CRAWL
				    : ACTION_START_DEEP_CRAWL));
      Input depthText = new Input(Input.Text, KEY_REFETCH_DEPTH, formDepth);
      depthText.setSize(4);
      setTabOrder(depthText);
      frm.add(" ");
      frm.add(deepCrawl);
      frm.add(depthText);
    }
    Input checkSubstance = new Input(Input.Submit, KEY_ACTION,
				     ACTION_CHECK_SUBSTANCE);
    frm.add("<br>");
    frm.add(checkSubstance);
    Input validateFiles = new Input(Input.Submit, KEY_ACTION,
				    ACTION_VALIDATE_FILES);
    frm.add(" ");
    frm.add(validateFiles);
    Input reindex = new Input(Input.Submit, KEY_ACTION,
                              ( showForceReindexMetadata
                                ? ACTION_FORCE_REINDEX_METADATA
                                : ACTION_REINDEX_METADATA));
    if (mdxServiceBinding == null) {
      disableButton(reindex);
    }
    frm.add(" ");
    frm.add(reindex);
    frm.add("</center>");

    comp.add(frm);
    return comp;
  }

  private void disableButton(Input button) {
    button.attribute("disabled", "true");
    button.attribute("class", "disabled");
  }

}
