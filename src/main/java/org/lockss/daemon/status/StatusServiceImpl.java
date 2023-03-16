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
import javax.jms.*;

import org.apache.oro.text.regex.*;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.jms.*;
import org.lockss.util.*;
import org.lockss.util.time.*;
import org.lockss.util.time.TimeBase;
import org.lockss.config.*;
import org.lockss.daemon.status.StatusTable.ForeignTable;
import org.lockss.daemon.status.StatusTable.ForeignOverview;

/**
 * Main implementation of {@link StatusService}
 */
public class StatusServiceImpl
  extends BaseLockssManager implements StatusService, ConfigurableManager {

  private static L4JLogger logger = L4JLogger.getLogger();

  public static final String PREFIX = Configuration.PREFIX + "status.";

  /**
   * Name of default daemon status table
   */
  public static final String PARAM_DEFAULT_TABLE = PREFIX + "defaultTable";
  public static final String DEFAULT_DEFAULT_TABLE =
    OverviewStatus.OVERVIEW_STATUS_TABLE;

  public static final String JMS_PREFIX = PREFIX + "jms.";

  /** The jms topic to which status table notifications are sent
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_NOTIFICATION_TOPIC =
    JMS_PREFIX + "topic";
  public static final String DEFAULT_JMS_NOTIFICATION_TOPIC =
    "StatusTable";

  /** Enable jms send/receive
   * @ParamRelevance Testing
   */
  public static final String PARAM_JMS_ENABLED = JMS_PREFIX + "enabled";
  public static final boolean DEFAULT_JMS_ENABLED = true;

  /** The jms clientid of the StatusService.
   * @ParamRelevance Rare
   */
  public static final String PARAM_JMS_CLIENT_ID = JMS_PREFIX + "clientId";
  public static final String DEFAULT_JMS_CLIENT_ID = "StatusSvc";

  /** The amount of time to wait for responses to RequestOverviews
   * @ParamRelevance Rare
   */
  public static final String PARAM_OVERVIEW_TIMEOUT =
    JMS_PREFIX + "overviewTimeout";
  public static final long DEFAULT_OVERVIEW_TIMEOUT = 3000;

  /** The time after which an overview received from another component is
   * considered stale, and not displayed
   * @ParamRelevance Rare
   */
  public static final String PARAM_OVERVIEW_STALE =
    JMS_PREFIX + "overviewStale";
  public static final long DEFAULT_OVERVIEW_STALE = Constants.MINUTE;


  private String paramDefaultTable = DEFAULT_DEFAULT_TABLE;
  private boolean paramJmsEnabled = DEFAULT_JMS_ENABLED;

  // Maps table name to locally registered StatusAccessor
  private Map<String,StatusAccessor> statusAccessors = new HashMap<>();

  // Maps table name to locally registered OverviewAccessor
  private Map<String,OverviewAccessor> overviewAccessors = new HashMap<>();

  // Maps table name to locally registered ObjectReferenceAccessor
  private Map<String,ObjRefAccessorSpec> objRefAccessors = new HashMap<>();

  // Maps table name to ForeignTable registered in another service
  private Map<String,ForeignTable> foreignTableBindings = new HashMap<>();

  // Maps overview name to ForeignOverview describing service that has
  // globally registered the overview
  private Map<String,ForeignOverview> foreignOverviewBindings = new HashMap<>();

  // Table names whose registrations we have broadcast to others
  private Set<String> globallyRegisteredTables =
    Collections.synchronizedSet(new HashSet<>());

  // Overview names whose registrations we have broadcast to others
  private Set<String> globallyRegisteredOverviews =
    Collections.synchronizedSet(new HashSet<>());

  /** Associates a table with a service, This is an expedient way of
   * declaring which service should globally register which tables, and
   * which ones are of interest locally even if there's a global one.  This
   * knowledge should probably reside with the tables, but this is an
   * interim facility.
   */
  static class GlobalTableAssociation {
    private String name;
    private ServiceDescr descr;
    boolean globalOnly = false;
    GlobalTableAssociation(String name, ServiceDescr descr) {
      this.name = name;
      this.descr = descr;
    }

    GlobalTableAssociation setGlobalOnly() {
      globalOnly = true;
      return this;
    }

    /** Return the table name */
    String getName() {
      return name;
    }

    /** Return the ServiceDescr with which the table should be globally
     * associated */
    ServiceDescr getServiceDescr() {
      return descr;
    }

    /** Return true if a the manu item for a local table of the same name
     * should be suppressed when a global one is available */
    boolean isGlobalOnly() {
      return globalOnly;
    }
  }

  void initGlobalAssocs() {
    for (GlobalTableAssociation gta : globalAssocs) {
      globalTableAssocs.put(gta.getName(), gta);
    }
  }

  public boolean isGlobalOnlyTable(String name) {
    GlobalTableAssociation gta = globalTableAssocs.get(name);
    if (gta == null) return false;
    return gta.isGlobalOnly();
  }

  ServiceDescr getGlobalTableService(String name) {
    GlobalTableAssociation gta = globalTableAssocs.get(name);
    if (gta == null) return null;
    return gta.getServiceDescr();
  }

  private Map<String,GlobalTableAssociation> globalTableAssocs =
    new HashMap<>();

  /** Specification of tables that should be globally associated with some
   * service
   */
  static GlobalTableAssociation globalAssocs[] = {
    new GlobalTableAssociation(org.lockss.plugin.PluginStatus.ALL_TITLE_AUIDS,
			       ServiceDescr.SVC_CONFIG).setGlobalOnly(),
    new GlobalTableAssociation(org.lockss.crawler.CrawlManagerImpl.CRAWL_STATUS_TABLE_NAME,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.hasher.HashSvcSchedImpl.HASH_STATUS_TABLE,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.poller.v3.V3PollStatus.POLLER_STATUS_TABLE_NAME,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.poller.v3.V3PollStatus.VOTER_STATUS_TABLE_NAME,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation("SCommChans",
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation("SCommPeers",
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation("Identities",
			       ServiceDescr.SVC_POLLER).setGlobalOnly(),
    new GlobalTableAssociation("SchedQ",
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.state.ArchivalUnitStatus.SERVICE_STATUS_TABLE_NAME,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.state.ArchivalUnitStatus.AU_STATUS_TABLE_NAME,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.state.ArchivalUnitStatus.AUIDS_TABLE_NAME,
			       ServiceDescr.SVC_POLLER),
    new GlobalTableAssociation(org.lockss.metadata.MetadataManager.METADATA_STATUS_TABLE_NAME,
			       ServiceDescr.SVC_MDX).setGlobalOnly(),
  };


  private String notificationTopic = DEFAULT_JMS_NOTIFICATION_TOPIC;
  private String clientId = DEFAULT_JMS_CLIENT_ID;
  private long overviewTimeout = DEFAULT_OVERVIEW_TIMEOUT;
  private long overviewStale = DEFAULT_OVERVIEW_STALE;
  private JMSManager.TransportListener tListener;

  @Override
  public void initService(LockssApp app) throws LockssAppException {
    super.initService(app);
    initGlobalAssocs();
  }

  @Override
  public void startService() {
    super.startService();
    setUpJmsNotifications();
    tListener = new JMSManager.TransportListener() {
	// When connection to JMS broker re-established, must send all
	// registrations and ask others for theirs
	public void transportResumed() {
	  sendRequestRegisteredTables();
	  sendAllTableRegs();
	}};
    JMSManager mgr = getApp().getManagerByType(JMSManager.class);
    mgr.registerTransportListener(tListener);
    registerStatusAccessor(ALL_TABLES_TABLE, new AllTableStatusAccessor());


    sendRequestRegisteredTables();
  }

  public void stopService() {
    JMSManager mgr = getApp().getManagerByType(JMSManager.class);
    mgr.unregisterTransportListener(tListener);
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    paramDefaultTable = config.get(PARAM_DEFAULT_TABLE, DEFAULT_DEFAULT_TABLE);
    notificationTopic = config.get(PARAM_JMS_NOTIFICATION_TOPIC,
				   DEFAULT_JMS_NOTIFICATION_TOPIC);
    clientId = config.get(PARAM_JMS_CLIENT_ID, DEFAULT_JMS_CLIENT_ID);
    overviewTimeout = config.getTimeInterval(PARAM_OVERVIEW_TIMEOUT,
					     DEFAULT_OVERVIEW_TIMEOUT);
    overviewStale = config.getTimeInterval(PARAM_OVERVIEW_STALE,
					     DEFAULT_OVERVIEW_STALE);
    paramJmsEnabled = config.getBoolean(PARAM_JMS_ENABLED, DEFAULT_JMS_ENABLED);
  }

  void setUpJmsNotifications() {
    if (paramJmsEnabled) {
      setUpJmsReceive(clientId, notificationTopic, true,
		      new MapMessageListener("StatusTable Registration Listener"));
      setUpJmsSend(clientId, notificationTopic);
    }
  }

  public String getDefaultTableName() {
    return paramDefaultTable;
  }

  public StatusTable getTable(String tableName, String key)
      throws StatusService.NoSuchTableException {
    return getTable(tableName, key, null);
  }

  public StatusTable getTable(String tableName, String key, BitSet options)
      throws StatusService.NoSuchTableException {
    if (tableName == null) {
      throw new
	StatusService.NoSuchTableException("Called with null tableName");
    }

    StatusTable table = new StatusTable(tableName, key);
    if (options != null) {
      table.setOptions(options);
    }
    fillInTable(table);
    return table;
  }

  public void fillInTable(StatusTable table)
      throws StatusService.NoSuchTableException {
    StatusAccessor statusAccessor;
    String tableName = table.getName();
    String key = table.getKey();
    synchronized(statusAccessors) {
      statusAccessor = statusAccessors.get(tableName);
    }
    if (statusAccessor == null) {
      throw new StatusService.NoSuchTableException("Table not found: "
						   + tableName);
    }
    if (statusAccessor.requiresKey() && table.getKey() == null) {
      throw new StatusService.NoSuchTableException(tableName +
						   " requires a key value");
    }
    statusAccessor.populateTable(table);
    if (table.getTitle() == null) {
      try {
	table.setTitle(statusAccessor.getDisplayName());
      } catch (Exception e) {
	// ignored
      }
    }
  }

  public ForeignTable getForeignTable(String table) {
    return foreignTableBindings.get(table);
  }

  public ForeignOverview getForeignOverview(String table) {
    synchronized (foreignOverviewBindings) {
      ForeignOverview fo = foreignOverviewBindings.get(table);
      if (fo == null) {
	return null;
      }
      if (fo.getValueTimestamp() < TimeBase.nowMs() - overviewStale) {
	logger.debug2("Omitting stale overview for {}", table);
	return null;
      }
      return fo;
    }
  }

  static Pattern badTablePat =
    RegexpUtil.uncheckedCompile("[^a-zA-Z0-9_-]",
				Perl5Compiler.READ_ONLY_MASK);

  private boolean isBadTableName(String tableName) {
    return RegexpUtil.getMatcher().contains(tableName, badTablePat);
  }

  public void registerStatusAccessor(String tableName,
				     StatusAccessor statusAccessor) {
    registerStatusAccessor(tableName, statusAccessor,
			   getGlobalTableService(tableName));
  }

  /** Register an accessor for a StatusTable
   * @param tableName table name
   * @param statusAccessor
   * @param globalInService descriptor of service in which this table's
   * registration should be broadcast globally
   */
  public void registerStatusAccessor(String tableName,
				     StatusAccessor statusAccessor,
				     ServiceDescr globalInService) {
    logger.debug("Registering statusAccessor for table "+tableName);
    if (isBadTableName(tableName)) {
      throw new InvalidTableNameException("Invalid table name: "+tableName);
    }

    synchronized(statusAccessors) {
      StatusAccessor oldAccessor = statusAccessors.get(tableName);
      if (oldAccessor != null) {
	throw new
	  StatusService.MultipleRegistrationException(oldAccessor
						      +" already registered "
						      +"for "+tableName);
      }
      statusAccessors.put(tableName, statusAccessor);
    }
    if (isGlobal(globalInService)) {
      globallyRegisteredTables.add(tableName);
      sendRegisterTable(tableName, statusAccessor);
    }
  }

  boolean isGlobal(ServiceDescr globalInService) {
    logger.debug2("isGlobal({}): {}", globalInService, getApp().isMyService(globalInService));
    return (globalInService != null
	    && getApp().isMyService(globalInService));
  }

  public void unregisterStatusAccessor(String tableName){
    synchronized(statusAccessors) {
      statusAccessors.remove(tableName);
    }
    logger.debug2("Unregistered statusAccessor for table "+tableName);
    if (globallyRegisteredTables.remove(tableName)) {
      sendUnregisterTable(tableName);
    }
  }

  public void registerOverviewAccessor(String tableName,
				       OverviewAccessor acc) {
    registerOverviewAccessor(tableName, acc,
			     getGlobalTableService(tableName));
  }

  public void registerOverviewAccessor(String tableName,
				       OverviewAccessor acc,
				       ServiceDescr globalInService) {
    if (isBadTableName(tableName)) {
      throw new InvalidTableNameException("Invalid table name: "+tableName);
    }
    logger.debug("Registering OverviewAccessor for table "+tableName);

    synchronized(overviewAccessors) {
      OverviewAccessor oldAccessor = overviewAccessors.get(tableName);
      if (oldAccessor != null) {
	throw new
	  StatusService.MultipleRegistrationException(oldAccessor
						      +" already registered "
						      +"for "+tableName);
      }
      overviewAccessors.put(tableName, acc);
    }
    if (isGlobal(globalInService)) {
      globallyRegisteredOverviews.add(tableName);
      sendRegisterOverview(tableName, acc);
    }
    logger.debug2("Registered overview accessor for table "+tableName);
  }

  public void unregisterOverviewAccessor(String tableName){
    synchronized(overviewAccessors) {
      overviewAccessors.remove(tableName);
    }
    logger.debug2("Unregistered overviewAccessor for table "+tableName);
    if (globallyRegisteredOverviews.remove(tableName)) {
      sendUnregisterOverview(tableName);
    }
  }

  static final BitSet EMPTY_BITSET = new BitSet();

  public Object getOverview(String tableName) {
    return getOverview(tableName, null);
  }

  public Object getOverview(String tableName, BitSet options) {
    OverviewAccessor acc;
    synchronized (overviewAccessors) {
      acc = overviewAccessors.get(tableName);
    }
    if (acc != null) {
      return acc.getOverview(tableName,
			     (options == null) ? EMPTY_BITSET : options);
    } else {
      return null;
    }
  }

  public StatusTable.Reference getReference(String tableName, Object obj) {
    ObjRefAccessorSpec spec;
    synchronized (objRefAccessors) {
      spec = objRefAccessors.get(tableName);
    }
    if (spec != null && spec.cls.isInstance(obj)) {
      return spec.accessor.getReference(tableName, obj);
    } else {
      return null;
    }
  }

  // not implemented yet.
  public List getReferences(Object obj) {
    return Collections.EMPTY_LIST;
  }

  public void
    registerObjectReferenceAccessor(String tableName, Class cls,
				    ObjectReferenceAccessor objRefAccessor) {
    synchronized (objRefAccessors) {
      ObjRefAccessorSpec oldSpec = objRefAccessors.get(tableName);
      if (oldSpec != null) {
	throw new
	  StatusService.MultipleRegistrationException(oldSpec.accessor
						      +" already registered "
						      +"for "+tableName);
      }
      ObjRefAccessorSpec spec = new ObjRefAccessorSpec(cls, tableName,
						       objRefAccessor);
      objRefAccessors.put(tableName, spec);
    }
    logger.debug2("Registered ObjectReferenceAccessor for table "+tableName +
		  ", class " + cls);
  }

  public void
    unregisterObjectReferenceAccessor(String tableName, Class cls) {
    synchronized (objRefAccessors) {
      objRefAccessors.remove(tableName);
    }
    logger.debug2("Unregistered ObjectReferenceAccessor for table "+tableName);
  }

  private static class ObjRefAccessorSpec {
    Class cls;
    String table;
    ObjectReferenceAccessor accessor;

    ObjRefAccessorSpec(Class cls, String table,
		       ObjectReferenceAccessor accessor) {
      this.cls = cls;
      this.table = table;
      this.accessor = accessor;
    }
  }

  // JMS notification support

  // Notification message is a map:
  // verb - {RegisterTable, UnregisterTable, RequestTableRegistrations}
  // tableName - table key
  // tableTitle - display name
  // requiresKey - true iff StatusAccessor requires a key
  // debugOnly

  public static final String JMS_VERB = "verb";
  public static final String JMS_TABLE_NAME = "tableName";
  public static final String JMS_TABLE_TITLE = "tableTitle";
  public static final String JMS_TABLE_REQUIRES_KEY = "requiresKey";
  public static final String JMS_TABLE_DEBUG_ONLY = "debugOnly";
  public static final String JMS_TABLE_OPTIONS = "options";
  public static final String JMS_URL_STEM = "urlStem";
  public static final String JMS_SERVICE_NAME = "serviceName";
  public static final String JMS_CONTENT = "content";

  public static final String VERB_REG_TABLE = "RegisterTable";
  public static final String VERB_UNREG_TABLE = "UnregisterTable";
  public static final String VERB_REG_OVERVIEW = "RegisterOverview";
  public static final String VERB_UNREG_OVERVIEW = "UnregisterOverview";
  public static final String VERB_REQ_REGS = "RequestTableRegistrations";
  public static final String VERB_REQ_OVERVIEWS = "RequestOverviews";
  public static final String VERB_OVERVIEW = "Overview";

  /** Send a message with the specified verb and the values in the map, if
   * supplied */
  private void sendVerb(String verb, Map<String,Object> map) {
    if (jmsProducer != null) {
      if (map == null) {
	map = new HashMap<>();
      }
      map.put(JMS_VERB, verb);
      try {
	logger.debug("Sending {}", map);
	jmsProducer.sendMap(map);
      } catch (JMSException e) {
	logger.error("Couldn't send {}", VERB_REQ_REGS, e);
      }
    }
  }

  protected void sendRequestRegisteredTables() {
    sendVerb(VERB_REQ_REGS, null);
  }

  protected void sendRegisterTable(String tableName, StatusAccessor sa) {
    if (jmsProducer != null) {
      ServiceBinding myBinding = getApp().getMyServiceBinding();
      if (myBinding == null) {
	logger.warn("Can't send table registration because we have no ServiceBinding");
	return;
      }
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_TABLE_NAME, tableName);
      map.put(JMS_TABLE_TITLE, getTableTitle(tableName, sa));
      if (sa.requiresKey()) {
	map.put(JMS_TABLE_REQUIRES_KEY, "true");
      }
      if (sa instanceof StatusAccessor.DebugOnly) {
	map.put(JMS_TABLE_DEBUG_ONLY, "true");
      }
      map.put(JMS_URL_STEM, myBinding.getUiStem("http"));
      sendVerb(VERB_REG_TABLE, map);
    }
  }

  protected void sendUnregisterTable(String tableName) {
    if (jmsProducer != null) {
      ServiceBinding myBinding = getApp().getMyServiceBinding();
      if (myBinding == null) {
	logger.warn("Can't send table registration because we have no ServiceBinding");
	return;
      }
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_TABLE_NAME, tableName);
      map.put(JMS_URL_STEM, myBinding.getUiStem("http"));
      sendVerb(VERB_UNREG_TABLE, map);
    }
  }

  protected void sendRegisterOverview(String tableName, OverviewAccessor acc) {
    if (jmsProducer != null) {
      ServiceDescr myDescr = getApp().getMyServiceDescr();
      ServiceBinding myBinding = getApp().getMyServiceBinding();
      if (myBinding == null) {
	logger.warn("Can't send table registration because we have no ServiceBinding");
	return;
      }
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_VERB, VERB_REG_OVERVIEW);
      map.put(JMS_TABLE_NAME, tableName);
      map.put(JMS_URL_STEM, myBinding.getUiStem("http"));
      map.put(JMS_SERVICE_NAME, myDescr.getAbbrev());
      sendVerb(VERB_REG_OVERVIEW, map);
    }
  }

  protected void sendUnregisterOverview(String tableName) {
    if (jmsProducer != null) {
      ServiceBinding myBinding = getApp().getMyServiceBinding();
      if (myBinding == null) {
	logger.warn("Can't send table registration because we have no ServiceBinding");
	return;
      }
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_TABLE_NAME, tableName);
      map.put(JMS_URL_STEM, myBinding.getUiStem("http"));
      sendVerb(VERB_UNREG_OVERVIEW, map);
    }
  }

  /** Send a request for overview values, wait until they arrive or
   * timeout */
  public void requestOverviews(BitSet options) {
    sendRequestOverviews(options);
    waitForOverviews(overviewTimeout);
  }

  private void sendRequestOverviews(BitSet options) {
    Map<String,Object> map = new HashMap<>();
    map.put(JMS_TABLE_OPTIONS, JsonUtil.asLong(options));
    sendVerb(VERB_REQ_OVERVIEWS, map);
  }

  void sendAllTableRegs() {
    Map<String,StatusAccessor> copy = new HashMap<>();
    synchronized(statusAccessors) {
      copy.putAll(statusAccessors);
    }
    for (Map.Entry<String,StatusAccessor> ent : copy.entrySet()) {
      if (globallyRegisteredTables.contains(ent.getKey())) {
	sendRegisterTable(ent.getKey(), ent.getValue());
      }
    }

    Map<String,OverviewAccessor> copy2 = new HashMap<>();
    synchronized(overviewAccessors) {
      copy2.putAll(overviewAccessors);
    }
    for (Map.Entry<String,OverviewAccessor> ent : copy2.entrySet()) {
      if (globallyRegisteredOverviews.contains(ent.getKey())) {
	sendRegisterOverview(ent.getKey(), ent.getValue());
      }
    }
  }

  // Broadcast all the overviews we have registered globally
  void sendOverviews(Map map, String table) {
    BitSet options = null;
    // This treatment of the options is wrong, as the generated overviews
    // get broadcast to everybody, not just us.  E.g., if OPTION_DEBUG_USER
    // is set, the resulting overview value may be used in a context where
    // ROLE_DEBUG is not set.
    if (map.containsKey(JMS_TABLE_OPTIONS)) {
      long opts = (long)map.get(JMS_TABLE_OPTIONS);
      options = JsonUtil.asBitSet(opts);
    }
    List<String> copy = new ArrayList<>();
    synchronized(globallyRegisteredOverviews) {
      copy.addAll(globallyRegisteredOverviews);
    }
    for (String overTable : copy) {
      sendOverview(overTable, options);
    }
  }

  /** Generate and send the overview value for the named table */
  void sendOverview(String table, BitSet options) {
    Object val = getOverview(table, options);
    if (val == null) {
      return;
    }
    if (jmsProducer != null) {
      ServiceDescr myDescr = getApp().getMyServiceDescr();
      ServiceBinding myBinding = getApp().getMyServiceBinding();
      if (myBinding == null) {
	logger.warn("Can't send table registration because we have no ServiceBinding");
	return;
      }
      Map<String,Object> map = new HashMap<>();
      map.put(JMS_TABLE_NAME, table);
      map.put(JMS_URL_STEM, myBinding.getUiStem("http"));
      String str = getTextDisplayString(val);
      if (StringUtil.isNullString(str)) {
	return;
      }
      map.put(JMS_CONTENT, str);
      sendVerb(VERB_OVERVIEW, map);
    }
  }


  /** Incoming JMS message */
  @Override
  protected void receiveMessage(Map map) {
    logger.debug2("Received message: " + map);
    try {
      String verb = (String)map.get(JMS_VERB);
      String table = (String)map.get(JMS_TABLE_NAME); // might be null
      switch (verb) {
      case VERB_REG_TABLE:
	processIncomingTableReg(map, table);
	break;
      case VERB_UNREG_TABLE:
	processIncomingTableUnreg(map, table);
	break;
      case VERB_REG_OVERVIEW:
	processIncomingOverviewReg(map, table);
	break;
      case VERB_UNREG_OVERVIEW:
	processIncomingOverviewUnreg(map, table);
	break;
      case VERB_REQ_REGS:
	sendAllTableRegs();
	break;
      case VERB_REQ_OVERVIEWS:
	sendOverviews(map, table);
	break;
      case VERB_OVERVIEW:
	processIncomingOverview(map, table);
	break;
      default:
	logger.warn("Received unknown status registration message: {}", verb);
      }
    } catch (ClassCastException e) {
      logger.error("Wrong type field in message: {}", map, e);
    }
  }

  /** RegisterTable */
  void processIncomingTableReg(Map map, String table) {
    if (isFromMe(map)) {
      logger.warn("Received message from me, shouldn't happen: {}", map);
      return;
    }
    ForeignTable curFt = foreignTableBindings.get(table);
    String title = (String)map.get(JMS_TABLE_TITLE);
    String stem = (String)map.get(JMS_URL_STEM);
    boolean requiresKey = getMapBool(map, JMS_TABLE_REQUIRES_KEY);
    boolean debugOnly = getMapBool(map, JMS_TABLE_DEBUG_ONLY);
    ForeignTable ft =
      new ForeignTable(table, title, stem, requiresKey, debugOnly);
    if (curFt == null) {
      logger.debug("Registering foreign table {}", ft);
      foreignTableBindings.put(table, ft);
    } else if (!curFt.getStem().equals(stem)) {
      // XXX Can't rely on services to unregister tables when they crash,
      // so this will be normal.  Will have to change to handle multiple
      // service instances.
      logger.warn("Replacing global registration for table {} with {} was {}",
		  table, stem, curFt.getStem());
      foreignTableBindings.put(table, ft);
    }
  }

  /** UnregisterTable */
  void processIncomingTableUnreg(Map map, String table) {
    String stem = (String)map.get(JMS_URL_STEM);
    ForeignTable ft = foreignTableBindings.get(table);
    if (ft == null) {
      logger.warn("Ignored global unregistration for unregistered table {}",
		  table);
    } else if (stem.equals(ft.getStem())) {
      logger.debug("Unregistering foreign table {}", ft);
      foreignTableBindings.remove(table);
    } else {
      logger.warn("Ignored global unregistration for table {} from {}; is bound to {}",
		  table, stem, ft.getStem());
    }
  }

  /** RegisterOverview */
  void processIncomingOverviewReg(Map map, String table) {
    if (isFromMe(map)) {
      logger.warn("Received message from me, shouldn't happen: {}", map);
      return;
    }
    synchronized (foreignOverviewBindings) {
      ForeignOverview curFo = foreignOverviewBindings.get(table);
      String title = (String)map.get(JMS_TABLE_TITLE);
      String stem = (String)map.get(JMS_URL_STEM);
      String serviceName = (String)map.get(JMS_SERVICE_NAME);
      ForeignOverview fo = new ForeignOverview(table, serviceName, stem);
      if (curFo == null) {
	logger.debug("Registering foreign overview {}", fo);
	foreignOverviewBindings.put(table, fo);
      } else if (!curFo.getStem().equals(stem)) {
	// XXX Can't rely on services to unregister overviews when they crash,
	// so this will be normal.  Will have to change to handle multiple
	// service instances.
	logger.warn("Replacing global registration for overview {} with {} was {}",
		    table, stem, curFo.getStem());
	foreignOverviewBindings.put(table, fo);
      }
    }
  }

  /** UnregisterOverview */
  void processIncomingOverviewUnreg(Map map, String table) {
    String stem = (String)map.get(JMS_URL_STEM);
    synchronized (foreignOverviewBindings) {
      ForeignOverview fo = foreignOverviewBindings.get(table);
      if (fo == null) {
	logger.warn("Ignored global unregistration for unregistered overview {}",
		    table);
      } else if (stem.equals(fo.getStem())) {
	logger.debug("Unregistering foreign overview {}", fo);
	foreignOverviewBindings.remove(table);
      } else {
	logger.warn("Ignored global unregistration for overview {} from {}; is bound to {}",
		    table, stem, fo.getStem());
      }
    }
  }

  /** Overview value */
  void processIncomingOverview(Map map, String table) {
    if (isFromMe(map)) {
      logger.warn("Received message from me, shouldn't happen: {}", map);
      return;
    }
    String title = (String)map.get(JMS_TABLE_TITLE);
    String stem = (String)map.get(JMS_URL_STEM);
    String content = (String)map.get(JMS_CONTENT);
    String key = table + ":" + stem;
    StatusTable.Reference ref =
      new StatusTable.Reference(content, table).setServiceStem(stem);
    synchronized (foreignOverviewBindings) {
      ForeignOverview fo = foreignOverviewBindings.get(table);
      if (fo == null) {
	logger.error("Received Overview for table with no known ForeignOverview binding: {}", table);
	return;
      }
      fo.setValue(ref);
      synchronized(waitMonitor) {
	waitMonitor.notifyAll();
      }
    }
  }


  // Return true iff we've recently received an overview value from all the
  // expected sources
  boolean areOverviewsRecent() {
    long now = TimeBase.nowMs();
    synchronized (foreignOverviewBindings) {
      for (ForeignOverview fo : foreignOverviewBindings.values()) {
	if (fo.getStem() == null) {
	  // Shouldn't happen currently - ForeignOverview created only when
	  // receive msg w/ stem
	  continue;
	}
	if (fo.getValueTimestamp() >= now - overviewTimeout) {
	  // If this value is recent enough, keep checking
	  continue;
	}
	// Found one that's not recent
	return false;
      }
    }
    return true;
  }

  Object waitMonitor = new Object();

  // Wait until all expected overview values are recent, or the timeout
  // elapses
  boolean waitForOverviews(long timeout) {
    long exp = TimeBase.nowMs() + timeout;
    while (!areOverviewsRecent()) {
      synchronized(waitMonitor) {
	long wait = TimeBase.msUntil(exp);
	if (wait <= 0) {
	  return false;
	}
	try {
	  logger.debug2("waitMonitor.wait({})", wait);
	  waitMonitor.wait(wait);
	} catch (InterruptedException e) {
	  return false;
	}
      }
    }
    return true;
  }

  String getTextDisplayString(Object val) {
    return org.lockss.servlet.DaemonStatus.getTextDisplayString(val);
  }

  String getTableTitle(String tableName, StatusAccessor statusAccessor) {
    String title = null;
    try {
      title = statusAccessor.getDisplayName();
    } catch (Exception e) {
      // no action, title is null here
    }
    // getDisplayName can return null or throw
    if (title == null) {
      title = tableName;
    }
    return title;
  }

  boolean getMapBool(Map map, String key) {
    try {
      return Boolean.parseBoolean((String)map.get(key));
    } catch (ClassCastException e) {
      return false;
    }
  }

  boolean isFromMe(Map map) {
    ServiceBinding myBinding = getApp().getMyServiceBinding();
    if (myBinding == null) {
      logger.warn("Can't check isFromMe because we have no ServiceBinding");
      return false;
    }
    return myBinding.getUiStem("http").equals(map.get(JMS_URL_STEM));
  }


  private class AllTableStatusAccessor implements StatusAccessor {
    private List columns;
    private List sortRules;
    private static final String COL_NAME = "table_name";
    private static final String COL_TITLE = "Available Tables";
    private static final String ALL_TABLE_TITLE = "Box Overview";

    public AllTableStatusAccessor() {
      ColumnDescriptor col =
	new ColumnDescriptor(COL_NAME, COL_TITLE,
			     ColumnDescriptor.TYPE_STRING);
      columns = ListUtil.list(col);

      StatusTable.SortRule sortRule =
	new StatusTable.SortRule(COL_NAME, true);

      sortRules = ListUtil.list(sortRule);
    }

    public String getDisplayName() {
      return ALL_TABLE_TITLE;
    }

    private List getRows(boolean isDebugUser) {
      synchronized(statusAccessors) {
	List rows = new ArrayList(statusAccessors.size());
	// Include the locally registered tables that don't require a key
	for (Map.Entry<String,StatusAccessor> ent : statusAccessors.entrySet()){
	  String tableName = ent.getKey();
	  StatusAccessor statusAccessor = ent.getValue();;
	  // Don't include the table of all tables
	  if (ALL_TABLES_TABLE.equals(tableName) ||
	      statusAccessor.requiresKey() ||
	      (!isDebugUser &&
	       (statusAccessor instanceof StatusAccessor.DebugOnly))) {
	    continue;
	  }
	  String label = getTableTitle(tableName, statusAccessor);
	  boolean includeLocal = true;

	  // Check for a global table with this name
	  ForeignTable ft = getForeignTable(tableName);
	  if (isIncludeForeignTable(ft, isDebugUser)) {
	    // Identify the local one iff a global one will also be included
	    label += " (local)";
	    // exclude the local one if the global one should be exlusive
	    GlobalTableAssociation gta = globalTableAssocs.get(tableName);
	    if (gta != null && gta.isGlobalOnly()) {
	      includeLocal = false;
	    }
	  }
	  if (includeLocal) {
	    StatusTable.Reference ref =
	      new StatusTable.Reference(label, tableName, null);
	    ref.setLocal(true);
	    rows.add(Collections.singletonMap(COL_NAME, ref));
	  }
	}
	// Add the globally registered tables that don't require a key
	for (Map.Entry<String,ForeignTable> ent :
	       foreignTableBindings.entrySet()){
	  String tableName = ent.getKey();
	  ForeignTable ft = ent.getValue();
	  if (!isIncludeForeignTable(ft, isDebugUser)) {
	    continue;
	  }
	  String title = ft.getTitle();
	  // Ensure this sorts after the local table of the same name
	  // XXX should use a sort colume
	  StatusTable.Reference ref =
	    new StatusTable.Reference(title + " ", tableName, null)
	    .setServiceStem(ft.getStem())
// 	    .setServiceName(ft.getDisplayName());
	    .setServiceName(getGlobalTableService(tableName).getAbbrev());

	  rows.add(Collections.singletonMap(COL_NAME, ref));
	}
	return rows;
      }
    }

    boolean isIncludeForeignTable(ForeignTable ft, boolean isDebugUser) {
      return ft != null && !ft.requiresKey()
	&& (isDebugUser || !ft.isDebugOnly());
    }

    /**
     * Returns false
     * @return false
     */
    public boolean requiresKey() {
      return false;
    }

    /**
     * Populate the {@link StatusTable} with entries for each table that
     * doesn't require a key
     * @param table {@link StatusTable} to populate as the table of all tables
     * that don't require a key
     */
    public void populateTable(StatusTable table) {
      table.setColumnDescriptors(columns);
      table.setDefaultSortRules(sortRules);
      table.setRows(getRows(table.getOptions().get(StatusTable.OPTION_DEBUG_USER)));
    }

  }
}
