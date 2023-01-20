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
import java.net.*;
import javax.jms.*;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.*;

import org.lockss.app.*;
import org.lockss.jms.*;
import org.lockss.test.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.util.jms.*;
import org.lockss.util.net.IPAddr;
import org.lockss.util.time.TimerUtil;


public class TestStatusServiceImpl extends LockssTestCase4 {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final Object[][] colArray1 = {
    {"name", "Name", Integer.valueOf(ColumnDescriptor.TYPE_STRING), "Foot note"},
    {"rank", "Rank", Integer.valueOf(ColumnDescriptor.TYPE_INT)}
  };

  private static final Object[][] rowArray1 = {
    {"AA", "1"},
    {"BB", "2"},
    {"CC", "3"},
    {"DD", "4"}
  };

  private static final List sortRules1 =
    ListUtil.list(new StatusTable.SortRule("name", true));

  private static final Object[][] colArray2 = {
    {"cache", "Box", Integer.valueOf(ColumnDescriptor.TYPE_STRING)},
  };

  private static final Object[][] rowArray2 = {
    {"Cache A"},
    {"Cache B"},
    {"Cache C"},
    {"Cache D"},
    {"Cache E"},
    {"Cache F"},
    {"Cache G"}
  };

  private static final List sortRules2 =
    ListUtil.list(new StatusTable.SortRule("cache", true));

  private static final Object[][] colArray3 = {
    {"name", "Name", Integer.valueOf(ColumnDescriptor.TYPE_STRING)},
    {"rank", "Rank", Integer.valueOf(ColumnDescriptor.TYPE_INT)},
  };

  private static final Object[][] rowArray3 = {
    {"Cache B", Integer.valueOf(1)},
    {"Cache A", Integer.valueOf(2)},
    {"Cache C", Integer.valueOf(0)}
  };

  private static final Object[][] colArray4 = {
    {"name", "Name", Integer.valueOf(ColumnDescriptor.TYPE_STRING)},
    {"rank", "Rank", Integer.valueOf(ColumnDescriptor.TYPE_INT)},
    {"secondRank", "SecondRank", Integer.valueOf(ColumnDescriptor.TYPE_INT)},
  };

  private static final Object[][] rowArray4 = {
    {"AName", Integer.valueOf(0), Integer.valueOf(400)},
    {"BName", Integer.valueOf(2), Integer.valueOf(450)},
    {"BName", Integer.valueOf(4), Integer.valueOf(0)},
    {"BName", Integer.valueOf(4), Integer.valueOf(2)},
    {"CName", Integer.valueOf(1), Integer.valueOf(-1)}
  };

  private static BrokerService broker;

  private MyStatusServiceImpl statusService;
  private JMSManager jmsMgr;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_START_BROKER, "true");
    getMockLockssDaemon().startManagers(JMSManager.class);
    jmsMgr = getMockLockssDaemon().getManagerByType(JMSManager.class);
    statusService = new MyStatusServiceImpl();
    statusService.initService(getMockLockssDaemon());
  }

  @After
  public void tearDown() throws Exception {
    statusService.stopService();
    getMockLockssDaemon().stopManagers();
    getMockLockssDaemon().stopDaemon();
    super.tearDown();
  }

  @Test
  public void testGetTableWithNullTableNameThrows()
      throws StatusService.NoSuchTableException {
    try {
      statusService.getTable(null, "blah");
      fail("Should have thrown when given a null name");
    } catch (StatusService.NoSuchTableException iae) {
    }
  }

  @Test
  public void testGetTableWithUnknownTableThrows() {
    try {
      statusService.getTable("Bad name", "bad key");
      fail("Should have thrown when given a bad name and key");
    } catch (StatusService.NoSuchTableException tnfe) {
    }
  }

  @Test
  public void testMultipleRegistriesThrows() {
    statusService.registerStatusAccessor("table1", new MockStatusAccessor());
    try {
      statusService.registerStatusAccessor("table1", new MockStatusAccessor());
      fail("Should have thrown after multiple register attempts");
    } catch (StatusService.MultipleRegistrationException re) {
    }
  }

  @Test
  public void testRegisteringAllTableThrows() {
    try {
      statusService.startService(); //registers table of all tables
      statusService.registerStatusAccessor(StatusService.ALL_TABLES_TABLE,
					   new MockStatusAccessor());
      fail("Should have thrown after trying to register StatusAccessor for "+
	   "all tables");
    } catch (StatusService.MultipleRegistrationException re) {
    }
  }

  @Test
  public void testRegisteringInvalidTableNameThrows() {
    try {
      statusService.registerStatusAccessor("!Table",
					   new MockStatusAccessor());
      fail("Should have thrown after trying to register StatusAccessor "+
	   "with bad table name");
    } catch (StatusService.InvalidTableNameException re) {
    }

    try {
      statusService.registerStatusAccessor("name with spaces",
					   new MockStatusAccessor());
      fail("Should have thrown after trying to register StatusAccessor "+
	   "with bad table name");
    } catch (StatusService.InvalidTableNameException re) {
    }
  }

  @Test
  public void testUnregisteringBadDoesntThrow() {
    statusService.unregisterStatusAccessor("table1");
  }

  @Test
  public void testMultipleUnregisteringDontThrow() {
    statusService.registerStatusAccessor("table1", new MockStatusAccessor());
    statusService.unregisterStatusAccessor("table1");
    statusService.unregisterStatusAccessor("table1");
  }

  @Test
  public void testCanRegisterUnregisteredTable() {
    statusService.registerStatusAccessor("table1", new MockStatusAccessor());
    statusService.unregisterStatusAccessor("table1");
    statusService.registerStatusAccessor("table1", new MockStatusAccessor());
  }

  JmsProducer prod;
  JmsConsumer cons;

  void setUpJms() throws JMSException {
    // Can't use the connection maintained by JMSManager (at least not for
    // our Producer), as StatusServiceImpl's Consumer ignores locally sent
    // messages
    ConnectionFactory connectionFactory =
      new ActiveMQConnectionFactory(jmsMgr.getConnectUri());
    Connection conn = connectionFactory.createConnection();
    conn.start();

    JmsFactory fact = jmsMgr.getJmsFactory();

    prod = fact.createTopicProducer(null, StatusServiceImpl.DEFAULT_JMS_NOTIFICATION_TOPIC, conn);
    cons = fact.createTopicConsumer(null, StatusServiceImpl.DEFAULT_JMS_NOTIFICATION_TOPIC, true, null, conn);
  }

  Map MSG_REQ = MapUtil.map("verb", "RequestTableRegistrations");

  Map MSG_REG_1 = MapUtil.map("urlStem", "http://localhost:1234",
			      "verb", "RegisterTable",
			      "tableTitle", "MockStatusAccessor",
			      "tableName", "V3PollerTable");

  Map MSG_REG_2 = MapUtil.map("urlStem", "http://localhost:4321",
			      "verb", "RegisterTable",
			      "tableTitle", "Vibes",
			      "tableName", "Xylophones");

  @Test
  public void testRegistrationNotification() throws Exception {
    // Must have a ServiceDescr with a ServiceBinding in order for table
    // registrations to be sent
    getMockLockssDaemon().setMyServiceDescr(ServiceDescr.SVC_POLLER);
    ConfigurationUtil.addFromArgs(LockssApp.PARAM_SERVICE_BINDINGS,
				  "poller=:1233:1234");

    setUpJms();
    // This should cause a RequestTableRegistrations message to be sent
    statusService.startService();

    // Register global table, ensure RegisterTable message is sent
    String pollTable =
      org.lockss.poller.v3.V3PollStatus.POLLER_STATUS_TABLE_NAME;
    statusService.registerStatusAccessor(pollTable, new MockStatusAccessor());
    assertEquals(ListUtil.list(MSG_REQ, MSG_REG_1), nMsgs(cons, 2));

    // Send a RequestTableRegistrations message, should receive it (from
    // ourselves) and the one registered global table
    prod.sendMap(MSG_REQ);
    assertEquals(ListUtil.list(MSG_REG_1), nMsgs(cons, 1));

    // Tell MyStatusServiceImpl to post sem when foreign registration done
    SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
    statusService.setForRegSem(sem);

    // Send a RegisterTable message, ensure it shows up in foreign
    // registrations map
    prod.sendMap(MSG_REG_2);

    assertTableExists(pollTable);
    
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
    assertNotNull(statusService.getForeignTable("Xylophones"));

  }

  // Read and return n messages, ensure there are no more waiting
  List<Map> nMsgs(JmsConsumer cons, int n) throws Exception {
    List<Map> res = new ArrayList<>();
    while (res.size() != n) {
      res.add(cons.receiveMap(TIMEOUT_SHOULDNT));
    }
    assertEquals(null, cons.receiveMap(TIMEOUT_SHOULD));
    return res;
  }

  void assertTableExists(String tableName) {
    try {
      statusService.getTable(tableName, null);
    } catch (StatusService.NoSuchTableException e) {
      fail("Table " + tableName + " threw", e);
    }
  }

  @Test
  public void testGetTableHasName()
      throws StatusService.NoSuchTableException {
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1,
						rowArray1);
    statusAccessor.setDefaultSortRules(sortRules1, null);
    statusService.registerStatusAccessor("table1", statusAccessor);

    StatusTable table = statusService.getTable("table1", null);
    assertEquals("table1", table.getName());
    assertNull(table.getKey());
  }

  @Test
  public void testGetTableHasKey()
      throws StatusService.NoSuchTableException {
    String key = "theKey";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1, rowArray1, key);
    statusAccessor.setDefaultSortRules(sortRules1, key);
    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", key);
    assertEquals(key, table.getKey());
  }

  @Test
  public void testGetTableTitle()
      throws StatusService.NoSuchTableException {
    String key = "theKey";
    String tableTitle = "Table title";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1, rowArray1, key);
    statusAccessor.setDefaultSortRules(sortRules1, key);
    statusAccessor.setTitle(tableTitle, key);
    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", key);
    assertEquals(tableTitle, table.getTitle());
  }

  static final Object[][] summaryInfo = {
    {"SummaryInfo1", Integer.valueOf(ColumnDescriptor.TYPE_STRING), "SummaryInfo value one"},
    {"SummaryInfo2", Integer.valueOf(ColumnDescriptor.TYPE_INT), Integer.valueOf(2)},
    {"SummaryInfo3", Integer.valueOf(ColumnDescriptor.TYPE_STRING), "SummaryInfo value 3"}
  };

  @Test
  public void testGetTableSummaryInfo()
      throws StatusService.NoSuchTableException {
    String key = "theKey";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1, rowArray1,
						key, summaryInfo);

    statusAccessor.setDefaultSortRules(sortRules1, key);
    statusService.registerStatusAccessor("table1", statusAccessor);

    StatusTable table = statusService.getTable("table1", key);
    List expectedSummaryInfo =
      MockStatusAccessor.makeSummaryInfoFrom(summaryInfo);
    assertNotNull(table.getSummaryInfo());
    assertSummaryInfoEqual(expectedSummaryInfo, table.getSummaryInfo());
  }

  private void assertSummaryInfoEqual(List expected, List actual) {
    assertEquals("Lists had different sizes", expected.size(), actual.size());
    Iterator expectedIt = expected.iterator();
    Iterator actualIt = actual.iterator();
    while(expectedIt.hasNext()) {
      StatusTable.SummaryInfo expectedSInfo =
	(StatusTable.SummaryInfo)expectedIt.next();
      StatusTable.SummaryInfo actualSInfo =
	(StatusTable.SummaryInfo)actualIt.next();
      assertEquals(expectedSInfo.getTitle(), actualSInfo.getTitle());
      assertEquals(expectedSInfo.getType(), actualSInfo.getType());
      assertEquals(expectedSInfo.getValue(), actualSInfo.getValue());
    }
    assertFalse(actualIt.hasNext());
  }

  @Test
  public void testGetTableWithKey()
      throws StatusService.NoSuchTableException {
    String key = "key1";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1, rowArray1, key);
    statusAccessor.setDefaultSortRules(sortRules1, key);

    statusService.registerStatusAccessor("table1", statusAccessor);

    StatusTable table = statusService.getTable("table1", key);

    List expectedColumns =
      MockStatusAccessor.makeColumnDescriptorsFrom(colArray1);
    assertColumnDescriptorsEqual(expectedColumns,
				 table.getColumnDescriptors());

    List expectedRows =
      MockStatusAccessor.makeRowsFrom(expectedColumns, rowArray1);
    assertRowsEqual(expectedRows, table.getSortedRows());
  }


  @Test
  public void testGetTablesWithDifferentKeys()
      throws StatusService.NoSuchTableException {
    String key1 = "key1";
    String key2 = "key2";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1, rowArray1, key1);
    statusAccessor.setDefaultSortRules(sortRules1, key1);

    MockStatusAccessor.addToStatusAccessor(statusAccessor, colArray2,
					   rowArray2, key2);
    statusAccessor.setDefaultSortRules(sortRules2, key2);

    statusService.registerStatusAccessor("table1", statusAccessor);

    StatusTable table = statusService.getTable("table1", key1);
    assertNotNull(table);

    List expectedColumns =
      MockStatusAccessor.makeColumnDescriptorsFrom(colArray1);
    assertColumnDescriptorsEqual(expectedColumns,
				 table.getColumnDescriptors());

    List expectedRows =
      MockStatusAccessor.makeRowsFrom(expectedColumns, rowArray1);
    assertRowsEqual(expectedRows, table.getSortedRows());

    table = statusService.getTable("table1", key2);
    assertNotNull(table);

    expectedColumns = MockStatusAccessor.makeColumnDescriptorsFrom(colArray2);
    assertColumnDescriptorsEqual(expectedColumns,
				 table.getColumnDescriptors());

    expectedRows = MockStatusAccessor.makeRowsFrom(expectedColumns, rowArray2);
    assertRowsEqual(expectedRows, table.getSortedRows());
  }

  @Test
  public void testSortsByAscStrings()
      throws StatusService.NoSuchTableException {
    StatusTable.SortRule rule = new StatusTable.SortRule("name", true);
    List sortRules = ListUtil.list(rule);

    Object[][] expectedRowsArray = new Object[3][];
    expectedRowsArray[0] = rowArray3[1];
    expectedRowsArray[1] = rowArray3[0];
    expectedRowsArray[2] = rowArray3[2];

    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray3, rowArray3);
    statusAccessor.setDefaultSortRules(sortRules, null);

    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", null);
    List expectedRows =
      MockStatusAccessor.makeRowsFrom(MockStatusAccessor.makeColumnDescriptorsFrom(colArray3),
				      expectedRowsArray);
    List actualRows = table.getSortedRows();
    assertRowsEqual(expectedRows, actualRows);
  }

  @Test
  public void testGetTableSortsDescStrings()
      throws StatusService.NoSuchTableException {
    StatusTable.SortRule rule = new StatusTable.SortRule("name", false);
    List sortRules = ListUtil.list(rule);

    Object[][] expectedRowsArray = new Object[3][];
    expectedRowsArray[0] = rowArray3[2];
    expectedRowsArray[1] = rowArray3[0];
    expectedRowsArray[2] = rowArray3[1];

    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray3, rowArray3);
    statusAccessor.setDefaultSortRules(sortRules, null);

    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", null);
    List expectedRows =
      MockStatusAccessor.makeRowsFrom(MockStatusAccessor.makeColumnDescriptorsFrom(colArray3),
				      expectedRowsArray);
    List actualRows = table.getSortedRows(sortRules);
    assertRowsEqual(expectedRows, actualRows);
  }

  @Test
  public void testGetTableSortsNumsAsc()
      throws StatusService.NoSuchTableException {
    StatusTable.SortRule rule = new StatusTable.SortRule("rank", true);
    List sortRules = ListUtil.list(rule);

    Object[][] expectedRowsArray = new Object[3][];
    expectedRowsArray[0] = rowArray3[2];
    expectedRowsArray[1] = rowArray3[0];
    expectedRowsArray[2] = rowArray3[1];

    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray3, rowArray3);
    statusAccessor.setDefaultSortRules(sortRules, null);

    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", null);
    List expectedRows =
      MockStatusAccessor.makeRowsFrom(MockStatusAccessor.makeColumnDescriptorsFrom(colArray3),
				      expectedRowsArray);
    List actualRows = table.getSortedRows(sortRules);
    assertRowsEqual(expectedRows, actualRows);
  }

  @Test
  public void testGetTableSortsMultiCols()
      throws StatusService.NoSuchTableException {
    List sortRules =
      ListUtil.list(new StatusTable.SortRule("name", false),
		    new StatusTable.SortRule("rank", true),
		    new StatusTable.SortRule("secondRank", false));

    Object[][] expectedRowsArray = new Object[5][];
    expectedRowsArray[0] = rowArray4[4];
    expectedRowsArray[1] = rowArray4[1];
    expectedRowsArray[2] = rowArray4[3];
    expectedRowsArray[3] = rowArray4[2];
    expectedRowsArray[4] = rowArray4[0];

    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray4, rowArray4);
    statusAccessor.setDefaultSortRules(sortRules, null);
    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", null);
    List expectedRows =
      MockStatusAccessor.makeRowsFrom(MockStatusAccessor.makeColumnDescriptorsFrom(colArray4),
				      expectedRowsArray);
    List actualRows = table.getSortedRows();
    assertRowsEqual(expectedRows, actualRows);
  }

  @Test
  public void testSortsByNonDefaultRules()
      throws StatusService.NoSuchTableException {
    List sortRules = ListUtil.list(new StatusTable.SortRule("name", false));

    Object[][] expectedRowsArray = new Object[3][];
    expectedRowsArray[0] = rowArray3[2];
    expectedRowsArray[1] = rowArray3[0];
    expectedRowsArray[2] = rowArray3[1];

    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray3, rowArray3);
    statusAccessor.setDefaultSortRules(sortRules1, null);

    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", null);
    List expectedRows =
      MockStatusAccessor.makeRowsFrom(MockStatusAccessor.makeColumnDescriptorsFrom(colArray3),
				      expectedRowsArray);
    List actualRows = table.getSortedRows(sortRules);
    assertRowsEqual(expectedRows, actualRows);
  }

  @Test
  public void testSortsByDefaultDefaultRules()
      throws StatusService.NoSuchTableException {
    Object[][] expectedRowsArray = new Object[3][];
    expectedRowsArray[0] = rowArray3[1];
    expectedRowsArray[1] = rowArray3[0];
    expectedRowsArray[2] = rowArray3[2];

    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray3, rowArray3);
    statusService.registerStatusAccessor("table1", statusAccessor);
    StatusTable table = statusService.getTable("table1", null);
    List expectedRows =
      MockStatusAccessor.makeRowsFrom(MockStatusAccessor.makeColumnDescriptorsFrom(colArray3),
				      expectedRowsArray);
    List actualRows = table.getSortedRows();
    assertRowsEqual(expectedRows, actualRows);
  }

  static Object[][] allTablesExpectedColArray =
  {
    {"table_name", "Available Tables",
     Integer.valueOf(ColumnDescriptor.TYPE_STRING)}
  };

  static Object[][] allTablesExpectedRowArray =
  {
    {new StatusTable.Reference("MockStatusAccessor", "A_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "B_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "F_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "Z_table", null)},
  };

  static Object[][] allTablesExpectedRowArrayDebugUser =
  {
    {new StatusTable.Reference("MockStatusAccessor", "A_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "B_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "F_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "Debug_table", null)},
    {new StatusTable.Reference("MockStatusAccessor", "Z_table", null)},
  };

  @Test
  public void registerSomeTables() {
    statusService.registerStatusAccessor("A_table",
					 makeMockStatusAccessor(null));
    statusService.registerStatusAccessor("B_table",
					 makeMockStatusAccessor("B Title"));
    statusService.registerStatusAccessor("F_table",
					 makeMockStatusAccessor("F_table"));
    statusService.registerStatusAccessor("Debug_table",
					 makeMockStatusAccessorDebugOnly("Debug_table"));
    statusService.registerStatusAccessor("Z_table",
					 makeMockStatusAccessor("Z_table"));
  }

  @Test
  public void testGetTableOfAllTables()
      throws StatusService.NoSuchTableException {
    statusService.startService(); //registers table of all tables
    registerSomeTables();

    StatusTable table =
      statusService.getTable(StatusService.ALL_TABLES_TABLE, null);

    assertNotNull(table);
    List expectedCols =
      MockStatusAccessor.makeColumnDescriptorsFrom(allTablesExpectedColArray);
    assertColumnDescriptorsEqual(expectedCols,
				 table.getColumnDescriptors());

    List expectedRows = MockStatusAccessor.makeRowsFrom(expectedCols,
					  allTablesExpectedRowArray);
    assertRowsEqualNoOrder(expectedRows, table.getSortedRows());
  }

  @Test
  public void testGetTableOfAllTablesDebugUser()
      throws StatusService.NoSuchTableException {
    statusService.startService(); //registers table of all tables
    registerSomeTables();

    BitSet tableOptions = new BitSet();
    tableOptions.set(StatusTable.OPTION_DEBUG_USER);
    StatusTable table = new StatusTable(StatusService.ALL_TABLES_TABLE, null);
    table.setOptions(tableOptions);
    statusService.fillInTable(table);

    List expectedCols =
      MockStatusAccessor.makeColumnDescriptorsFrom(allTablesExpectedColArray);
    assertColumnDescriptorsEqual(expectedCols,
				 table.getColumnDescriptors());

    List expectedRows = MockStatusAccessor.makeRowsFrom(expectedCols,
					  allTablesExpectedRowArrayDebugUser);
    assertRowsEqualNoOrder(expectedRows, table.getSortedRows());
  }

  MockStatusAccessor makeMockStatusAccessor(String title) {
    MockStatusAccessor sa = new MockStatusAccessor();
    sa.setTitle(title, null);
    return sa;
  }

  MockStatusAccessor makeMockStatusAccessorDebugOnly(String title) {
    MockStatusAccessor sa = new MockStatusAccessorDebugOnly();
    sa.setTitle(title, null);
    return sa;
  }

  class MockStatusAccessorDebugOnly
    extends MockStatusAccessor implements StatusAccessor.DebugOnly {
  }

  @Test
  public void testGetTableOfAllTablesFiltersTablesThatRequireKeys()
      throws StatusService.NoSuchTableException {
    statusService.startService(); //registers table of all tables
    statusService.registerStatusAccessor("A_table",
					 makeMockStatusAccessor("A_table"));
    statusService.registerStatusAccessor("B_table",
					 makeMockStatusAccessor("B Title"));
    statusService.registerStatusAccessor("F_table",
					 makeMockStatusAccessor("F_table"));
    statusService.registerStatusAccessor("Z_table",
					 makeMockStatusAccessor("Z_table"));

    MockStatusAccessor statusAccessor = new MockStatusAccessor();
    statusAccessor.setRequiresKey(true);
    statusService.registerStatusAccessor("excluded_table", statusAccessor);

    StatusTable table =
      statusService.getTable(StatusService.ALL_TABLES_TABLE, null);

    assertNotNull(table);
    List expectedCols =
      MockStatusAccessor.makeColumnDescriptorsFrom(allTablesExpectedColArray);
    assertColumnDescriptorsEqual(expectedCols,
				 table.getColumnDescriptors());

    List expectedRows = MockStatusAccessor.makeRowsFrom(expectedCols,
					  allTablesExpectedRowArray);
    assertRowsEqualNoOrder(expectedRows, table.getSortedRows());
  }


  private static final Object[][] rowArrayWithNulls = {
    {"AA", "1"},
    {"BB", "2"},
    {null, "3"},
    {"DD", "4"}
  };

  @Test
  public void testNullRowValues()
      throws StatusService.NoSuchTableException {
    String key = "key1";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(colArray1, rowArrayWithNulls,
						key);
    statusAccessor.setDefaultSortRules(sortRules1, key);

    statusService.registerStatusAccessor("table1", statusAccessor);

    StatusTable table = statusService.getTable("table1", key);

    List expectedColumns =
      MockStatusAccessor.makeColumnDescriptorsFrom(colArray1);
    assertColumnDescriptorsEqual(expectedColumns,
				 table.getColumnDescriptors());

    Object[][] expectedRowsArray = new Object[4][];
    expectedRowsArray[0] = rowArrayWithNulls[2];
    expectedRowsArray[1] = rowArrayWithNulls[0];
    expectedRowsArray[2] = rowArrayWithNulls[1];
    expectedRowsArray[3] = rowArrayWithNulls[3];

    List expectedRows =
      MockStatusAccessor.makeRowsFrom(expectedColumns, expectedRowsArray);
    assertRowsEqual(expectedRows, table.getSortedRows());
  }

  private static final Object[][] inetAddrColArray = {
    {"address", "Address", Integer.valueOf(ColumnDescriptor.TYPE_IP_ADDRESS)},
    {"name", "Name", Integer.valueOf(ColumnDescriptor.TYPE_STRING)}
  };

  @Test
  public void testSortsIPAddres()
      throws UnknownHostException, StatusService.NoSuchTableException {
    Object[][] inetAddrRowArray = {
      {IPAddr.getByName("127.0.0.2"), "A"},
      {IPAddr.getByName("127.0.0.1"), "B"},
      {IPAddr.getByName("127.0.0.4"), "C"},
      {IPAddr.getByName("127.0.0.3"), "D"}
    };
    String key = "key1";
    MockStatusAccessor statusAccessor =
      MockStatusAccessor.generateStatusAccessor(inetAddrColArray,
						inetAddrRowArray, key);
    List rules = ListUtil.list(new StatusTable.SortRule("address", true));
    statusAccessor.setDefaultSortRules(rules, key);

    statusService.registerStatusAccessor("table1", statusAccessor);

    StatusTable table = statusService.getTable("table1", key);

    List expectedColumns =
      MockStatusAccessor.makeColumnDescriptorsFrom(inetAddrColArray);

    Object[][] expectedRowsArray = new Object[4][];
    expectedRowsArray[0] = inetAddrRowArray[1];
    expectedRowsArray[1] = inetAddrRowArray[0];
    expectedRowsArray[2] = inetAddrRowArray[3];
    expectedRowsArray[3] = inetAddrRowArray[2];

    List expectedRows =
      MockStatusAccessor.makeRowsFrom(expectedColumns, expectedRowsArray);
    assertRowsEqual(expectedRows, table.getSortedRows());
  }

  private void assertRowsEqual(List expected, List actual) {
    assertEquals("Different number of rows", expected.size(), actual.size());
    Iterator expectedIt = expected.iterator();
    Iterator actualIt = actual.iterator();
    int rowNum=0;
    while (expectedIt.hasNext()) {
      Map expectedMap = (Map)expectedIt.next();
      Map actualMap = (Map)actualIt.next();
      assertEquals("Failed on row "+rowNum, expectedMap, actualMap);
      rowNum++;
    }
  }

  // This should be equivalent to Set.equals(), but when called on two sets
  // of Maps that fails and this succeeds.  What am I missing?
  private void assertRowsEqualNoOrder(Collection expected, Collection actual) {
    assertEquals("Different number of rows", expected.size(), actual.size());
    Iterator expectedIt = expected.iterator();
    while (expectedIt.hasNext()) {
      Map expectedMap = (Map)expectedIt.next();
      assertTrue("missing: " + expectedMap, actual.contains(expectedMap));
    }
  }

  private void assertColumnDescriptorsEqual(List expected, List actual) {
    assertEquals("Lists had different sizes", expected.size(), actual.size());
    Iterator expectedIt = expected.iterator();
    Iterator actualIt = actual.iterator();
    while(expectedIt.hasNext()) {
      ColumnDescriptor expectedCol = (ColumnDescriptor)expectedIt.next();
      ColumnDescriptor actualCol = (ColumnDescriptor)actualIt.next();
      assertEquals(expectedCol.getColumnName(), actualCol.getColumnName());
      assertEquals(expectedCol.getTitle(), actualCol.getTitle());
      assertEquals(expectedCol.getType(), actualCol.getType());
      assertEquals(expectedCol.getFootnote(), actualCol.getFootnote());
    }
    assertFalse(actualIt.hasNext());
  }

  @Test
  public void testGetDefaultTableName() {
    assertEquals(OverviewStatus.OVERVIEW_STATUS_TABLE,
		 statusService.getDefaultTableName());
    ConfigurationUtil.addFromArgs(StatusServiceImpl.PARAM_DEFAULT_TABLE,
				  "other_table_and_chairs");
    assertEquals("other_table_and_chairs",
		 statusService.getDefaultTableName());
  }

  @Test
  public void testRegisterOveriewAccessor() throws Exception {
    setUpJms();
    getMockLockssDaemon().setMyServiceDescr(ServiceDescr.SVC_POLLER);
    ConfigurationUtil.addFromArgs(LockssApp.PARAM_SERVICE_BINDINGS,
				  "poller=:1237:1238");
    // This should cause a RequestTableRegistrations message to be sent
    statusService.startService();
    assertEquals(ListUtil.list(MSG_REQ), nMsgs(cons, 1));
    statusService.registerOverviewAccessor("table1",
					   new OverviewAccessor() {
					     public Object getOverview(String tableName, 
								       BitSet options) {
					       return "over1";
					     }});
    statusService.registerOverviewAccessor("table2",
					   new OverviewAccessor() {
					     public Object getOverview(String tableName, 
								       BitSet options) {
					       return "over2";
					     }});
    assertEquals("over1", statusService.getOverview("table1"));
    assertEquals("over2", statusService.getOverview("table2"));
    assertNull(statusService.getOverview("table3"));

    statusService.registerOverviewAccessor(org.lockss.poller.v3.V3PollStatus.POLLER_STATUS_TABLE_NAME,
					   new OverviewAccessor() {
					     public Object getOverview(String tableName, 
								       BitSet options) {
					       return "Poller Over";
					     }});
    assertEquals(ListUtil.list(MSG_REGOVER_1), nMsgs(cons, 1));


    prod.sendMap(MSG_REQOVER);
    assertEquals(ListUtil.list(MSG_OVER_1), nMsgs(cons, 1));

  }

  Map MSG_REGOVER_1 = MapUtil.map("urlStem", "http://localhost:1238",
				  "verb", "RegisterOverview",
				  "serviceName", "poller",
				  "tableName", "V3PollerTable");

  Map MSG_REQOVER = MapUtil.map("verb", "RequestOverviews");

  Map MSG_OVER_1 = MapUtil.map("verb", "Overview",
			       "urlStem", "http://localhost:1238",
			       "content", "Poller Over",
			       "tableName", "V3PollerTable");


  @Test
  public void testRegisterObjectReferenceAccessor() {
    MockObjectReferenceAccessor refAcc = new MockObjectReferenceAccessor();
    statusService.registerObjectReferenceAccessor("table1", Integer.class,
						  refAcc);
    try {
      // 2nd register should fail
      statusService.registerObjectReferenceAccessor("table1", Integer.class,
						    refAcc);
      fail("Should have thrown after multiple register attempts");
    } catch (StatusService.MultipleRegistrationException re) {
    }
    // should be able to unregister then reregister
    statusService.unregisterObjectReferenceAccessor("table1", Integer.class);
    statusService.registerObjectReferenceAccessor("table1", Integer.class,
						  refAcc);
  }


  private class C1 {}
  private class C2 extends C1 {}

  @Test
  public void testGetReference() {
    MockObjectReferenceAccessor refAcc = new MockObjectReferenceAccessor();
    refAcc.setRef(new StatusTable.Reference("value", "table1", "key"));
    statusService.registerObjectReferenceAccessor("table1", C2.class,
						  refAcc);
    assertNull(statusService.getReference("table2", new C2()));
    assertNull(statusService.getReference("table1", Integer.valueOf(42)));
    assertNull(statusService.getReference("table1", new C1()));
    StatusTable.Reference ref = statusService.getReference("table1", new C2());
    assertNotNull(ref);
    assertEquals("value", ref.getValue());
    assertEquals("table1", ref.getTableName());
  }

  @Test
  public void testSAThrowsAreTrapped() throws
    StatusService.NoSuchTableException {
    StatusAccessor statusAccessor = new StatusAccessor() {
	public String getDisplayName() {
	  return null;
	}
	public void populateTable(StatusTable table) {
	  throw new NullPointerException();
	}
	public boolean requiresKey() {
	  return false;
	}
      };
    statusService.startService(); //registers table of all keyless tables
    statusService.registerStatusAccessor("table1", statusAccessor);
    statusService.getTable("table_of_all_tables", null);
  }

  class MyStatusServiceImpl extends StatusServiceImpl {
    private SimpleBinarySemaphore forRegSem;

    @Override
    void processIncomingTableReg(Map map, String table) {
      super.processIncomingTableReg(map, table);
      if (forRegSem != null) forRegSem.give();
    }
    void setForRegSem(SimpleBinarySemaphore sem) {
      forRegSem = sem;
    }
  }
}
