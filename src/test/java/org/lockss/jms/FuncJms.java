/*
 * Copyright (c) 2018-2019 Board of Trustees of Leland Stanford Jr. University,
 * all rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * Except as contained in this notice, the name of Stanford University shall not
 * be used in advertising or otherwise to promote the sale, use or other dealings
 * in this Software without prior written authorization from Stanford University.
 */

package org.lockss.jms;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.*;
import javax.jms.*;
import org.apache.activemq.broker.BrokerService;
import org.junit.*;

import org.lockss.app.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.jms.*;
import org.lockss.jms.*;

public class FuncJms extends LockssTestCase4 {
  protected static Logger log = Logger.getLogger();

  static String BROKER_URI =
    "vm://localhost?create=false&marshal=true";

  private static BrokerService broker;

  private JmsProducer producerPublishSubscribe,
    producerMultipleConsumers, producerNonDurableConsumer;
  private JmsConsumer consumerPublishSubscribe,
    consumerNoLocal,
    consumer1MultipleConsumers, consumer2MultipleConsumers,
    consumer3MultipleConsumers,
    consumer1NonDurableConsumer,
    consumer2NonDurableConsumer;
  private MyMessageListener listener;
  private SimpleQueue msgQueue;


  MockLockssDaemon daemon;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    log.debug("create broker");
    broker = JMSManager.createBroker(BROKER_URI);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (broker != null) {
      log.debug("stop broker");
      broker.stop();
    }
  }

  JmsFactory getJmsFact() {
    return LockssApp.getManagerByTypeStatic(JMSManager.class).getJmsFactory();
  }

  @Before
  public void setUpObjs() throws Exception {
    // ensure manager doesn't start another broker
    ConfigurationUtil.addFromArgs(JMSManager.PARAM_START_BROKER, "false");
    daemon = getMockLockssDaemon();
    daemon.startManagers(JMSManager.class);

    producerPublishSubscribe =
      getJmsFact().createTopicProducer("producer-publishsubscribe",
				       "publishsubscribe.t");

    producerMultipleConsumers =
      getJmsFact().createTopicProducer("producer-multipleconsumers",
				       "multipleconsumers.t");

    producerNonDurableConsumer =
      getJmsFact().createTopicProducer("producer-nondurableconsumer",
				       "nondurableconsumer.t");

    consumerPublishSubscribe =
      getJmsFact().createTopicConsumer("consumer-publishsubscribe",
				       "publishsubscribe.t");

    consumerNoLocal =
      getJmsFact().createTopicConsumer("consumer-publishsubscribe",
				       "publishsubscribe.t", true, null);

    consumer1MultipleConsumers =
      getJmsFact().createTopicConsumer("consumer1-multipleconsumers",
				       "multipleconsumers.t");

    consumer2MultipleConsumers =
      getJmsFact().createTopicConsumer("consumer2-multipleconsumers",
				       "multipleconsumers.t");

    msgQueue = new SimpleQueue.Fifo();
    listener = new MyMessageListener("listenerConsuemer", msgQueue);

    consumer3MultipleConsumers =
      getJmsFact().createTopicConsumer("consumer3-multipleconsumers",
				       "multipleconsumers.t", listener);

    consumer1NonDurableConsumer =
      getJmsFact().createTopicConsumer("consumer1-nondurableconsumer",
				       "nondurableconsumer.t");

    consumer2NonDurableConsumer =
      getJmsFact().createTopicConsumer("consumer2-nondurableconsumer",
				       "nondurableconsumer.t");
  }

  @After
  public void tearDownObjs() throws Exception {
    producerPublishSubscribe.close();
    producerMultipleConsumers.close();
    producerNonDurableConsumer.close();

    consumerPublishSubscribe.close();
    consumerNoLocal.close();
    consumer1MultipleConsumers.close();
    consumer2MultipleConsumers.close();
    consumer3MultipleConsumers.close();
    consumer1NonDurableConsumer.close();
    consumer2NonDurableConsumer.close();

    daemon.stopManagers();
  }

  @Test
  public void testReceiveText() throws JMSException {
    String textString = "This is a test text string";
    producerPublishSubscribe.sendText(textString);

    String ret1 = consumerPublishSubscribe.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret1);

    assertNull(consumerPublishSubscribe.receiveText(TIMEOUT_SHOULD));
  }

  @Test
  public void testReceiveMap() throws JMSException {
    Map map = MapUtil.map("k1", "v1", "k2", ListUtil.list("l1", 1.7D));
    producerPublishSubscribe.sendMap(map);

    Map ret1 = consumerPublishSubscribe.receiveMap(TIMEOUT_SHOULDNT);
    assertEquals(map, ret1);
    assertNotSame(map, ret1);

    assertNull(consumerPublishSubscribe.receiveText(TIMEOUT_SHOULD));
  }

  static class FooObj {
  }

  @Test
  public void testIllMap() throws JMSException {
    try {
      producerPublishSubscribe.sendMap(MapUtil.map(47.6, "v1"));
      fail("Send map w/ non-string key should throw");
    } catch (IllegalArgumentException e) {
      assertMatchesRE("non-String key", e.getMessage());
    }
    try {
      producerPublishSubscribe.sendMap(MapUtil.map("k1", new FooObj()));
      fail("Send map w/ non-primitive value should throw");
    } catch (MessageFormatException e) {
      assertMatchesRE("but was.*FooObj", e.getMessage());
    }

  }

  // Verify that a consumer created with NoLocal doesn't receive messages
  // sent on the same connection
  @Test
  public void testNoLocal() throws JMSException {
    String textString = "This is a test text string";
    producerPublishSubscribe.sendText(textString);

    String ret1 = consumerPublishSubscribe.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret1);

    assertNull(consumerNoLocal.receiveText(TIMEOUT_SHOULD));
  }

  @Test
  public void testMultipleConsumers() throws JMSException {
    String textString = "This is a test text string";
    producerMultipleConsumers.sendText(textString);

    String ret1 =
      consumer1MultipleConsumers.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret1);

    String ret2 =
      consumer2MultipleConsumers.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret2);
  }

  @Test public void testMessageListener() throws JMSException {
    String textString = "This is a test text string";
    producerMultipleConsumers.sendText(textString);
    String ret1 =
      consumer1MultipleConsumers.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret1);

    String ret2 =
      consumer2MultipleConsumers.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret2);

    String ret3 = (String) msgQueue.get(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret3);
  }

  @Test
  public void testNonDurableConsumer() throws JMSException {
    String testStr1 = "This is a durable test text string";
    String testStr2 = "This is another durable test text string";
    // nondurable subscriptions, will not receive messages sent while
    // the consumers are not active
    consumer2NonDurableConsumer.close();

    producerNonDurableConsumer.sendText(testStr1);

    // recreate a connection for the nondurable subscription
    consumer2NonDurableConsumer =
      getJmsFact().createTopicConsumer("consumer2-nondurableconsumer",
				       "nondurableconsumer.t");

    producerNonDurableConsumer.sendText(testStr2);

    String ret1 =
      consumer1NonDurableConsumer.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(testStr1, ret1);
    String ret2 =
      consumer1NonDurableConsumer.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(testStr2, ret2);

    String ret3 = consumer2NonDurableConsumer.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(testStr2, ret3);
    String ret4 = consumer2NonDurableConsumer.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(null, ret4);
  }

  private static class MyMessageListener
    extends JmsConsumerImpl.SubscriptionListener {

    SimpleQueue queue;

    MyMessageListener(String listenerName, SimpleQueue queue) {
      super(listenerName);
      this.queue = queue;
    }

    @Override
    public void onMessage(Message message) {
      try {
	queue.put(JmsUtil.convertMessage(message));
      } catch (JMSException e) {
        fail("Exception converting message", e);
      }
    }
  }
}
