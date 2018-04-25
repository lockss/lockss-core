/*
 * Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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

import javax.jms.*;
import org.apache.activemq.broker.BrokerService;
import org.junit.*;

import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.jms.*;

public class FuncJms extends LockssTestCase4 {
  protected static Logger log = Logger.getLogger("FuncJMS");

  private static BrokerService broker;

  private Producer producerPublishSubscribe,
      producerMultipleConsumers, producerNonDurableConsumer;
  private Consumer consumerPublishSubscribe,
      consumer1MultipleConsumers, consumer2MultipleConsumers,
      consumer3MultipleConsumers,
      consumer1NonDurableConsumer,
      consumer2NonDurableConsumer;
  private MyMessageListener listener;
  private SimpleQueue msgQueue;


  MockLockssDaemon daemon;
  JMSManager mgr;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    log.debug("create broker");
    broker = JMSManager.createBroker(JMSManager.DEFAULT_BROKER_URI);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (broker != null) {
      log.debug("stop broker");
      broker.stop();
    }
  }

  @Before
  public void setUpObjs() throws Exception {

    producerPublishSubscribe =
      Producer.createTopicProducer("producer-publishsubscribe",
				     "publishsubscribe.t");

    producerMultipleConsumers =
      Producer.createTopicProducer("producer-multipleconsumers",
				     "multipleconsumers.t");

    producerNonDurableConsumer =
      Producer.createTopicProducer("producer-nondurableconsumer",
				     "nondurableconsumer.t");

    consumerPublishSubscribe =
      Consumer.createTopicConsumer("consumer-publishsubscribe",
				       "publishsubscribe.t");

    consumer1MultipleConsumers =
      Consumer.createTopicConsumer("consumer1-multipleconsumers",
				       "multipleconsumers.t");

    consumer2MultipleConsumers =
      Consumer.createTopicConsumer("consumer2-multipleconsumers",
				       "multipleconsumers.t");

    msgQueue = new SimpleQueue.Fifo();
    listener = new MyMessageListener("listenerConsuemer", msgQueue);

    consumer3MultipleConsumers =
      Consumer.createTopicConsumer("consumer3-multipleconsumers",
				       "multipleconsumers.t", listener);

    consumer1NonDurableConsumer =
      Consumer.createTopicConsumer("consumer1-nondurableconsumer",
				       "nondurableconsumer.t");

    consumer2NonDurableConsumer =
      Consumer.createTopicConsumer("consumer2-nondurableconsumer",
				       "nondurableconsumer.t");
  }

  @After
  public void tearDownObjs() throws Exception {
    producerPublishSubscribe.closeConnection();
    producerMultipleConsumers.closeConnection();
    producerNonDurableConsumer.closeConnection();

    consumerPublishSubscribe.closeConnection();
    consumer1MultipleConsumers.closeConnection();
    consumer2MultipleConsumers.closeConnection();
    consumer3MultipleConsumers.closeConnection();
    consumer1NonDurableConsumer.closeConnection();
    consumer2NonDurableConsumer.closeConnection();
  }

  @Test
  public void testReceiveText() throws JMSException {
    String textString = "This is a test text string";
    producerPublishSubscribe.sendText(textString);

    String ret1 = consumerPublishSubscribe.receiveText(TIMEOUT_SHOULDNT);
    assertEquals(textString, ret1);

    String ret2 = consumerPublishSubscribe.receiveText(TIMEOUT_SHOULDNT);
    assertNull(ret2);
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
    consumer2NonDurableConsumer.closeConnection();

    producerNonDurableConsumer.sendText(testStr1);

    // recreate a connection for the nondurable subscription
    consumer2NonDurableConsumer =
      Consumer.createTopicConsumer("consumer2-nondurableconsumer",
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
    extends Consumer.SubscriptionListener {

    SimpleQueue queue;

    MyMessageListener(String listenerName, SimpleQueue queue) {
      super(listenerName);
      this.queue = queue;
    }

    @Override
    public void onMessage(Message message) {
      try {
	queue.put(Consumer.convertMessage(message));
      } catch (JMSException e) {
        fail("Exception converting message", e);
      }
    }
  }
}
