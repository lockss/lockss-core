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

import javax.jms.JMSException;

import javax.jms.Message;
import org.apache.activemq.broker.BrokerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.lockss.jms.Publisher;
import org.lockss.jms.Subscriber;

public class TestSubscriber {
  private static BrokerService broker;
  private static Publisher publisherPublishSubscribe,
      publisherMultipleConsumers, publisherNonDurableSubscriber;
  private static Subscriber subscriberPublishSubscribe,
      subscriber1MultipleConsumers, subscriber2MultipleConsumers,
      subscriber3MultipleConsumers,
      subscriber1NonDurableSubscriber,
      subscriber2NonDurableSubscriber;
  private static MyMessageListener listener;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    broker = new BrokerService();
// configure the broker
    broker.addConnector("tcp://localhost:61616");
    broker.start();

    publisherPublishSubscribe = new Publisher();
    publisherPublishSubscribe.create("publisher-publishsubscribe",
        "publishsubscribe.t");

    publisherMultipleConsumers = new Publisher();
    publisherMultipleConsumers.create("publisher-multipleconsumers",
        "multipleconsumers.t");

    publisherNonDurableSubscriber = new Publisher();
    publisherNonDurableSubscriber.create(
        "publisher-nondurablesubscriber", "nondurablesubscriber.t");

    subscriberPublishSubscribe = new Subscriber();
    subscriberPublishSubscribe.create("subscriber-publishsubscribe",
        "publishsubscribe.t");

    subscriber1MultipleConsumers = new Subscriber();
    subscriber1MultipleConsumers.create(
        "subscriber1-multipleconsumers", "multipleconsumers.t");

    subscriber2MultipleConsumers = new Subscriber();
    subscriber2MultipleConsumers.create(
        "subscriber2-multipleconsumers", "multipleconsumers.t");

    subscriber3MultipleConsumers = new Subscriber();
    listener = new MyMessageListener("listenerConsuemer");
    subscriber3MultipleConsumers.create(
        "subscriber3-multipleconsumers", "multipleconsumers.t", listener);

    subscriber1NonDurableSubscriber = new Subscriber();
    subscriber1NonDurableSubscriber.create(
        "subscriber1-nondurablesubscriber", "nondurablesubscriber.t");

    subscriber2NonDurableSubscriber = new Subscriber();
    subscriber2NonDurableSubscriber.create(
        "subscriber2-nondurablesubscriber", "nondurablesubscriber.t");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    publisherPublishSubscribe.closeConnection();
    publisherMultipleConsumers.closeConnection();
    publisherNonDurableSubscriber.closeConnection();

    subscriberPublishSubscribe.closeConnection();
    subscriber1MultipleConsumers.closeConnection();
    subscriber2MultipleConsumers.closeConnection();
    subscriber3MultipleConsumers.closeConnection();
    subscriber1NonDurableSubscriber.closeConnection();
    subscriber2NonDurableSubscriber.closeConnection();
    broker.stop();
  }

  @Test
  public void testReceiveText() {
    String textString = "This is a test text string";
    try {
      publisherPublishSubscribe.sendText(textString);

      String ret1 = subscriberPublishSubscribe.receiveText(1000);
      assertEquals(textString, ret1);

      String ret2 = subscriberPublishSubscribe.receiveText(1000);
      assertNull(ret2);

    } catch (JMSException e) {
      fail("a JMS Exception occurred");
    }

  }

  @Test
  public void testMultipleConsumers() {
    String textString = "This is a test text string";
    try {
      publisherMultipleConsumers.sendText(textString);

      String ret1 =
          subscriber1MultipleConsumers.receiveText(1000);
      assertEquals(textString, ret1);

      String ret2 =
          subscriber2MultipleConsumers.receiveText(1000);
      assertEquals(textString, ret2);

    } catch (JMSException e) {
      fail("a JMS Exception occurred");
    }
  }

  @Test public void testMessageListener() {
    String textString = "This is a test text string";
    try {
      publisherMultipleConsumers.sendText(textString);
      String ret1 =
          subscriber1MultipleConsumers.receiveText(1000);
      assertEquals(textString, ret1);

      String ret2 =
          subscriber2MultipleConsumers.receiveText(1000);
      assertEquals(textString, ret2);

      String ret3 = (String) listener.getMsgObject();
      assertEquals(textString, ret3);
    }
    catch (JMSException e) {
      fail("a JMS Exception occurred");
    }
  }

  @Test
  public void testNonDurableSubscriber() {
    String testStr1 = "This is a durable test text string";
    String testStr2 = "This is another durable test text string";
    try {
      // nondurable subscriptions, will not receive messages sent while
      // the subscribers are not active
      subscriber2NonDurableSubscriber.closeConnection();

      publisherNonDurableSubscriber.sendText(testStr1);

      // recreate a connection for the nondurable subscription
      subscriber2NonDurableSubscriber.create(
          "subscriber2-nondurablesubscriber",
          "nondurablesubscriber.t");

      publisherNonDurableSubscriber.sendText(testStr2);

      String ret1 =
          subscriber1NonDurableSubscriber.receiveText(1000);
      assertEquals(testStr1, ret1);
      String ret2 =
          subscriber1NonDurableSubscriber.receiveText(1000);
      assertEquals(testStr2, ret2);

      String ret3 =
          subscriber2NonDurableSubscriber.receiveText(1000);
      assertEquals(testStr2, ret3);
      String ret4 =
          subscriber2NonDurableSubscriber.receiveText(1000);
      assertEquals(null, ret4);

    } catch (JMSException e) {
      fail("a JMS Exception occurred");
    }
  }

  private static class MyMessageListener extends Subscriber.SubscriptionListener {

    MyMessageListener(String listenerName) {
      super(listenerName);
    }

    @Override
    public void onMessage(Message message) {
      try {
        msgObject =  Subscriber.convertMessage(message);
      }
      catch (JMSException e) {
        fail("a JMS Exception occurred.");
     }
    }

  }
}
