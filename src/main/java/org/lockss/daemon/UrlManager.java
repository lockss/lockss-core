/*
 * $Id$
 */

/*

Copyright (c) 2000-2005 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.daemon;

import java.io.*;
import java.net.*;
import java.lang.reflect.Field;

import org.lockss.util.*;
import org.lockss.app.*;

/**
 * UrlManager does one-time-only URLStreamHandlerFactory initialization.
 * A URLStreamHandlerFactory can be installed only once in a JVM, so don't
 * die just because the service is stopped and restarted.  (Which happens
 * during unit testing.)
 */

public class UrlManager extends BaseLockssManager {
  public static final String PROTOCOL_CU = "locksscu";
  public static final String PROTOCOL_AU = "lockssau";
  public static final String PROTOCOL_RESOURCE = "resource";

  private static Logger log = Logger.getLogger();

  /** Install the URLStreamHandlerFactory */
  public void startService() {
    setOrAddUrlFactory();
  }

  public void stopService() {
  }

  private void setOrAddUrlFactory() {
    try {
      Field field = URL.class.getDeclaredField("factory");
      field.setAccessible(true);
      final URLStreamHandlerFactory currentFactory =
	(URLStreamHandlerFactory)field.get(null);
      if (currentFactory != null) {
	log.debug("old fact: " + currentFactory);
	if (currentFactory instanceof LockssUrlFactory) {
	  return;
	}
	field.set(null, null);
      }

      URLStreamHandlerFactory fact = new LockssUrlFactory(currentFactory);
      URL.setURLStreamHandlerFactory(fact);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      log.error("Setting URLStreamHandlerFactory", e);
    }
  }

  /** A URLStreamHandlerFactory that returns URLStreamHandlers for
      locksscu: and lockssau: protocols. */
  private static class LockssUrlFactory implements URLStreamHandlerFactory {
    private URLStreamHandlerFactory wrapFact;

    public LockssUrlFactory(URLStreamHandlerFactory wrapFact) {
      this.wrapFact = wrapFact;
    }

    public URLStreamHandler createURLStreamHandler(String protocol) {
      if (PROTOCOL_CU.equalsIgnoreCase(protocol)) {
	// locksscu: gets a CuUrlConnection
	return new URLStreamHandler() {
	    protected URLConnection openConnection(URL u) throws IOException {
	      // passing pluginManager runs into problems with class loaders
	      // when running unit tests
// 	      return new CuUrl.CuUrlConnection(u, pluginManager);
	      return new CuUrl.CuUrlConnection(u);
	    }};
      }
      if (PROTOCOL_AU.equalsIgnoreCase(protocol)) {
	// AuUrls are never opened.
	return new URLStreamHandler() {
	    protected URLConnection openConnection(URL u) throws IOException {
	      return null;
	    }};
      }
      if (PROTOCOL_RESOURCE.equalsIgnoreCase(protocol)) {
	return new URLStreamHandler() {
	    protected URLConnection openConnection(URL u) throws IOException {
	      return new ResourceURLConnection(u);
	    }};
      }
      if (wrapFact != null) {
	return wrapFact.createURLStreamHandler(protocol);
      }
      return null;	 // use default stream handlers for other protocols
    }
  }
}