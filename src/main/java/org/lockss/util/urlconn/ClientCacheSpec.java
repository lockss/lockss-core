/*
Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.util.urlconn;

import java.io.*;
import java.util.*;
import org.apache.http.client.cache.*;
import org.apache.http.impl.client.*;
import org.apache.http.impl.client.cache.*;

import org.lockss.config.*;
import org.lockss.util.*;
import org.lockss.log.*;

/** Encapsulates all the info needed by HttpClientUrlConnection to set up a
 * client cache, and holds the HttpCacheStorage instance.  Currently mostly
 * hardwired for HTTPConfigFile, intended to be made more flexible if/when
 * other uses arise.
 */
public class ClientCacheSpec {
  public static L4JLogger log = L4JLogger.getLogger();

  File cacheDir = null;
  HttpCacheStorage cacheStorage;
  ResourceFactory resourceFact;
  CacheConfig cacheConfig;


  CacheConfig getCacheConfig() {
    return CacheConfig.custom()
      .setMaxCacheEntries(100)
      .setMaxObjectSize(1000 * 1000 * 1000)
      // cache responses even if no headers saying that's ok
      .setHeuristicCachingEnabled(true)
      .setHeuristicDefaultLifetime(Constants.HOUR)
//       .setSharedCache(false)
      // Don't do background cache revalidation
      .setAsynchronousWorkersMax(0)
      .build();
  }
  
  public ClientCacheSpec setCacheDir(File dir) {
    cacheDir = dir;
    return this;
  }
  
  public ClientCacheSpec setCacheStorage(HttpCacheStorage storage) {
    cacheStorage = storage;
    return this;
  }

  public ClientCacheSpec setResourceFactory(ResourceFactory fact) {
    resourceFact = fact;
    return this;
  }

  public File getCacheDir() {
    return cacheDir;
  }
  
  public HttpCacheStorage getCacheStorage() {
    if (cacheStorage == null) {
      cacheStorage = new ManagedHttpCacheStorage(getCacheConfig());
    }
    return cacheStorage;
  }

  public ResourceFactory getResourceFactory() {
    return resourceFact;
  }

}
