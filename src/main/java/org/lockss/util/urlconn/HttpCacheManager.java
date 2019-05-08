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
import java.net.*;

import org.lockss.util.*;
import org.lockss.app.*;
import org.lockss.config.*;

/**
 * Manages instances of ClientCacheSpec.
 *
 * Intended to support multiple configurable http client cache
 * configurations.  Currently used only by HTTPConfigFile, which happens
 * before config is loaded, so startup is special.
 *
 * Not a true LockssManager, as is needed by ConfigManager before normal
 * manager startup. */
public class HttpCacheManager {

  private static Logger log = Logger.getLogger();

  static final String PREFIX = Configuration.PREFIX + "httpCache.";

  static final String SUFFIX_CACHEDIR_ = PREFIX + "cacheDir";
  
  private Map<String,ClientCacheSpec> configs = new HashMap<>();
  private File tmpdir;

  public HttpCacheManager(File tmpdir) {
    this.tmpdir = tmpdir;
  }

  public ClientCacheSpec getCacheSpec(String id) {
    ClientCacheSpec res = configs.get(id);
    if (res == null) {
      res = makeDefaultCacheSpec();
      configs.put(id, res);
    }
    return res;
  }
  
  private ClientCacheSpec makeDefaultCacheSpec() {
    ClientCacheSpec res = new ClientCacheSpec()
      .setCacheDir(getDefaultCacheDir());
    return res;
  }

  // MUST EXIST
  File defaultCacheDir = new File(System.getProperty("java.io.tmpdir"));

  File getDefaultCacheDir() {
    return tmpdir;
  }

  public void setConfig(Configuration newConfig,
			Configuration oldConfig,
			Configuration.Differences changedKeys) {

    if (changedKeys.contains(PREFIX)) {
    }
  }

}
