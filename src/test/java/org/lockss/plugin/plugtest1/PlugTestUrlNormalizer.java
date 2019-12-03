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

package org.lockss.plugin.plugtest1;

import java.io.*;

import org.lockss.daemon.PluginException;
import org.lockss.plugin.*;
import org.lockss.util.*;
import org.lockss.log.*;

/** Not really a UrlNormalizer, just a simple means of testing that the
 * contents of the plugin's dependency jar are accessible on the classpath.
 * Loads strings from two resources and returns their concatenation */
public class PlugTestUrlNormalizer implements UrlNormalizer {
  private static L4JLogger log = L4JLogger.getLogger();

  final String TOPLEVEL_RESOURCE = "/toplevel_resource.txt";
  final String PACKAGED_RESOURCE = "/org/lockss/pkgpkg/pkgd_resource.txt";

  public String normalizeUrl(String url, ArchivalUnit au) {
    return readres(TOPLEVEL_RESOURCE) + readres(PACKAGED_RESOURCE);
  }

  String readres(String res) {
    logres(res);
    logres(res.substring(1));
    try (InputStream is = this.getClass().getResourceAsStream(res)) {
      return StringUtil.fromInputStream(is);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void logres(String res) {
    log.debug("thread: " +
	      Thread.currentThread().getContextClassLoader().getResource(res));
    log.debug("class: " + this.getClass().getResource(res));
    log.debug("classloader: " +
	      this.getClass().getClassLoader().getResource(res));

  }
}
