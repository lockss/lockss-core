/*

Copyright (c) 2000-2022 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.plugin.definable;

import java.io.FileNotFoundException;
import java.net.*;
import java.util.*;

import org.apache.commons.lang3.StringEscapeUtils;
import org.htmlparser.*;
import org.htmlparser.tags.TitleTag;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.util.*;
import org.lockss.config.*;
import org.lockss.crawler.FollowLinkCrawler;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.plugin.base.BaseArchivalUnit;
import org.lockss.state.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeUtil;

/**
 * <p>PluginArchivalUnit: The Archival Unit Class for PluginPlugin.
 * This archival unit uses a base url to define an archival unit.
 * @author Seth Morabito
 * @version 1.0
 */

public class NamedArchivalUnit extends DefinableArchivalUnit {
  private static final Logger log = Logger.getLogger();

  public NamedArchivalUnit(Plugin plugin) {
    super(plugin);
  }

  public NamedArchivalUnit(DefinablePlugin myPlugin,
                           ExternalizableMap definitionMap) {
    super(myPlugin, definitionMap);
  }

  // Called by NamedPlugin iff any config below NamedPlugin.PREFIX
  // has changed
//   protected void setConfig(Configuration config,
// 			   Configuration prevConfig,
// 			   Configuration.Differences changedKeys) {
//   }


  public boolean isNamedArchivalUnit() {
    return true;
  }

  protected void setCrawlRelatedParams(Configuration config) {
  }

  /**
   * return a string that points to the plugin registry page.
   * @return a string that points to the plugin registry page for
   * this registry.  This is just the base URL.
   */
  @Override
  public Collection<String> getStartUrls() {
    return Collections.emptyList();
  }

  @Override
  public Collection<String> getPermissionUrls() {
    return Collections.emptyList();
  }

  @Override
  public boolean shouldCrawlForNewContent(AuState aus) {
    return false;
  }

  @Override
  protected CrawlRule makeRule() {
    return null;
  }

  @Override
  public String getCookiePolicy() {
    return null;
  }

  @Override
  public LoginPageChecker getLoginPageChecker() {
    return null;
  }

  @Override
  public int getRefetchDepth() {
    return 0;
  }

  @Override
  public List<PermissionChecker> makePermissionCheckers() {
    return null;
  }
}
