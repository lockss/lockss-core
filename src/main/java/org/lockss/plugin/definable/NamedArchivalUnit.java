/*

Copyright (c) 2000-2023 Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeUtil;

/**
 * A NamedArchivalUnit is defined solely by a user-supplied identifier
 * (handle), not by definitional params (eg., base_url), which
 * determine its contents and allow it to be crawled.
 */
public class NamedArchivalUnit extends DefinableArchivalUnit {
  private static final L4JLogger log = L4JLogger.getLogger();

  private boolean isCrawled = false;

  /** The name of the standard NamedPlugin. */
  // In there a better place for this?
  public static final String NAMED_PLUGIN_NAME = "org.lockss.plugin.NamedPlugin";

  public static final String AU_PARAM_FEATURES = "features";
  public static final String FEATURE_CRAWLED = "crawledAu";
  public static final String AU_PARAM_START_URLS = "start_urls";
  public static final String AU_PARAM_URL_STEMS = "url_stems";

  public NamedArchivalUnit(Plugin plugin) {
    super(plugin);
  }

  public NamedArchivalUnit(DefinablePlugin myPlugin,
                           ExternalizableMap definitionMap) {
    super(myPlugin, definitionMap);
  }

  public boolean isNamedArchivalUnit() {
    return true;
  }

  public boolean isCrawlable() {
    return isCrawled;
  }

  protected void setAdditionalParams(Configuration config)
      throws ConfigurationException {
    List<String> features = config.getList(AU_PARAM_FEATURES);
    isCrawled = features.contains(FEATURE_CRAWLED);
  }

  /** Suppress all crawl-related params */
  protected void setCrawlRelatedParams(Configuration config) {
  }

  /** Return non-def param with start URLs, or empty */
  @Override
  public Collection<String> getStartUrls() {
    Configuration config = getConfiguration();
    return config.getList(AU_PARAM_START_URLS, Collections.emptyList());
  }

  /** Return URL stems derived from start URLs and non-def url_stems,
   * or empty */
  @Override
  public Collection<String> getUrlStems() {
    Configuration config = getConfiguration();
    Set<String> res = new HashSet<>();
    try {
      for (String url : getStartUrls()) {
        res.add(UrlUtil.getUrlPrefix(url));
      }
      for (String url : (List<String>)config.getList(AU_PARAM_URL_STEMS,
                                                     Collections.emptyList())) {
        res.add(url);
      }
      return res;
    } catch (MalformedURLException e) {
      log.error("getUrlStems(" + getName() + ")", e);
      // XXX should throw
      return Collections.emptyList();
    }
  }

  /** Empty */
  @Override
  public Collection<String> getPermissionUrls() {
    return Collections.emptyList();
  }

  /** Never crawl */
  @Override
  public boolean shouldCrawlForNewContent(AuState aus) {
    if (isCrawled) {
      return super.shouldCrawlForNewContent(aus);
    } else {
      return false;
    }
  }
}
