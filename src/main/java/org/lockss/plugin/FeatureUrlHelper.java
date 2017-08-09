/*

Copyright (c) 2000-2017 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.plugin;

import java.io.IOException;
import java.util.*;

import org.lockss.daemon.*;
import org.lockss.config.*;
import org.lockss.plugin.ArchivalUnit.ConfigurationException;
import org.lockss.util.TypedEntryMap;

/**
 * Interface for plugin auxilliary class to assist with feature URL and
 * access URL resolution.  Set <code>plugin_access_url_factory</code>
 * and/or value in <code>au_feature_urls</code> map to name of factory
 * implementing {@link FeatureUrlHelperFactory}.<br>Plugins should
 * generally extend {@link BaseFeatureUrlHelper} rather than implement this
 * interface in order to retain binary compatibility in case methods are
 * added to the interface. */
public interface FeatureUrlHelper {

  /** Compute the URL(s) to access a feature of a publication.
   * @param au the AU, or null if no matching AU is configured
   * @param plugin the Plugin
   * @param itemType an {@link OpenUrlResolver.OpenUrlInfo#ResolvedTo}
   * describing the type of item desired.  This is the key in the
   * au_feature_urls map that's being tried
   * @param paramMap param map containing properties and AU config params
   *   from TdbAu, plus possibly <code>issn</code>, <code>eissn</code>,
   *   <code>feature_key</code>, <code>volume</code>,
   *   <code>volume_str</code>, <code>volume_name</code>,
   *   <code>year</code>, <code>au_short_year</code>, <code>issue</code>,
   *   <code>article</code>, <code>page</code>, <code>item</code> (article
   *   number).
   * @return a list of URLs that can be used to access the named feature
   * type in the au, or null to indicate that no such URL exists
   */
  public List<String> getFeatureUrls(ArchivalUnit au,
				     OpenUrlResolver.OpenUrlInfo.ResolvedTo itemType,
				     TypedEntryMap paramMap) 
      throws PluginException, IOException;

  /** Compute URL(s) to access an AU.
   * @param au the AU
   * @return a list of URLs that can be used to access (browse) the AU */
  public Collection<String> getAccessUrls(ArchivalUnit au) 
      throws PluginException, IOException;
}
