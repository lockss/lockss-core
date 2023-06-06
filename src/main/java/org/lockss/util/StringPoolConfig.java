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

package org.lockss.util;
import java.util.*;
import org.lockss.log.*;
import org.lockss.config.*;
import org.lockss.plugin.definable.*;

/**
 * Configuration control of StringPool, which moved into lockss-util
 */
public class StringPoolConfig {
  static L4JLogger log = L4JLogger.getLogger();

  static final String PREFIX = Configuration.PREFIX + "stringPool.";

  /** Subtrees of config for individual StringPools.  Currently<ul>

   * <li><tt><i>poolname</i>.mapKeys</tt> - list of keys whose values
   * should be interned</li>

   * <li><tt><i>poolname</i>.keyPattern</tt> regexp of keys whose
   * values should be interned</li>

   * </ul>
   * Defaults are pool-specific; See {@link org.lockss.StringPool} static
   * fields. */
  static final String PARAM_STRING_POOL_NAME_PREFIX = PREFIX + "<poolname>";
  /** Keys whose value should be interned in the named map */
  static final String PARAM_MAP_KEYS = PREFIX + "<poolname>.mapKeys";
  /** Regexp matching keys whose value should be interned in the named map */
  static final String PARAM_KEY_PATTERN = PREFIX + "<poolname>.keyPattern";

  static final String SUFFIX_MAP_KEYS = "mapKeys";
  static final String SUFFIX_KEY_PATTERN = "keyPattern";

  // Somewhat awkward mechanism to set default for various keys in the
  // subtree below org.lockss.stringPool
  static Configuration defaultPoolsTree = ConfigManager.newConfiguration();
  static {
    defaultPoolsTree.put(StringPool.TDBAU_ATTRS.getName() + "." + SUFFIX_KEY_PATTERN,
                         ".*" + DefinableArchivalUnit.SUFFIX_HASH_FILTER_FACTORY + "$");
  }

  private StringPoolConfig() {
  }

  /** Called by org.lockss.config.MiscConfig
   */
  public static void setConfig(Configuration config,
                               Configuration oldConfig,
                               Configuration.Differences diffs) {
    if (diffs.contains(PREFIX)) {
      // Make a copy of the defaults and copy the acntual config tree into it
      Configuration poolsTree = defaultPoolsTree.copy();
      poolsTree.copyFrom(config.getConfigTree(PREFIX));
      for (Iterator<String> iter = poolsTree.nodeIterator(); iter.hasNext(); ) {
        String poolName = iter.next();
        Configuration poolTree = poolsTree.getConfigTree(poolName);
        StringPool.PoolConfig poolConfig = new StringPool.PoolConfig()
          .setMapKeys(poolTree.getList(SUFFIX_MAP_KEYS,
                                       Collections.emptyList()))
          .setKeyPattern(poolTree.get(SUFFIX_KEY_PATTERN));
        StringPool.setPoolConfig(poolName, poolConfig);
      }
    }
  }

}
