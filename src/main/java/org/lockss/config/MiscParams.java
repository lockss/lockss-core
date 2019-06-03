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

package org.lockss.config;

import java.net.*;
import java.util.regex.*;
import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.util.*;
import org.lockss.plugin.*;

/**
 * Centralized place for miscellaneous that don't belong in any specific
 * manager
 */
public class MiscParams extends BaseLockssManager
 implements ConfigurableManager {

  static Logger log = Logger.getLogger();

  public static final String PREFIX = Configuration.PREFIX;

  /**
   * Regexp matching URLs we never want to collect.  Intended to stop runaway crawls by catching recursive URLS
   */
  public static final String PARAM_EXCLUDE_URL_PATTERN =
      PREFIX + "globallyExcludedUrlPattern";
  static final String DEFAULT_EXCLUDE_URL_PATTERN = null;

  /**
   * Note that these are Apache ORO Patterns, not Java Patterns
   */
  private Pattern globallyExcludedUrlPattern;


  public void startService() {
    super.startService();
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PARAM_EXCLUDE_URL_PATTERN)) {
      setExcludedUrlPattern(config.get(PARAM_EXCLUDE_URL_PATTERN,
				       DEFAULT_EXCLUDE_URL_PATTERN),
			    DEFAULT_EXCLUDE_URL_PATTERN);
    }

  }

  /** Return true if the URL should be excluded from crawls, polls, etc.
   * Intended to be used to stop runaway crawls caused recursive URLs,
   * etc., and to deal with the resulting fallout */
  public boolean isGloballyExcludedUrl(ArchivalUnit au, String url) {
    if (globallyExcludedUrlPattern == null) {
      return false;
    }
    Matcher mat = globallyExcludedUrlPattern.matcher(url);
    return mat.find();
  }

  void setExcludedUrlPattern(String pat, String defaultPat) {
    if (pat != null) {
      int flags = Pattern.CASE_INSENSITIVE;
      try {
	globallyExcludedUrlPattern = Pattern.compile(pat, flags);
        log.info("Global exclude pattern: " + pat);
        return;
      } catch (PatternSyntaxException e) {
        log.error("Illegal global exclude pattern: " + pat, e);
        if (defaultPat != null && !defaultPat.equals(pat)) {
          try {
            globallyExcludedUrlPattern = Pattern.compile(defaultPat, flags);
            log.info("Using default global exclude pattern: " + defaultPat);
            return;
          } catch (PatternSyntaxException e2) {
            log.error("Illegal default global exclude pattern: " + defaultPat,
                e2);
          }
        }
      }
    }
    globallyExcludedUrlPattern = null;
    log.debug("No global exclude pattern");
  }

}
