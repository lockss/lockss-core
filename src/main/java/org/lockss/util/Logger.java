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

package org.lockss.util;
import java.util.*;
import java.text.Format;

import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.logging.log4j.*;
import org.apache.logging.log4j.util.StackLocatorUtil;

import org.lockss.log.L4JLogger;
import org.lockss.config.*;

/**
 * Makes org.lockss.log.Logger (in lockss-util) available the
 * org.lockss.util package, for compatability with existing LOCKSS code and
 * plugins, and hooks it into the LOCKSS configuration mechanism
 */
public class Logger extends org.lockss.log.LockssLogger {

  protected Logger(L4JLogger log) {
    super(log);
  }

  /**
   * Logger factory.  Return the unique instance
   * of <code>Logger</code> with the given name, creating it if necessary.
   * @param name identifies the log instance, appears in output
   */
  public static Logger getLogger(String name) {
    return getWrappedLogger(name);
  }
  
  /**
   * Convenience method to name a logger after a class.
   * Simply calls {@link #getLogger(String)} with the result of
   * {@link Class#getName()}.
   * @param clazz The class after which to name the returned logger.
   * @return A logger named after the given class.
   * @since 1.56
   */
  public static Logger getLogger(Class<?> clazz) {
    return getLogger(clazz.getName());
  }

  /**
   * Logger factory.  Return the unique instance
   * of <code>Logger</code> with the given name, creating it if necessary.
   * @param name identifies the log instance, appears in output
   */
  public static Logger getLogger() {
    return getLogger(StackLocatorUtil.getCallerClass(2));
  }

  /**
   * Delegate to org.lockss.log.Logger to return or create a Logger that's
   * an instance of org.lockss.util.Logger
   */
  protected static Logger getWrappedLogger(String name) {
    return (Logger)getWrappedLogger(name,
				    s -> new Logger(L4JLogger.getLogger(s)));
  }

  /**
   * Special purpose Logger factory.  Return the unique instance
   * of <code>Logger</code> with the given name, creating it if necessary.
   * This is here primarily so <code>Configuration</code> can create a
   * log without being invoked recursively, which causes its class
   * initialization to not complete correctly.
   * @param name identifies the log instance, appears in output
   * @param initialLevel the initial log level (<code>Logger.LEVEL_XXX</code>).
   * @deprecated Use {@link #getLogger(String)}
   */
  @Deprecated
  public static Logger getLoggerWithInitialLevel(String name,
						 int initialLevel) {
    return getLogger(name);
  }

  /**
   * Special purpose Logger factory.  Return the <code>Logger</code> with
   * the given name, establishing its initial default level, and the
   * configuration parameter name of its default level.  This is primarily
   * intended to be used by classes that implement a foreign logging
   * interface in terms of this logger.  Because such interfaces
   * potentially support logging from a large number of classes not
   * directly related to LOCKSS, it is useful to be able to establish
   * defaults that are distinct from the normal system-wide defaults.
   * @param name identifies the log instance, appears in output
   * @param defaultLevelName the default log level if no config param is
   * present specifying the level or the default level
   * @param defaultLevelParam the name of the config param specifying the
   * default level.
   * @deprecated No longer relevant; use {@link #getLogger(String)}
   */
  @Deprecated
  public static Logger getLoggerWithDefaultLevel(String name,
						 String defaultLevelName,
						 String defaultLevelParam) {
    return getLogger(name);
  }

  /** Get the initial default log level, specified by the
   * org.lockss.defaultLogLevel system property if present, or LEVEL_INFO
   * @deprecated Unnecessary; use {@link #getLogger(String)}
   */
  @Deprecated
  public static int getInitialDefaultLevel() {
    String s = System.getProperty(SYSPROP_DEFAULT_LOG_LEVEL);
    int l = LEVEL_INFO;
    if (s != null && !"".equals(s)) {
      try {
	l = levelOf(s);
      } catch (IllegalLevelException e) {
	// no action
      }
    }
    return l;
  }

  /** Called by MiscConfig */
  public static void setConfig(Configuration newConfig,
			       Configuration oldConfig,
			       Configuration.Differences diffs) {
    if (diffs.contains(PREFIX)) {
      setAllLogLevels(newConfig);
    }
  }

  /** Extract the logging related params from the config and hand them to
   * Logger */
  private static void setAllLogLevels(Configuration config) {
    Map<String,String> map = new HashMap<>();
    Configuration logConfig = config.getConfigTree(PREFIX);
    for (String key : config.keySet()) {
      if (key.startsWith(PREFIX)) {
	map.put(key, config.get(key));
      }
    }
    // Ensure default values for these get stored in map if not present in
    // config
    map.put(PARAM_STACKTRACE_LEVEL, config.get(PARAM_STACKTRACE_LEVEL,
					       DEFAULT_STACKTRACE_LEVEL));
    map.put(PARAM_STACKTRACE_SEVERITY, config.get(PARAM_STACKTRACE_SEVERITY,
						  DEFAULT_STACKTRACE_SEVERITY));
    Logger.setLockssConfig(map);
  }

}
