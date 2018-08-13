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

import org.apache.logging.log4j.*;
import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import org.lockss.config.*;

/**
 * Compatability layer for lockss-core and plugins, forwards to new
 * org.lockss.log.Logger in lockss-util.  Supports obtaining and using
 * loggers, setting log levels with config.  Does not include support for
 * setting targets or LogTarget class hierarchy.
 */
public class Logger extends org.lockss.log.Logger {

  /** Experimental for use in unit tests */
  public static void resetLogs() {
//     logs = new HashMap<String, Logger>();
  }

  protected Logger(org.apache.logging.log4j.Logger log) {
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
   * <p>Convenience method to name a logger after a class.
   * Simply calls {@link #getLogger(String)} with the result of
   * {@link Class#getSimpleName()}.</p>
   * @param clazz The class after which to name the returned logger.
   * @return A logger named after the given class.
   * @since 1.56
   */
  public static Logger getLogger(Class<?> clazz) {
    return getLogger(clazz.getSimpleName());
  }

  /**
   * Special purpose Logger factory.  Return the unique instance
   * of <code>Logger</code> with the given name, creating it if necessary.
   * This is here primarily so <code>Configuration</code> can create a
   * log without being invoked recursively, which causes its class
   * initialization to not complete correctly.
   * @param name identifies the log instance, appears in output
   * @param initialLevel the initial log level (<code>Logger.LEVEL_XXX</code>).
   */
  protected static Logger getWrappedLogger(String name) {
    return (Logger)getWrappedLogger(name, (s) -> new Logger(LogManager.getLogger(s)));
  }

  /**
   * Special purpose Logger factory.  Return the unique instance
   * of <code>Logger</code> with the given name, creating it if necessary.
   * This is here primarily so <code>Configuration</code> can create a
   * log without being invoked recursively, which causes its class
   * initialization to not complete correctly.
   * @param name identifies the log instance, appears in output
   * @param initialLevel the initial log level (<code>Logger.LEVEL_XXX</code>).
   */
  public static Logger getLoggerWithInitialLevel(String name,
						 int initialLevel) {
    return getLogger(name);
//     // This method MUST NOT make any reference to Configuration !!
//     if (name == null) {
//       name = genName();
//     }
//     Logger l = (Logger)logs.get(name);
//     if (l == null) {
//       l = new Logger(initialLevel, name);
//       if (myLog != null) myLog.debug2("Creating logger: " + name);
//       logs.put(name, l);
//     }
//     return l;
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
   */
  public static Logger getLoggerWithDefaultLevel(String name,
						 String defaultLevelName,
						 String defaultLevelParam) {
    return getLogger(name);
//     deferredInit();
//     if (name == null) {
//       name = genName();
//     }
//     Logger l = (Logger)logs.get(name);
//     if (l == null) {
//       int defaultLevel = globalDefaultLevel;
//       try {
// 	defaultLevel = levelOf(defaultLevelName);
//       } catch (Exception e) {
//       }
//       l = new Logger(defaultLevel, name, defaultLevelParam);
//       if (myLog != null) myLog.debug2("Creating logger: " + name);
//       l.setLevel(l.getConfiguredLevel());
//       logs.put(name, l);
//     }
//     return l;
  }

//   public static Format getTimeStampFormat() {
//     return timestampDf;
//   }


  /** Get the initial default log level, specified by the
   * org.lockss.defaultLogLevel system property if present, or DEFAULT_LEVEL
   */
  public static int getInitialDefaultLevel() {
    return LEVEL_INFO;
//     String s = System.getProperty(SYSPROP_DEFAULT_LOG_LEVEL);
//     int l = DEFAULT_LEVEL;
//     if (s != null && !"".equals(s)) {
//       try {
// 	l = levelOf(s);
//       } catch (IllegalArgumentException e) {
// 	// no action
//       }
//     }
//     return l;
  }


  public static void setConfig(Configuration newConfig,
			       Configuration oldConfig,
			       Configuration.Differences diffs) {
    if (diffs.contains(PREFIX)) {
      setAllLogLevels(newConfig);
//       if (diffs.contains(PARAM_LOG_TARGETS)) {
// 	setLogTargets();
//       }
//       paramStackTraceLevel = newConfig.getInt(PARAM_STACKTRACE_LEVEL,
// 					      DEFAULT_STACKTRACE_LEVEL);
//       paramStackTraceSeverity =
// 	newConfig.getInt(PARAM_STACKTRACE_SEVERITY,
// 			 DEFAULT_STACKTRACE_SEVERITY);

//       String df = newConfig.get(PARAM_TIMESTAMP_DATEFORMAT,
// 				DEFAULT_TIMESTAMP_DATEFORMAT);

//       try {
// 	timestampDf = FastDateFormat.getInstance(df);
//       } catch (IllegalArgumentException e) {
// 	timestampDf =
// 	  FastDateFormat.getInstance(DEFAULT_TIMESTAMP_DATEFORMAT);
// 	myLog.warning("Invalid DataFormat: " + df + ", using default");
//       }
    }
  }

  /** Extract the logging related params from the config and hand them to
   * Logger */
  private static void setAllLogLevels(Configuration config) {
    Map<String,String> map = new HashMap<>();
    Configuration logConfig = config.getConfigTree(PREFIX);
    myLog.critical("logConfig: " + logConfig);
    for (String key : config.keySet()) {
      map.put(key, config.get(key));
    }
    Logger.setLockssConfig(map);
  }

//   /**
//    * Set minimum severity level logged by this log
//    * @param level <code>Logger.LEVEL_XXX</code>
//    */
//   public void setLevel(int level) {
//     if (this.level != level) {
// //        info("Changing log level to " + nameOf(level));
//       this.level = level;
//     }
//   }


}
