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

package org.lockss.doconly;

import org.lockss.util.*;

/**
 * Config params duplicated for ParamDoc purposes from
 * lockss-util:org.lockss.log.LockssLogger
 */
public class LockssLogger {
  public static final String PREFIX = "org.lockss." + "log.";

  // Documentation only
  /** Sets the log level of the named logger */
  static final String PARAM_LOG_LEVEL = PREFIX + "<logname>.level";

  /** Log level (numeric) at which stack traces will be included */
  public static final String PARAM_STACKTRACE_LEVEL = PREFIX + "stackTraceLevel";
  public static final String DEFAULT_STACKTRACE_LEVEL = "debug";

  /** Log severity (numeric) for which stack traces will be included no
   * matter what the current log level */
  public static final String PARAM_STACKTRACE_SEVERITY =
    PREFIX + "stackTraceSeverity";
  public static final String DEFAULT_STACKTRACE_SEVERITY = "error";

  /** Not supported, kept so can log error if used */
  static final String PARAM_LOG_TARGETS = PREFIX + "targets";

}
