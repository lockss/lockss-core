/*

 Copyright (c) 2014-2016 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.db;

/**
 * Constants used in SQL code.
 * 
 * @author Fernando García-Loygorri
 */
public class SqlConstants {
  //
  // Database table names.
  //
  /** Name of the version table. */
  public static final String VERSION_TABLE = "version";

  //
  // Database table column names.
  //
  /** System column. */
  public static final String SYSTEM_COLUMN = "system";

  /** Version column. */
  public static final String VERSION_COLUMN = "version";

  /** Subsystem column. */
  public static final String SUBSYSTEM_COLUMN = "subsystem";

  //
  // Maximum lengths of variable text length database columns.
  //
  /** Length of the system column. */
  public static final int MAX_SYSTEM_COLUMN = 16;

  /** Length of the subsystem column. */
  public static final int MAX_SUBSYSTEM_COLUMN = 32;
}
