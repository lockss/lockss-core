/*

 Copyright (c) 2017 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.Test;
import org.lockss.util.StreamUtil;

/**
 * Test class for org.lockss.test.SpringLockssTestCase
 */
public class TestSpringLockssTestCase extends SpringLockssTestCase {

  /**
   * Set up code to be run before each test.
   */
  @Before
  public void setUpBeforeEachTest() throws IOException {
    setUpTempDirectory("TestSpringLockssTestCase");
  }

  /**
   * Tests of the temporary directory path.
   */
  @Test
  public void testTempDirectory() {
    assertTrue(getTempDirPath().indexOf(File.separator
	+ "TestSpringLockssTestCase") > -1);
    assertTrue(getTempDirPath().endsWith(".tmp"));
  }

  /**
   * Tests of the platform disk space configuration file path.
   */
  @Test
  public void testPlatformDiskSpaceConfigPath() {
    assertTrue(getPlatformDiskSpaceConfigPath().startsWith(getTempDirPath()
	+ File.separator));
    assertTrue(getPlatformDiskSpaceConfigPath().endsWith(File.separator
	+ "platform.opt"));
  }

  /**
   * Tests of copying files/directories to the temporary directory.
   */
  @Test
  public void testCopyToTempDir() throws IOException {
    File fileA = null;

    try {
      fileA = File.createTempFile("fileA", "fileA");
      System.out.println(fileA.getAbsolutePath());
      FileOutputStream fos = new FileOutputStream(fileA);
      InputStream sis = new StringInputStream("some content");
      StreamUtil.copy(sis, fos);
      sis.close();
      fos.close();

      copyToTempDir(fileA);

      File fileACopy = new File(new File(getTempDirPath()), fileA.getName());
      assertTrue(fileACopy.exists());
      assertTrue(fileACopy.isFile());
    } finally {
      boolean deleted = fileA.delete();
      assertTrue(deleted);
    }

    File dirB = null;

    try {
      dirB = Files.createTempDirectory("dirB").toFile();
      System.out.println(dirB.getAbsolutePath());

      copyToTempDir(dirB);

      File dirBCopy = new File(new File(getTempDirPath()), dirB.getName());
      assertTrue(dirBCopy.exists());
      assertTrue(dirBCopy.isDirectory());
    } finally {
      boolean deleted = dirB.delete();
      assertTrue(deleted);
    }
  }
}
