/*

Copyright (c) 2000-2023, Board of Trustees of Leland Stanford Jr. University,
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.test;

import java.io.*;
import java.util.*;
import java.util.jar.*;
import java.util.stream.*;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.test.*;
import org.lockss.log.*;
// import org.lockss.util.PluginWellformednessTests.*;

public class TestPluginWellformednessTests extends LockssTestCase4 {
  static L4JLogger log = L4JLogger.getLogger();

  static String LARGE_ID = "org.lockss.plugin.definable.LargeTestPlugin";
  static String GOOD_ID = "org.lockss.plugin.definable.GoodPlugin";
  static String BAD1_ID = "org.lockss.plugin.definable.BadPluginIllArg1";
  static String BAD2_ID = "org.lockss.plugin.definable.ChildPluginNoParent";
  static String BAD3_ID = "org.lockss.plugin.definable.ChildPluginParentLoop";

  static String GOOD_JAR = "/org/lockss/test/plugins/good-xml.jar";
  static String NOMANIFEST_JAR = "/org/lockss/test/plugins/nomanifest.jar";
  static String AUX_JAR = "/org/lockss/test/plugins/good-with-aux.jar";
  static String PARENT_JAR = "/org/lockss/test/plugins/good-with-parent.jar";
  static String EXT_AUX_JAR = "/org/lockss/test/plugins/good-with-ext-aux.jar";
  static String MISSING_PARENT_JAR = "/org/lockss/test/plugins/good-with-missing-parent.jar";
  static String MISSING_AUX_JAR = "/org/lockss/test/plugins/good-with-missing-aux.jar";
  static String NO_VALIDATE_JAR = "/org/lockss/test/plugins/failvalidate.jar";

  private void testPlug(String id) throws Exception {
    new PluginWellformednessTests().testPlugins(ListUtil.list(id));
  }

  private void testJar(String path) throws Exception {
    String jarfile = copyResourceToTempFile(path);
    new PluginWellformednessTests().testJars(ListUtil.list(jarfile));
//     new PluginWellformednessTests().testJars(ListUtil.list(path));
  }

  private String copyResourceToTempFile(String resource) throws IOException {
    File tmpjar = getTempFile(resource, "temp");
    try (InputStream input = getResourceAsStream(resource)) {
      FileUtils.copyToFile(input, tmpjar);
    }
    return tmpjar.toString();
  }

  @Test
  public void testGoodPlugins() throws Exception {
    testPlug(GOOD_ID);
    testPlug(LARGE_ID);
  }

  @Test
  public void testBadPlugins() throws Exception {
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "Unknown param in printf",
                      () -> testPlug(BAD1_ID));;
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "Parent of .*\\.ChildPluginNoParent not found",
                      () -> testPlug(BAD2_ID));;
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "Plugin inheritance loop",
                      () -> testPlug(BAD3_ID));;
  }

  @Test
  public void testGoodJars() throws Exception {
    testJar(GOOD_JAR);
    testJar(PARENT_JAR);
    testJar(AUX_JAR);
    testJar(EXT_AUX_JAR);
  }

  @Test
  public void testBadJars() throws Exception {
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "Plugin jar has no manifest",
                      () -> testJar(NOMANIFEST_JAR));;
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "ParentNotFoundException",
                 () -> testJar(MISSING_PARENT_JAR));;
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "class not found .*MockHtmlLinkExtractorFactory",
                 () -> testJar(MISSING_AUX_JAR));;
    assertThrowsMatch(PluginWellformednessTests.MalformedPluginException.class,
                      "DispatchingUrlFetcherFactory is not a .*\\.CrawlUrlComparatorFactory",
                 () -> testJar(NO_VALIDATE_JAR));;

  }

}
