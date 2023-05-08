/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.util;

import java.io.*;
import java.util.*;
import java.util.jar.*;
import java.util.stream.*;
import org.junit.jupiter.api.*;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.config.*;
import org.lockss.test.*;
import org.lockss.log.*;
import org.lockss.util.PluginPackager.*;

public class TestPluginPackager extends LockssTestCase5 {
  static L4JLogger log = L4JLogger.getLogger();

  static String ID_A = "org.lockss.util.testplugin.child.APlug";
  static String ID_B = "org.lockss.util.testplugin.child.BPlug";
  static String ID_C = "org.lockss.util.testplugin.child.CPlug";
  static String ID_P = "org.lockss.util.testplugin.PPlug";
  static String ID_PL = "org.lockss.util.testpluginwithlib.Plug";
  static String ID_AUX = "org.lockss.util.testplugin.PlugWithAux";
  static String ID_AUX_WITH_PARENT = "org.lockss.util.testplugin.PlugWithParentAux";
  static String ID_PARENT_AUX = "org.lockss.util.testplugin.ParentAux";

  PluginPackager pkgr;
  File tmpdir;
  File jar;

  @BeforeEach
  public void beach() throws IOException {
    tmpdir = getTempDir("TestPluginPackager");
    jar = new File(tmpdir, "plug1.jar");
    pkgr = new PluginPackager();
  }

  public void assertMarkedAsPlugin(Manifest man, String id) {
    Attributes attrs = man.getAttributes(idToPath(id));
    assertNotNull(attrs, "Member attributes");
    assertEquals("true", attrs.getValue("Lockss-Plugin"));
  }

  public void assertNotMarkedAsPlugin(Manifest man, String id) {
    Attributes attrs = man.getAttributes(idToPath(id));
    if (attrs != null) {
      assertNotEquals("true", attrs.getValue("Lockss-Plugin"));
    }
  }

  static String idToPath(String plugId) {
    return plugId.replace('.', '/') + ".xml";
  }

  List<Result> runPackager(PlugSpec spec) throws Exception {
    pkgr.addSpec(spec);
    pkgr.build();
    pkgr.reportResults(pkgr);
    return pkgr.getResults();
  }

  @Test
  public void testPackage() throws Exception {
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_A)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    assertFalse(res.isNotModified());
    assertFalse(res.isSigned());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    // All files in dir should be there, but not parent dir
    assertThat(names, hasItems("META-INF/",
			       "META-INF/MANIFEST.MF",
			       "org/lockss/util/testplugin/child/",
			       idToPath(ID_A),
			       idToPath(ID_B),
			       idToPath(ID_C)));
    assertThat(names, not(hasItems(idToPath(ID_P))));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertMarkedAsPlugin(man, ID_A);
    assertNotMarkedAsPlugin(man, ID_B);
    assertNotMarkedAsPlugin(man, ID_C);
    assertNotMarkedAsPlugin(man, ID_P);
    Attributes plug =
      man.getAttributes("org/lockss/util/testplugin/child/APlug.xml");
    assertNotNull(plug);
    assertEquals("true", plug.getValue("Lockss-Plugin"));
  }

  @Test
  public void testPackageTwo() throws Exception {
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_B)
				  .addPlug(ID_C)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    assertThat(names, hasItems("META-INF/",
			       "META-INF/MANIFEST.MF",
			       "org/lockss/util/testplugin/child/",
			       idToPath(ID_B),
			       idToPath(ID_C),
			       "org/lockss/util/testplugin/",
			       idToPath(ID_P)));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertNotMarkedAsPlugin(man, ID_A);
    assertMarkedAsPlugin(man, ID_B);
    assertMarkedAsPlugin(man, ID_C);
    assertNotMarkedAsPlugin(man, ID_P);
  }

  @Test
  public void testPackageWithAuxPackages() throws Exception {
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_AUX)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    assertEquals(SetUtil.set("META-INF/",
                             "META-INF/MANIFEST.MF",
                             "org/lockss/util/testplugin/",
                             idToPath(ID_AUX),
                             idToPath(ID_AUX_WITH_PARENT),
                             idToPath(ID_PARENT_AUX),
                             "org/lockss/util/testplugin/auxpackage/",
                             "org/lockss/util/testplugin/auxpackage/FooFilterFactory.class",
                             "org/lockss/util/testplugin/PPlug.xml"),
                 SetUtil.theSet(names));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertNotMarkedAsPlugin(man, ID_A);
    assertMarkedAsPlugin(man, ID_AUX);
    assertNotMarkedAsPlugin(man, ID_AUX_WITH_PARENT);
    assertNotMarkedAsPlugin(man, ID_PARENT_AUX);
    assertNotMarkedAsPlugin(man, ID_P);
  }

  @Test
  public void testPackageWithParentAuxPackages() throws Exception {
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_AUX_WITH_PARENT)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    assertEquals(SetUtil.set("META-INF/",
                             "META-INF/MANIFEST.MF",
                             "org/lockss/util/testplugin/",
                             idToPath(ID_AUX),
                             idToPath(ID_AUX_WITH_PARENT),
                             idToPath(ID_PARENT_AUX),
                             "org/lockss/util/testplugin/auxpackage/",
                             "org/lockss/util/testplugin/auxpackage/FooFilterFactory.class",
                             "org/lockss/util/testplugin/auxpackage2/",
                             "org/lockss/util/testplugin/auxpackage2/FooLinkExtractorFactory$FooLinkExtractor.class",
                             "org/lockss/util/testplugin/auxpackage2/FooLinkExtractorFactory.class",
                             "org/lockss/util/testplugin/PPlug.xml"),
                 SetUtil.theSet(names));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertNotMarkedAsPlugin(man, ID_A);
    assertNotMarkedAsPlugin(man, ID_AUX);
    assertMarkedAsPlugin(man, ID_AUX_WITH_PARENT);
    assertNotMarkedAsPlugin(man, ID_PARENT_AUX);
    assertNotMarkedAsPlugin(man, ID_P);
  }

  @Test
  public void testSignWithResourceKeystore() throws Exception {
    pkgr.setKeystore("resource:org/lockss/test/goodguy.keystore");
    pkgr.setAlias("goodguy");
    pkgr.setKeyPass("f00bar");
    pkgr.setStorePass("f00bar");
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_A)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    assertFalse(res.isNotModified());
    assertTrue(res.isSigned());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    // All files in dir should be there, but not parent dir
    assertThat(names, hasItems("META-INF/",
			       "META-INF/MANIFEST.MF",
			       "org/lockss/util/testplugin/child/",
			       idToPath(ID_A),
			       idToPath(ID_B),
			       idToPath(ID_C)));
    assertThat(names, not(hasItems(idToPath(ID_P))));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertMarkedAsPlugin(man, ID_A);
    assertNotMarkedAsPlugin(man, ID_B);
    assertNotMarkedAsPlugin(man, ID_C);
    assertNotMarkedAsPlugin(man, ID_P);
    Attributes plug =
      man.getAttributes("org/lockss/util/testplugin/child/APlug.xml");
    assertNotNull(plug);
    assertEquals("true", plug.getValue("Lockss-Plugin"));
  }

  @Test
  public void testPackageLib() throws Exception {
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_PL)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    // jar in lib dir should be copied in
    assertThat(names, hasItems("META-INF/",
			       "META-INF/MANIFEST.MF",
			       "org/lockss/util/testpluginwithlib/",
			       idToPath(ID_PL),
			       "lib/",
			       "lib/plugin-lib.jar"));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertMarkedAsPlugin(man, ID_PL);
  }

  @Test
  public void testPackageExplodedLib() throws Exception {
    pkgr.setExplodeLib(true);
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug(ID_PL)
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertFalse(res.isError());
    JarFile jf = new JarFile(jar);
    List<String> names = Collections.list(jf.entries()).stream()
      .map(JarEntry::getName)
      .collect(Collectors.toList());

    // jar in lib dir should be exploded
    assertThat(names, hasItems("META-INF/",
			       "META-INF/MANIFEST.MF",
			       "org/lockss/util/testpluginwithlib/",
			       idToPath(ID_PL),
			       "org/",
			       "org/lockss/",
			       "org/lockss/pkgpkg/",
			       "org/lockss/pkgpkg/pkgd_resource.txt",
			       "toplevel_resource.txt"));
    Manifest man = jf.getManifest();
    Attributes main = man.getMainAttributes();
    assertMarkedAsPlugin(man, ID_PL);
  }

  boolean isNoExplode(String s) {
    return PluginPackager.EXCLUDE_FROM_EXPLODE_PAT.matcher(s).matches();
  }

  @Test
  public void testNoExplodePat() throws Exception {
    assertTrue(isNoExplode("meta-inf/manifest.mf"));
    assertTrue(isNoExplode("META-INF/MANIFEST.MF"));
    assertTrue(isNoExplode("meta-inf/signer.sf"));
    assertTrue(isNoExplode("meta-inf/signer.dsa"));
    assertTrue(isNoExplode("meta-inf/ECPLISE_.rsa"));
    assertFalse(isNoExplode("meta-inf/foo.cxf"));
    assertFalse(isNoExplode("meta-inf/services/foo"));
  }

  @Test
  public void testFail() throws Exception {
    List<Result> rs = runPackager(new PlugSpec()
				  .addPlug("org.lockss.util.testplugin.NoPlug")
				  .setJar(jar.toString()));
    assertEquals(1, rs.size());
    Result res = rs.get(0);
    assertTrue(res.isError());
  }
}
