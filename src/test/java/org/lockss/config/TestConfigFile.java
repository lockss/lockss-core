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

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.lockss.config.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.lockss.config.ConfigManager.RemoteConfigFailoverInfo;
import org.lockss.config.TdbTitle;
import org.lockss.hasher.*;
import org.lockss.util.test.FileTestUtil;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.io.*;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimerUtil;
import org.lockss.util.urlconn.*;
import static org.lockss.config.ConfigManager.KeyPredicate;

/**
 * Abstract superclass for tests of ConfigFile variants, nested in this
 * class
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestConfigFile.TestFile.class,
                     TestConfigFile.TestDynamic.class,
                     TestConfigFile.TestHttp.class,
                     TestConfigFile.TestJar.class})
public class TestConfigFile {

  public static abstract class TestConfigFileParent extends LockssTestCase4 {

    protected static final String text1 =
        "prop.1=foo\n" +
        "prop.2=bar\n" +
        "prop.3=baz";

    // valid tdb entry
    protected static final String tdbText1 =
      "org.lockss.prop=value\n" +
      "org.lockss.title.title1.title=Air & Space volume 3\n" +
      "org.lockss.title.title1.plugin=org.lockss.testplugin1\n" +
      "org.lockss.title.title1.pluginVersion=4\n" +
      "org.lockss.title.title1.issn=0003-0031\n" +
      "org.lockss.title.title1.journal.link.1.type=\n" + TdbTitle.LinkType.continuedBy.toString() +
      "org.lockss.title.title1.journal.link.1.journalId=0003-0031\n" +  // link to self
      "org.lockss.title.title1.param.1.key=volume\n" +
      "org.lockss.title.title1.param.1.value=3\n" +
      "org.lockss.title.title1.param.2.key=year\n" +
      "org.lockss.title.title1.param.2.value=1999\n" +
      "org.lockss.title.title1.attributes.publisher=The Smithsonian Institution";

    // invalid tdb entry: missing "plugin"
    protected static final String tdbText2 =
      "org.lockss.prop=value\n" +
      "org.lockss.title.title1.title=Air & Space volume 3\n" +
//        "org.lockss.title.title1.plugin=org.lockss.testplugin1\n" +
      "org.lockss.title.title1.pluginVersion=4\n" +
      "org.lockss.title.title1.issn=0003-0031\n" +
      "org.lockss.title.title1.journal.link.1.type=\n" + TdbTitle.LinkType.continuedBy.toString() +
      "org.lockss.title.title1.journal.link.1.journalId=0003-0031\n" +  // link to self
      "org.lockss.title.title1.param.1.key=volume\n" +
      "org.lockss.title.title1.param.1.value=3\n" +
      "org.lockss.title.title1.param.2.key=year\n" +
      "org.lockss.title.title1.param.2.value=1999\n" +
      "org.lockss.title.title1.attributes.publisher=The Smithsonian Institution";


    // valid tdb entry
    protected static final String tdbXml1 =
      "<lockss-config>\n" +
      "  <property name=\"org.lockss\">\n" +
      "    <property name=\"prop\" value=\"value\" />\n" +
      "    <property name=\"title\">\n" +
      "      <property name=\"title1\">\n" +
      "        <property name=\"title\" value=\"Air &amp; Space volume 3\" />\n" +
      "        <property name=\"plugin\" value=\"org.lockss.testplugin1\" />\n" +
      "        <property name=\"pluginVersion\" value=\"4\" />\n" +
      "        <property name=\"issn\" value=\"0003-0031\" />\n" +
      "        <property name=\"journal\">\n" +
      "          <property name=\"link.1.type\" value=\"" + TdbTitle.LinkType.continuedBy.toString() + "\" />\n" +
      "          <property name=\"link.1.journalId\" value=\"0003-0031\" />\n" +  // link to self
      "        </property>\n" +
      "        <property name=\"param.1.key\" value=\"volume\" />\n" +
      "        <property name=\"param.1.value\" value=\"3\" />\n" +
      "        <property name=\"param.2.key\" value=\"year\" />\n" +
      "        <property name=\"param.2.value\" value=\"1999\" />\n" +
      "        <property name=\"attributes.publisher\" value=\"The Smithsonian Institution\" />\n" +
      "      </property>\n" +
      "    </property>\n" +
      "  </property>\n" +
    "</lockss-config>";
    
    // invalid tdb entry: missing "plugin"
    protected static final String tdbXml2 =
      "<lockss-config>\n" +
      "  <property name=\"org.lockss\">\n" +
      "    <property name=\"prop\" value=\"value\" />\n" +
      "    <property name=\"title\">\n" +
      "      <property name=\"title1\">\n" +
      "        <property name=\"title\" value=\"Air &amp; Space volume 3\" />\n" +
//        "        <property name=\"plugin\" value=\"org.lockss.testplugin1\" />\n" +
      "        <property name=\"pluginVersion\" value=\"4\" />\n" +
      "        <property name=\"issn\" value=\"0003-0031\" />\n" +
      "        <property name=\"journal\">\n" +
      "          <property name=\"link.1.type\" value=\"" + TdbTitle.LinkType.continuedBy.toString() + "\" />\n" +
      "          <property name=\"link.1.journalId\" value=\"0003-0031\" />\n" +  // link to self
      "        </property>\n" +
      "        <property name=\"param.1.key\" value=\"volume\" />\n" +
      "        <property name=\"param.1.value\" value=\"3\" />\n" +
      "        <property name=\"param.2.key\" value=\"year\" />\n" +
      "        <property name=\"param.2.value\" value=\"1999\" />\n" +
      "        <property name=\"attributes.publisher\" value=\"The Smithsonian Institution\" />\n" +
      "      </property>\n" +
      "    </property>\n" +
      "  </property>\n" +
    "</lockss-config>";
    
  /*
   */
    protected static final String xml1 =
      "<lockss-config>\n" +
      "  <property name=\"prop.7\" value=\"foo\" />\n" +
      "  <property name=\"prop.8\" value=\"bar\" />\n" +
      "  <property name=\"prop.9\" value=\"baz\" />\n" +
      "</lockss-config>";

    protected static final String badConfig =
      "<lockss-config>\n" +
      "  <property name=\"prop.10\" value=\"foo\" />\n" +
      "  <property name=\"prop.11\">\n" +
      "    <value>bar</value>\n" +
      "  <!-- missing closing property tag -->\n" +
      "</lockss-config>";

    /** subless should set this to the URL it generates */
    protected String expectedUrl;

    /** subclass must implement to create a ConfigFile instance of the
     * appropriate type, with the specified content
     */
    protected abstract ConfigFile makeConfigFile(String contents,
                                                 boolean isXml)
        throws IOException;

    /** subclass must implement to update the last modification time of the
     * underlying file/URL
     */
    protected abstract void updateLastModified(ConfigFile cf, long time)
        throws IOException;

    /** subclass must implement to say whether each call to
     * getConfiguration() is expected to check whether the file has been
     * modified.  If false, should only happen if setNeedsReload() has been
     * called.
     */
    protected abstract boolean isAlwaysAttempt();

    public static String dateString(long time) {
      Date date = new Date(time);
      return DateTimeUtil.GMT_DATE_FORMATTER.format(date);
    }

    protected String suff(boolean isXml) {
      return isXml ? ".xml" : ".txt";
    }

    // Parameterized tests - invoked either from tests in this class or
    // subclasses

    /** Load file, check status, load again, check no change, force change,
     * load again, check reloaded
     */
    public Configuration doTestLoad(String content, boolean xml)
        throws IOException {
      ConfigFile cf = makeConfigFile(content, xml);
      assertFalse(cf.isLoaded());
      assertEquals(null, cf.getLastModified());
      assertEquals("Not yet loaded", cf.getLoadErrorMessage());
      long lastAttempt;
      long prevAttempt = cf.getLastAttemptTime();

      Configuration config = cf.getConfiguration();
      assertTrue(cf.isLoaded());
      assertEquals(expectedUrl, cf.getFileUrl());
      assertEquals(expectedUrl, cf.getLoadedUrl());
      String last = cf.getLastModified();
      assertNotNull("last modified shouldn't be null", last);
      assertNotEquals(prevAttempt, lastAttempt = cf.getLastAttemptTime());
      prevAttempt = lastAttempt;

      TimerUtil.guaranteedSleep(1);
      assertSame(config, cf.getConfiguration());
      assertEquals(last, cf.getLastModified());
      if (isAlwaysAttempt()) {
        assertNotEquals(prevAttempt, lastAttempt = cf.getLastAttemptTime());
        prevAttempt = lastAttempt;
      } else {
        assertEquals(prevAttempt, lastAttempt = cf.getLastAttemptTime());
      }
      TimerUtil.guaranteedSleep(1);
      cf.setNeedsReload();
      assertSame(config, cf.getConfiguration());
      assertEquals(last, cf.getLastModified());
      assertNotEquals(prevAttempt, lastAttempt = cf.getLastAttemptTime());
      prevAttempt = lastAttempt;

      updateLastModified(cf, TimeBase.nowMs() + Constants.SECOND);
      cf.setNeedsReload();
      assertEqualsNotSame(config, cf.getConfiguration());
      assertNotEquals(last, cf.getLastModified());
      return cf.getConfiguration();
    }

    /** Test that reading the ConfigFile causes the appropriate error
     */
    public void doTestCantRead(ConfigFile cf, String re) throws IOException {
      assertFalse(cf.isLoaded());
      try {
        Configuration config = cf.getConfiguration();
        fail("Shouldn't have created config: " + config);
      } catch (IOException e) {
      }
      if (re != null) {
        assertMatchesRE(re, cf.getLoadErrorMessage());
      }
    }

    /** Test that reading the specified content from a ConfigFile causes the
     * appropriate error
     */
    public void doTestIllContent(String content, boolean xml, String re)
        throws IOException {
      doTestCantRead(makeConfigFile(content, xml), re);
    }

    @Test
    public void testKeyPredicate() throws IOException {
      KeyPredicate keyPred = new KeyPredicate() {
          public boolean evaluate(Object obj) {
            return ((String)obj).indexOf("bad") < 0;
          }
          public boolean failOnIllegalKey() {
            return false;
          }};

      String str = "foo.bar=one\nfoo.bad=two\nfoo.foo=four\n";

      ConfigFile cf = makeConfigFile(str, false);
      cf.setKeyPredicate(keyPred);
      assertFalse(cf.isLoaded());

      Configuration config = cf.getConfiguration();
      assertEquals("one", config.get("foo.bar"));
      assertEquals("four", config.get("foo.foo"));
      assertNull(config.get("foo.bad"));
      assertFalse(config.containsKey("foo.bad"));
    }

    // Matching key should throw IOException
    @Test
    public void testKeyPredicateThrow() throws IOException {
      KeyPredicate keyPred = new KeyPredicate() {
          public boolean evaluate(Object obj) {
            return ((String)obj).indexOf("bad") < 0;
          }
          public boolean failOnIllegalKey() {
            return true;
          }};

      String str = "foo.bar=one\nfoo.bad=two\nfoo.foo=four\n";

      ConfigFile cf = makeConfigFile(str, false);
      cf.setKeyPredicate(keyPred);
      assertFalse(cf.isLoaded());
      try {
        Configuration config = cf.getConfiguration();
        fail("Loading config file with illegal key should throw");
      } catch (IOException e) {
      }

      // this one doesn't match, shouldn't throw
      String str2 = "foo.bar=one\nfoo.dab=two\nfoo.foo=four\n";

      ConfigFile cf2 = makeConfigFile(str2, false);
      cf2.setKeyPredicate(keyPred);
      assertFalse(cf2.isLoaded());

      Configuration config2 = cf2.getConfiguration();
      assertEquals("one", config2.get("foo.bar"));
      assertEquals("four", config2.get("foo.foo"));
      assertEquals("two", config2.get("foo.dab"));
    }

    // Test cases.  These will be run once for each ConfigFile variant

    /** Load a props file */
    @Test
    public void testLoadText() throws IOException {
      Configuration config = doTestLoad(text1, false);
      assertEquals("foo", config.get("prop.1"));
      assertEquals("bar", config.get("prop.2"));
      assertEquals("baz", config.get("prop.3"));
    }

    /** Load valid Tdb property file with a single entry */
    @Test
    public void testValidLoadTdbText() throws IOException {
      Configuration config = doTestLoad(tdbText1, false);
      assertEquals("value", config.get("org.lockss.prop"));
      
      Tdb tdb = config.getTdb();
      assertNotNull(tdb);
      assertEquals(1, tdb.getTdbAuCount());
    }

    /** Load invalid Tdb property file with a single entry */
    @Test
    public void testInvalidLoadTdbText() throws IOException {
      Configuration config = doTestLoad(tdbText2, false);
      assertEquals("value", config.get("org.lockss.prop"));
      
      Tdb tdb = config.getTdb();
      assertNull(tdb);
    }

    /** Load an XML file */
    @Test
    public void testLoadXml() throws IOException {
      Configuration config = doTestLoad(xml1, true);
      assertEquals("foo", config.get("prop.7"));
      assertEquals("bar", config.get("prop.8"));
      assertEquals("baz", config.get("prop.9"));
    }

    /** Try to load a bogus XML file */
    @Test
    public void testIllXml() throws IOException {
      doTestIllContent(badConfig, true, "SAXParseException");
    }

    /** Load valid Tdb XML file with a single entry */
    @Test
    public void testValidLoadTdbXml() throws IOException {
      Configuration config = doTestLoad(tdbXml1, true);
      assertEquals("value", config.get("org.lockss.prop"));
      
      Tdb tdb = config.getTdb();
      assertNotNull(tdb);
      assertEquals(1, tdb.getTdbAuCount());
    }

    /** Load invalid Tdb XML file with a single entry */
    @Test
    public void testInvalidLoadTdbXml() throws IOException {
      Configuration config = doTestLoad(tdbXml2, true);
      assertEquals("value", config.get("org.lockss.prop"));
      
      Tdb tdb = config.getTdb();
      assertNull(tdb);
    }

    @Test
    public void testGeneration() throws IOException {
      ConfigFile cf = makeConfigFile("aa=54", false);

      assertFalse(cf.isLoaded());
      ConfigFile.Generation gen = cf.getGeneration();
      assertTrue(cf.isLoaded());
      assertEquals(cf.getConfiguration(), gen.getConfig());

      TimerUtil.guaranteedSleep(1);
      assertEquals(gen.getGeneration(), cf.getGeneration().getGeneration());
      assertEquals(gen.getUrl(), cf.getGeneration().getUrl());
      assertEquals(gen.getConfig(), cf.getGeneration().getConfig());

      TimerUtil.guaranteedSleep(1);
      cf.setNeedsReload();
      assertEquals(cf.getConfiguration(), gen.getConfig());

      updateLastModified(cf, TimeBase.nowMs() + Constants.SECOND);
      cf.setNeedsReload();
      ConfigFile.Generation gen2 = cf.getGeneration();
      assertEquals(gen.getGeneration() + 1, gen2.getGeneration());
      assertEqualsNotSame(gen.getConfig(), gen2.getConfig());
    }

    protected String gzippedTempFileUrl(String content, String ext) throws IOException {
      return gzippedTempFile(content, ext).toURI().toURL().toString();
    }

    protected File gzippedTempFile(String content, String ext) throws IOException {
      File tmpFile = getTempFile("config", ext + ".gz");
      OutputStream out =
        new BufferedOutputStream(new FileOutputStream(tmpFile));
      out = new GZIPOutputStream(out, true);
      Writer wrtr = new OutputStreamWriter(out, Constants.DEFAULT_ENCODING);
      wrtr.write(content);
      wrtr.close();
      return tmpFile;
    }

  }
  
  /** Test FileConfigFile */
  public static class TestFile extends TestConfigFileParent {

    protected ConfigFile makeConfigFile(String contents, boolean isXml)
	throws IOException {
      expectedUrl = FileTestUtil.urlOfString(contents, suff(isXml));
      return new FileConfigFile(expectedUrl, null);
    }

    protected void updateLastModified(ConfigFile cf, long time)
	throws IOException {
      FileConfigFile fcf = (FileConfigFile)cf;
      File file = fcf.makeFile();
      file.setLastModified(time);
    }

    protected boolean isAlwaysAttempt() {
      return true;
    }

    // Test cases

    @Test
    public void testNotFound() throws IOException {
      doTestCantRead(new FileConfigFile("/file/not/found", null),
		   "FileNotFoundException");
    }

    @Test
    public void testGzip() throws IOException {
      FileConfigFile fcf =
	new FileConfigFile(gzippedTempFileUrl(text1, ".txt"), null);
      Configuration config = fcf.getConfiguration();
      assertTrue(fcf.isLoaded());
      assertEquals("foo", config.get("prop.1"));
      assertEquals("bar", config.get("prop.2"));
      assertEquals("baz", config.get("prop.3"));
    }

    @Test
    public void testGzipXml() throws IOException {
      FileConfigFile fcf =
	new FileConfigFile(gzippedTempFileUrl(xml1, ".xml"), null);
      Configuration config = fcf.getConfiguration();
      assertTrue(fcf.isLoaded());
      assertEquals("foo", config.get("prop.7"));
      assertEquals("bar", config.get("prop.8"));
      assertEquals("baz", config.get("prop.9"));
    }

    // Ensure storedConfig() of a sealed config doesn't make a copy
    @Test
    public void testStoredConfigSealed() throws IOException {
      FileConfigFile fcf = (FileConfigFile)makeConfigFile("a=1\nb1=a", false);
      Configuration c = fcf.getConfiguration();
      Configuration c2 = ConfigurationUtil.fromArgs("x", "y");
      assertSame(c, fcf.getConfiguration());
      assertNotSame(c2, fcf.getConfiguration());
      c2.seal();
      fcf.storedConfig(c2);
      assertSame(c2, fcf.getConfiguration());
    }

    // Ensure storedConfig() of an unsealed config does make a copy
    @Test
    public void testStoredConfigUnsealed() throws IOException {
      FileConfigFile fcf = (FileConfigFile)makeConfigFile("a=1\nb1=a", false);
      Configuration c = fcf.getConfiguration();
      Configuration c2 = ConfigurationUtil.fromArgs("x", "y");
      assertSame(c, fcf.getConfiguration());
      assertNotSame(c2, fcf.getConfiguration());
      fcf.storedConfig(c2);
      assertEqualsNotSame(c2, fcf.getConfiguration());
    }

    @Test
    public void testPlatformFile() throws Exception {
      FileConfigFile cf = (FileConfigFile)makeConfigFile(text1, false);
      assertFalse(cf.isPlatformFile());

      cf.setConfigManager(new MyConfigManager(getTempDir(), null, null, null,
	  null));
      assertFalse(cf.isPlatformFile());

      cf.setConfigManager(new MyConfigManager(getTempDir(),
					      ListUtil.list(cf.getFileUrl()),
					      null, null, null));
      assertTrue(cf.isPlatformFile());
    }

    @Test
    public void testRestConfigFile() throws Exception {
      ConfigManager configMgr = new MyConfigManager(getTempDir(), null,
	  "http://localhost:1234", null, null);
      RestConfigFile rcf =
	  new RestConfigFile("http://localhost:1234/rcf1", configMgr);
      assertTrue(configMgr.getRestConfigClient().isActive());
      assertTrue(configMgr.getRestConfigClient()
	  .isPartOfThisService(rcf.getFileUrl()));
    }
  }

  /** Test DynamicConfigFile */
  public static class TestDynamic extends TestConfigFileParent {

    protected ConfigFile makeConfigFile(String contents, boolean isXml)
	throws IOException {
      expectedUrl = "Dynamic Url" + suff(isXml);
      return new DynamicConfigFile(expectedUrl, null) {
	@Override
	protected void generateFileContent(File file,
					   ConfigManager cfgMgr)
	    throws IOException {
	  FileTestUtil.writeFile(file, contents);
	}
      };
    }

    protected void updateLastModified(ConfigFile cf, long time)
	throws IOException {
      DynamicConfigFile dcf = (DynamicConfigFile)cf;
      dcf.newLastModified = time;
    }

    protected boolean isAlwaysAttempt() {
      return true;
    }

    // Test cases

    @Test
    public void testNotFound() throws IOException {
      doTestCantRead(new FileConfigFile("/file/not/found", null),
		   "FileNotFoundException");
    }

    @Test
    public void testGzip() throws IOException {
      FileConfigFile fcf =
	new FileConfigFile(gzippedTempFileUrl(text1, ".txt"), null);
      Configuration config = fcf.getConfiguration();
      assertTrue(fcf.isLoaded());
      assertEquals("foo", config.get("prop.1"));
      assertEquals("bar", config.get("prop.2"));
      assertEquals("baz", config.get("prop.3"));
    }

    @Test
    public void testGzipXml() throws IOException {
      FileConfigFile fcf =
	new FileConfigFile(gzippedTempFileUrl(xml1, ".xml"), null);
      Configuration config = fcf.getConfiguration();
      assertTrue(fcf.isLoaded());
      assertEquals("foo", config.get("prop.7"));
      assertEquals("bar", config.get("prop.8"));
      assertEquals("baz", config.get("prop.9"));
    }

    // Ensure storedConfig() of a sealed config doesn't make a copy
    @Test
    public void testStoredConfigSealed() throws IOException {
      FileConfigFile fcf = (FileConfigFile)makeConfigFile("a=1\nb1=a", false);
      Configuration c = fcf.getConfiguration();
      Configuration c2 = ConfigurationUtil.fromArgs("x", "y");
      assertSame(c, fcf.getConfiguration());
      assertNotSame(c2, fcf.getConfiguration());
      c2.seal();
      fcf.storedConfig(c2);
      assertSame(c2, fcf.getConfiguration());
    }

    // Ensure storedConfig() of an unsealed config does make a copy
    @Test
    public void testStoredConfigUnsealed() throws IOException {
      FileConfigFile fcf = (FileConfigFile)makeConfigFile("a=1\nb1=a", false);
      Configuration c = fcf.getConfiguration();
      Configuration c2 = ConfigurationUtil.fromArgs("x", "y");
      assertSame(c, fcf.getConfiguration());
      assertNotSame(c2, fcf.getConfiguration());
      fcf.storedConfig(c2);
      assertEqualsNotSame(c2, fcf.getConfiguration());
    }

    @Test
    public void testPlatformFile() throws Exception {
      FileConfigFile cf = (FileConfigFile)makeConfigFile(text1, false);
      assertFalse(cf.isPlatformFile());

      cf.setConfigManager(new MyConfigManager(getTempDir(), null, null, null,
	  null));
      assertFalse(cf.isPlatformFile());

      cf.setConfigManager(new MyConfigManager(getTempDir(),
					      ListUtil.list(cf.getFileUrl()),
					      null, null, null));
      assertTrue(cf.isPlatformFile());
    }

    @Test
    public void testRestConfigFile() throws Exception {
      ConfigManager configMgr = new MyConfigManager(getTempDir(), null,
	  "http://localhost:1234", null, null);
      RestConfigFile rcf =
	  new RestConfigFile("http://localhost:1234/rcf1", configMgr);
      assertTrue(configMgr.getRestConfigClient().isActive());
      assertTrue(configMgr.getRestConfigClient()
	  .isPartOfThisService(rcf.getFileUrl()));
    }
  }

  /** Test JarConfigFile */
  public static class TestJar extends TestConfigFileParent {

    protected ConfigFile makeConfigFile(String contents, boolean isXml)
	throws IOException {
      String jarName = getTempDir().getAbsolutePath() +
	File.separator + "test.jar";
      String entryName = "testent." + suff(isXml);
      // Create a jar file with a single resource (a text file)
      Map entries = new HashMap();
      entries.put(entryName, contents);
      JarTestUtils.createStringJar(jarName, entries);
      expectedUrl = UrlUtil.makeJarFileUrl(jarName, entryName);
      return new JarConfigFile(expectedUrl, null);
    }

    protected void updateLastModified(ConfigFile cf, long time)
	throws IOException {
      JarConfigFile jcf = (JarConfigFile)cf;
      File file = jcf.getFile();
      file.setLastModified(time);
    }

    protected boolean isAlwaysAttempt() {
      return true;
    }

    // Test cases

    @Test
    public void testNotFound() throws IOException {
      JarConfigFile jcf;

      jcf = new JarConfigFile("jar:file:///file/not/found!/who.cares",null);
      doTestCantRead(jcf, "(ZipException|FileNotFoundException|NoSuchFileException)");

      String jarName = getTempDir().getAbsolutePath() +
	File.separator + "test.jar";
      Map entries = new HashMap();
      entries.put("foo.bar", "bletch");
      JarTestUtils.createStringJar(jarName, entries);
      String url = UrlUtil.makeJarFileUrl(jarName, "no.such.entry");
      jcf = new JarConfigFile(url, null);
      doTestCantRead(jcf, "FileNotFoundException");
    }
  }

  /** Test HTTPConfigFile */
  public static class TestHttp extends TestConfigFileParent {

    protected ConfigFile makeConfigFile(String contents, boolean isXml)
	throws IOException {
      expectedUrl = "http://foo.bar/lockss" + suff(isXml);
      return new MyHttpConfigFile(expectedUrl, contents);
    }

    protected void updateLastModified(ConfigFile cf, long time)
	throws IOException {
      MyHttpConfigFile hcf = (MyHttpConfigFile)cf;
      hcf.setLastModified(time);
    }

    protected boolean isAlwaysAttempt() {
      return false;
    }

    // Test cases

    @Test
    public void testResolveConfigUrl() {
      String fbase = "./config/common.xml";
      String stem = "http://props.lockss.org/";
      String hbase = stem +"path/lockss.xml";
      MyHttpConfigFile hcf = new MyHttpConfigFile(hbase);
      assertEquals(stem + "path/tdb.xml",
		   hcf.resolveConfigUrl("tdb.xml"));
      hcf = new MyHttpConfigFile(hbase + "?foo=bar");
      assertEquals(stem + "path/tdb.xml",
		   hcf.resolveConfigUrl("tdb.xml"));
      hcf = new MyHttpConfigFile("http://localhost:54420/config/url/http://props/lockss.xml");
      assertEquals("http://localhost:54420/config/url/http://props/tdb.xml",
		   hcf.resolveConfigUrl("tdb.xml"));
    }

    @Test
    public void testNotFound() throws IOException {
      String url;
      MyHttpConfigFile hcf;

      hcf = new MyHttpConfigFile("http://a.b/not/found");
      hcf.setResponseCode(404);
      doTestCantRead(hcf, "FileNotFoundException");

      url = "http://a.b:80:81/malformed.url";
      hcf = new MyHttpConfigFile(url);
      hcf.setExecuteException(new MalformedURLException(url));
      doTestCantRead(hcf, "MalformedURLException");
    }

    @Test
    public void testForbidden() throws IOException {
      String url;
      MyHttpConfigFile hcf;

      url = "http://a.b/forbidden";
      hcf = new MyHttpConfigFile(url,
				 "Error page with no hint, shouldn't " +
				 "end up in config file error msg");
      hcf.setResponseCode(403);
      hcf.setResponseMessage("Forbidden");
      doTestCantRead(hcf, "403: Forbidden$");

      hcf = new MyHttpConfigFile(url,
				 "foobar \n" +
				 "locksshint: this is a hint endhint");
      hcf.setResponseCode(403);
      hcf.setResponseMessage("Forbidden");
      doTestCantRead(hcf, "403: Forbidden\nthis is a hint$");
    }

    @Test
    public void testXLockssInfo() throws IOException {
      InputStream in = new StringInputStream(xml1);
      MyHttpConfigFile hcf =
	new MyHttpConfigFile("http://foo.bar/lockss.xml", in);
      hcf.setProperty("X-Lockss-Info", "daemon=1.3.1");
      Configuration config = hcf.getConfiguration();
      assertTrue(hcf.isLoaded());
      assertEquals("daemon=1.3.1", hcf.connReqHdrs.get("X-Lockss-Info"));
      hcf.connReqHdrs.clear();
      hcf.setProperty("X-Lockss-Info", null);
      hcf.getConfiguration();
      assertTrue(hcf.isLoaded());
      assertEquals(null, hcf.connReqHdrs.get("X-Lockss-Info"));
      assertEquals(null, hcf.proxyHost);
    }

    @Test
    public void testGzip() throws IOException {
      InputStream zin = new GZIPpedInputStream(xml1);
      MyHttpConfigFile hcf =
	new MyHttpConfigFile("http://foo.bar/lockss.xml", zin);
      hcf.setContentEncoding("gzip");
      Configuration config = hcf.getConfiguration();
      assertTrue(hcf.isLoaded());
    }

    // Ensure null message in exception doesn't cause problems
    // Not specific to HTTPConfigFile, but handy to test here because we can
    // make the subclass throw
    @Test
    public void testNullExceptionMessage() throws IOException {
      MyHttpConfigFile hcf =
	new MyHttpConfigFile("http://foo.bar/lockss.xml", "");
      hcf.setExecuteException(new IOException((String)null));
      try {
	Configuration config = hcf.getConfiguration();
	fail("Should throw");
      } catch (NullPointerException e) {
	fail("Null exception message caused", e);
      } catch (IOException e) {
      }
    }

    @Test
    public void testSetConnectionPool1() throws IOException {
      MyHttpConfigFile hcf =
	new MyHttpConfigFile("http://foo.bar/lockss.xml");
      LockssUrlConnectionPool pool = new LockssUrlConnectionPool();
      hcf.setConnectionPool(pool);
      assertSame(pool, hcf.getConnectionPool());
    }

    @Test
    public void testSetConnectionPool2() throws IOException {
      MyHttpConfigFile hcf =
	new MyHttpConfigFile("http://foo.bar/lockss.xml");
      LockssUrlConnectionPool pool = hcf.getConnectionPool();
      assertNotNull(pool);
      assertSame(pool, hcf.getConnectionPool());
    }

    @Test
    public void testProxy() throws IOException {
      String phost = "phost.foo";
      int pport = 1234;
      ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PROPS_PROXY,
				    phost + ":" + pport);
      InputStream in = new StringInputStream(xml1);
      MyHttpConfigFile hcf =
	new MyHttpConfigFile("http://foo.bar/lockss.xml", in);
      Configuration config = hcf.getConfiguration();
      assertEquals(phost, hcf.proxyHost);
      assertEquals(pport, hcf.proxyPort);
    }

    MyConfigManager newMyConfigManager() throws IOException {
      return new MyConfigManager(getTempDir());
    }

    @Test
    public void testMakeRemoteCopy()
	throws IOException, NoSuchAlgorithmException {
      String url1 = "http://foo.bar/lockss.xml";
      InputStream in = new StringInputStream(xml1);
      MyHttpConfigFile hcf = new MyHttpConfigFile(url1, in);
      MyConfigManager mcm = newMyConfigManager();
      File tmpFile = getTempFile("configtmp", "");
      FileUtil.safeDeleteFile(tmpFile);
      assertFalse(tmpFile.exists());
      mcm.setTempFile(url1, tmpFile);
      hcf.setConfigManager(mcm);
      String last = TestConfigFileParent.dateString(TimeBase.nowMs());
      hcf.setLastModified(last);

      Configuration config = hcf.getConfiguration();
      assertTrue(tmpFile.exists());
      RemoteConfigFailoverInfo rcfi = mcm.getRcfi(url1);
      assertReaderMatchesString(xml1,
				new InputStreamReader(new GZIPInputStream(new FileInputStream(tmpFile))));
      assertEquals("foo", config.get("prop.7"));
      assertEquals("bar", config.get("prop.8"));
      assertEquals("baz", config.get("prop.9"));

      assertEquals(last, rcfi.getLastModified());
      // Check checksum
      String alg =
	ConfigManager.DEFAULT_REMOTE_CONFIG_FAILOVER_CHECKSUM_ALGORITHM;
      assertEquals(hashGzippedString(xml1, alg).toString(), rcfi.chksum);
    }

    HashResult hashGzippedString(String str, String alg)
	throws NoSuchAlgorithmException, IOException {
      MessageDigest md = MessageDigest.getInstance(alg);
      OutputStream out = new org.apache.commons.io.output.NullOutputStream();
      HashedOutputStream hos = new HashedOutputStream(out, md);
      out = new GZIPOutputStream(hos);
      Writer wrtr = new OutputStreamWriter(out);
      wrtr.write(str);
      wrtr.close();
      return HashResult.make(md.digest(), alg);
    }

    @Test
    public void testLocalFailover() throws IOException {
      ConfigurationUtil.setFromArgs(ConfigManager.PARAM_REMOTE_CONFIG_FAILOVER_CHECKSUM_REQUIRED,
				  "false");

      String url1 = "http://foo.bar/lockss.xml";
      File remoteCopy = gzippedTempFile(xml1, ".xml");
      MyHttpConfigFile hcf =
	new MyHttpConfigFile(url1, new StringInputStream(""));
      MyConfigManager mcm = newMyConfigManager();
      mcm.setPermFile(url1, remoteCopy);
      hcf.setConfigManager(mcm);
      hcf.setResponseCode(404);
      assertTrue(remoteCopy.exists());
      Configuration config = hcf.getConfiguration();
      assertEquals("foo", config.get("prop.7"));
      assertEquals("bar", config.get("prop.8"));
      assertEquals("baz", config.get("prop.9"));
      assertEquals(url1, hcf.getFileUrl());
      assertMatchesRE("config\\d+\\.xml\\.gz", hcf.getLoadedUrl());

      // Make sure it reloads successfully when site returns
      hcf.setResponseCode(200);
      hcf.setContent(xml1);
      hcf.setNeedsReload();
      Configuration config2 = hcf.getConfiguration();
      assertNotSame(config, config2);
      assertEquals("foo", config2.get("prop.7"));
      assertEquals("bar", config2.get("prop.8"));
      assertEquals("baz", config2.get("prop.9"));
      assertEquals(url1, hcf.getFileUrl());
      assertEquals(url1, hcf.getLoadedUrl());
    }

    @Test
    public void testNoLocalFailover() throws IOException {
      String url1 = "http://foo.bar/lockss.xml";
      String url2 = "http://bar.foo/lockss.xml";
      File remoteCopy = gzippedTempFile(xml1, ".xml");
      MyHttpConfigFile hcf =
	new MyHttpConfigFile(url2, new StringInputStream(""));
      MyConfigManager mcm = newMyConfigManager();
      mcm.setPermFile(url1, remoteCopy);
      hcf.setConfigManager(mcm);
      hcf.setResponseCode(404);
      hcf.setResponseMessage("knocking at your door");
      assertTrue(remoteCopy.exists());
      try {
	Configuration config = hcf.getConfiguration();
	fail("Without local failover, should throw FileNotFoundException");
      } catch (FileNotFoundException e) {
	// Should throw original exception
	assertMatchesRE("404: knocking at your door", e.getMessage());
      }
    }

    @Test
    public void testLocalFailoverNoChecksum() throws IOException {
      String url1 = "http://foo.bar/lockss.xml";
      File remoteCopy = gzippedTempFile(xml1, ".xml");
      MyHttpConfigFile hcf =
	new MyHttpConfigFile(url1, new StringInputStream(""));
      MyConfigManager mcm = newMyConfigManager();
      mcm.setPermFile(url1, remoteCopy);
      hcf.setConfigManager(mcm);
      hcf.setResponseCode(404);
      assertTrue(remoteCopy.exists());
      try {
	Configuration config = hcf.getConfiguration();
      } catch (FileNotFoundException e) {
	assertMatchesRE("404", e.getMessage());
      }
    }

    @Test
    public void testLocalFailoverBadChecksum() throws IOException {
      String url1 = "http://foo.bar/lockss.xml";
      File remoteCopy = gzippedTempFile(xml1, ".xml");
      MyHttpConfigFile hcf =
	new MyHttpConfigFile(url1, new StringInputStream(""));
      MyConfigManager mcm = newMyConfigManager();
      mcm.setPermFile(url1, remoteCopy);
      RemoteConfigFailoverInfo rcfi = mcm.getRcfi(url1);
      rcfi.setChksum("SHA-256:1234");
      hcf.setConfigManager(mcm);
      hcf.setResponseCode(404);
      assertTrue(remoteCopy.exists());
      try {
	Configuration config = hcf.getConfiguration();
      } catch (FileNotFoundException e) {
	assertMatchesRE("404", e.getMessage());
      }
    }

    @Test
    public void testLocalFailoverGoodChecksum()
	throws IOException, NoSuchAlgorithmException {
      String url1 = "http://foo.bar/lockss.xml";
      File remoteCopy = gzippedTempFile(xml1, ".xml");
      MyHttpConfigFile hcf =
	new MyHttpConfigFile(url1, new StringInputStream(""));
      MyConfigManager mcm = newMyConfigManager();
      mcm.setPermFile(url1, remoteCopy);
      RemoteConfigFailoverInfo rcfi = mcm.getRcfi(url1);
      String alg =
	ConfigManager.DEFAULT_REMOTE_CONFIG_FAILOVER_CHECKSUM_ALGORITHM;
      MessageDigest md = MessageDigest.getInstance(alg);
      md.update(xml1.getBytes(Constants.DEFAULT_ENCODING));
      rcfi.setChksum(hashGzippedString(xml1, alg).toString());
      hcf.setConfigManager(mcm);
      hcf.setResponseCode(404);
      assertTrue(remoteCopy.exists());
      Configuration config = hcf.getConfiguration();
    }
  }

  /** HTTPConfigFile that uses a programmable MockLockssUrlConnection */
  protected static class MyHttpConfigFile extends HTTPConfigFile {
    Map map = new HashMap();
    String lastModified;
    String contentEncoding = null;
    int resp = 200;
    String respMsg = null;
    IOException executeExecption;
    Properties connReqHdrs = new Properties();
    String proxyHost = null;
    int proxyPort = -1;

    public MyHttpConfigFile(String url) {
      this(url, "");
    }

    public MyHttpConfigFile(String url, String content) {
      super(url, null);
      map.put(url, content);
      lastModified = TestConfigFileParent.dateString(TimeBase.nowMs());
    }

    public MyHttpConfigFile(String url, InputStream content) {
      super(url, null);
      map.put(url, content);
      lastModified = TestConfigFileParent.dateString(TimeBase.nowMs());
    }

    public void setContent(String content) {
      map.put(getFileUrl(), content);
    }

    public void setLastModified(String time) {
      lastModified = time;
    }

    protected LockssUrlConnection openUrlConnection(String url)
	throws IOException {
      MyMockLockssUrlConnection conn = new MyMockLockssUrlConnection();
      conn.setURL(url);
      return conn;
    }

    // Setters to control MyMockLockssUrlConnection

    void setLastModified(long time) {
      lastModified = TestConfigFileParent.dateString(time);
    }

    void setResponseCode(int code) {
      resp = code;
    }

    void setResponseMessage(String msg) {
      respMsg = msg;
    }

    void setExecuteException(IOException e) {
      executeExecption = e;
    }

    void setContentEncoding(String encoding) {
      contentEncoding = encoding;
    }

    class MyMockLockssUrlConnection extends MockLockssUrlConnection {

      public MyMockLockssUrlConnection() throws IOException {
	super();
      }

      public MyMockLockssUrlConnection(String url) throws IOException {
	super(url);
      }

      public void execute() throws IOException {
	super.execute();
	String url = getURL();

	Object o = map.get(url);
	if (o == null) {
	  this.setResponseCode(404);
	} else {
	  if (executeExecption != null) {
	    throw executeExecption;
	  }
	  String ifSince = getRequestProperty("if-modified-since");
	  if (ifSince != null && ifSince.equalsIgnoreCase(lastModified)) {
	    this.setResponseCode(HttpURLConnection.HTTP_NOT_MODIFIED);
	  } else {
	    if (o instanceof String) {
	      this.setResponseInputStream(new StringInputStream((String)o));
	    } else if (o instanceof InputStream) {
	      this.setResponseInputStream((InputStream)o);
	    } else {
	      throw new UnsupportedOperationException("Unknown result stream type " + o.getClass());
	    }
	    this.setResponseHeader("last-modified", lastModified);
	    if (contentEncoding != null) {
	      this.setResponseHeader("Content-Encoding", contentEncoding);
	    }
	    this.setResponseCode(resp);
	    this.setResponseMessage(respMsg);
	  }
	}
      }

      public void setRequestProperty(String key, String value) {
	super.setRequestProperty(key, value);
	connReqHdrs.setProperty(key, value);
      }

      public String getResponseContentEncoding() {
	return contentEncoding;
      }
      public long getResponseContentLength() {
	String url = getURL();

	Object o = map.get(url);
	if (o != null &&o instanceof String) {
	  return ((String)o).length();
	}
	return 0;
      }

      public boolean canProxy() {
	return true;
      }

      @Override
      public void setProxy(String host, int port) {
	MyHttpConfigFile.this.proxyHost = host;
	MyHttpConfigFile.this.proxyPort = port;
      }
    }
  }

  // Just enough ConfigManager for HTTPConfigFile to save and load remote
  // config failover files
  protected static class MyConfigManager extends ConfigManager {
    File tmpdir;
    Map<String,File> tempfiles = new HashMap<String,File>();
    Map<String,File> permfiles = new HashMap<String,File>();
    Map<String,RemoteConfigFailoverInfo> rcfis =
      new HashMap<String,RemoteConfigFailoverInfo>();

    public MyConfigManager(File tmpdir) {
      this.tmpdir = tmpdir;
    }

    public MyConfigManager(File tmpdir, List<String> bootstrapPropsUrls,
			   String restConfigServiceUrl, List<String> urls,
			   String groupNames) {
      super(bootstrapPropsUrls, restConfigServiceUrl, urls, groupNames);
      this.tmpdir = tmpdir;
    }

    @Override
    public RemoteConfigFailoverInfo getRemoteConfigFailoverWithTempFile(String url) {
      return getRcfi(url);
    }
    @Override
    public File getRemoteConfigFailoverFile(String url) {
      RemoteConfigFailoverInfo rcfi = getRcfi(url);
      return new File(rcfi.filename);
    }
    @Override
    public RemoteConfigFailoverInfo getRcfi(String url) {
      RemoteConfigFailoverInfo rcfi = rcfis.get(url);
      if (rcfi == null) {
	rcfi = new MyRemoteConfigFailoverInfo(url, tmpdir, 1);
	rcfis.put(url, rcfi);
      }
      return rcfi;
    }

    void setTempFile(String url, File file) {
      RemoteConfigFailoverInfo rcfi = getRcfi(url);
      rcfi.tempfile = file;
    }

    void setPermFile(String url, File file) {
      RemoteConfigFailoverInfo rcfi = getRcfi(url);
      rcfi.filename = file.getAbsolutePath();
    }
  }

  protected static class MyRemoteConfigFailoverInfo extends RemoteConfigFailoverInfo {
    MyRemoteConfigFailoverInfo(String url, File dir, int seq) {
      super(url, dir, seq);
    }

    File getPermFileAbs() {
      return new File(filename);
    }
  }
}
