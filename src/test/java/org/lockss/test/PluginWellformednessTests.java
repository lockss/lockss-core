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

package org.lockss.test;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.nio.file.FileVisitResult.CONTINUE;

import org.apache.commons.lang3.tuple.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.app.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.plugin.definable.*;
import org.lockss.state.*;
import org.lockss.extractor.*;
import org.lockss.util.test.FileTestUtil;
import org.lockss.plugin.PluginManager.PluginInfo;

/** Performs basic well-formedness tests on one or more plugins.  The
 * set of plugins or plugin jars is specified on the command line.  If
 * plugin names are given (-p or -pd) they are loaded from the
 * classpath.  If plugin jars are given (-pj or -pjd) they are loaded
 * as normal packaged plugin jars and all the plugins found in them are tested
 *
 * This is invoked as a main, not a unit test.  It extends
 * LockssTestCase in order to use its MockLockssDaemon and setup
 * features.
 */
public final class PluginWellformednessTests extends LockssTestCase {
  static L4JLogger log = L4JLogger.getLogger();

  protected MockLockssDaemon daemon;
  protected MyPluginManager pluginMgr;
  protected String curPluginId;
  protected Plugin plugin;
  protected boolean emptyJarOk = false;

  public void setEmptyJarOk(boolean val) {
    emptyJarOk = val;
  }

  public void testPlugins(List<String> pluginIds) throws Exception {
    setUp();
//     pcp();
    List<Pair<String,String>> failed = new ArrayList<>();
    for (String pluginId : pluginIds) {
      try {
 	log.debug("Testing plugin: " + pluginId);
	resetAndTest(pluginId);
      } catch (PluginFailedToLoadException e) {
	log.error("Plugin " + pluginId + " couldn't be loaded", e);
	failed.add(new ImmutablePair(pluginId, e.toString()));
      } catch (Exception e) {
	log.error("Plugin " + pluginId + " failed", e);
	failed.add(new ImmutablePair(pluginId, e.getMessage()));
      }
    }
    if (!failed.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(StringUtil.numberOfUnits(failed.size(), "plugin") + " failed:");
      for (Pair<String,String> f : failed) {
        if (f.getLeft() != null) {
          sb.append("\n  ");
          sb.append(f.getLeft());
        }
	sb.append("\n    ");
	sb.append(f.getRight());
      }
      throw new MalformedPluginException(sb.toString());
    }
  }

  private void pcp() {
    ClassPathUtil cpu = new ClassPathUtil();
    cpu.setWhichPath(ClassPathUtil.Which.Class);
    cpu.printClasspath();
    System.out.println("classpath: end");
  }

  public void testJars(List<String> jarPaths) throws Exception {
    setUp();
//     pcp();
    List<Pair<String,String>> failed = new ArrayList<>();
    for (String jarPath : jarPaths) {
      try {
        log.debug("Testing jar: " + jarPath);
        Collection<PluginInfo> pInfos = loadJar(jarPath);
        for (PluginInfo pi : pInfos) {
          if (pi.isError()) {
            failed.add(new ImmutablePair(curPluginId,
                                         pi.getError().toString()));
          } else {
            Plugin plug = pi.getPlugin();
            log.debug("Testing plugin: " + plug.getPluginName());
            resetAndTest(plug.getPluginId());
          }
        }
      } catch (Exception e) {
	log.error("Jar " + failed + " jarPath", e);
	failed.add(new ImmutablePair(curPluginId, e.getMessage()));
      }
    }
    if (!failed.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(StringUtil.numberOfUnits(failed.size(), "plugin") + " failed:");
      for (Pair<String,String> f : failed) {
        if (f.getLeft() != null) {
          sb.append("\n  ");
          sb.append(f.getLeft());
        }
	sb.append("\n    ");
	sb.append(f.getRight());
      }
      throw new MalformedPluginException(sb.toString());
    }
  }


  public void setUp() throws Exception {
    super.setUp();
    setUpDiskSpace();

    daemon = getMockLockssDaemon();
    daemon.suppressStartAuManagers(false);
    pluginMgr = new MyPluginManager();
    daemon.setPluginManager(pluginMgr);
    pluginMgr.initService(daemon);
    pluginMgr.startService();
    daemon.setAusStarted(true);
    daemon.setAppRunning(true);
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  protected MockLockssDaemon newMockLockssDaemon() {
    return new MyMockLockssDaemon();
  }

  public class MyMockLockssDaemon extends MockLockssDaemon {
    protected MyMockLockssDaemon() {
      super();
    }

  }

  protected Plugin getPlugin() throws IOException, MalformedPluginException {
    if (plugin == null) {
      plugin = pluginMgr.getPluginFromId(curPluginId);
    }
    return plugin;
  }

  protected Collection<PluginInfo> loadJar(String jarName)
      throws IOException, MalformedPluginException {
    Map<String,PluginInfo> infoMap = new HashMap<>();
    File jarFile = new File(jarName);
    String jarUrl = FileTestUtil.urlOfFile(jarName);
    MockCachedUrl mcu = new MockCachedUrl(jarUrl, jarName, false);
    MockArchivalUnit mau = new MockArchivalUnit();
    Collection<PluginInfo> pInfos =
      pluginMgr.loadPluginsFromJar(jarFile, jarUrl, mau, mcu, infoMap);
    if (!emptyJarOk && pInfos.isEmpty()) {
      throw new MalformedPluginException("No plugins found in jar: " + jarName);
    }
    pluginMgr.installPlugins(infoMap);
    return pInfos;
  }

  protected Configuration getSampleAuConfig() throws
  IOException, MalformedPluginException {
    Configuration config = ConfigManager.newConfiguration();
    for (ConfigParamDescr descr : getPlugin().getAuConfigDescrs()) {
      config.put(descr.getKey(), descr.getSampleValue());
    }
    return config;
  }

  protected ArchivalUnit createAu()
      throws ArchivalUnit.ConfigurationException,
             IOException, MalformedPluginException {
    ArchivalUnit au = PluginTestUtil.createAu(curPluginId, getSampleAuConfig());
    return au;
  }

  // Hack to reset the local state in order to test a batch of the
  // plugins in one invocation (much faster than invoking for each
  // one).

  void resetAndTest(String pluginId) throws Exception {
    this.curPluginId = pluginId;
    plugin = null;
    testWellFormed(pluginId);
  }

  void resetAndTestJar(String pluginId) throws Exception {
    this.curPluginId = pluginId;
    plugin = null;
    testWellFormed(pluginId);
  }

  /** Load the named plugin, create an AU using sample parameters and
   * access all of its elements to ensure all the patterns are well formed
   * and the factories are loadable and runnable.
   */
  public void testWellFormed(String pluginId) throws Exception {
    Plugin plugin = getPlugin();
    if (plugin == null) {
      PluginManager.PluginInfo info =
        PluginTestUtil.findPluginOrThrow(pluginId, null);
      if (info.isError()) {
        throw info.getError();
      }
      plugin = info.getPlugin();
      if (plugin == null) {
        throw new PluginFailedToLoadException("Plugin failed to load: " +
                                              pluginId);
      }
    }
    PluginValidator pv = new PluginValidator(daemon, pluginId, plugin);
    pv.validatePlugin();
  }

  public void validatePlugin(Plugin plugin) throws Exception {
    PluginValidator pv = new PluginValidator(daemon, curPluginId, plugin);
    pv.validatePlugin();
   }

  static List<String> findPluginsInTree(String plugRoot) {
    Path dirPath = Paths.get(plugRoot);

    List<String> res = new ArrayList<>();
    PluginFileVisitor visitor =
      new PluginFileVisitor(res, dirPath)
//       .setExclusions(argExcludePats);
      ;
    try {
      log.debug("starting tree walk ...");
      Files.walkFileTree(dirPath, EnumSet.of(FOLLOW_LINKS), 100, visitor);
    } catch (IOException e) {
      throw new RuntimeException("unable to walk source tree.", e);
    }
    return res;
  }

  static List<String> findPluginJarsInTree(String root) {
    Path dirPath = Paths.get(root);

    List<String> res = new ArrayList<>();
    JarFileVisitor visitor =
      new JarFileVisitor(res, dirPath)
//       .setExclusions(argExcludePats);
      ;
    try {
      log.debug("starting tree walk ...");
      Files.walkFileTree(dirPath, EnumSet.of(FOLLOW_LINKS), 100, visitor);
    } catch (IOException e) {
      throw new RuntimeException("unable to walk source tree.", e);
    }
    return res;
  }

  /** Visitor for Files.walkFileTree(), makes a PlugSpec for each plugin in
   * tree */
  static class PluginFileVisitor extends SimpleFileVisitor<Path> {
    List<String> res;
    Path root;
    List<Pattern> excludePats;
    List<String> excluded = new ArrayList<>();

    PluginFileVisitor(List<String> res, Path root) {
      this.res = res;
      this.root = root;
    }

    static Pattern PLUG_PAT =
      Pattern.compile("(\\w+)\\.xml$", Pattern.CASE_INSENSITIVE);

    PluginFileVisitor setExclusions(List<Pattern> excludePats) {
      this.excludePats = excludePats;
      return this;
    }

    boolean isExcluded(String id) {
      if (excludePats == null) return false;
      for (Pattern pat : excludePats) {
	if (pat.matcher(id).matches()) return true;
      }
      return false;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
	throws IOException {

      Matcher mat = PLUG_PAT.matcher(file.getFileName().toString());
      if (mat.matches()) {
	String fname = mat.group(1);
	Path rel = root.relativize(file).getParent();
	String pkg = rel.toString().replace("/", ".");
 	log.debug2("file: {}, fname: {}, rel: {}, pkg: {}", file, fname, rel, pkg);
	String fqPlug = pkg + "." + fname;
	if (isExcluded(fqPlug)) {
	  excluded.add(fqPlug);
	} else {
	  res.add(fqPlug);
	}
      }
      return CONTINUE;
    }
  }

  /** Visitor for Files.walkFileTree(), makes a PlugSpec for each plugin in
   * tree */
  static class JarFileVisitor extends SimpleFileVisitor<Path> {
    List<String> res;
    Path root;
    List<Pattern> excludePats;
    List<String> excluded = new ArrayList<>();

    JarFileVisitor(List<String> res, Path root) {
      this.res = res;
      this.root = root;
    }

    JarFileVisitor setExclusions(List<Pattern> excludePats) {
      this.excludePats = excludePats;
      return this;
    }

    boolean isExcluded(String id) {
      if (excludePats == null) return false;
      for (Pattern pat : excludePats) {
	if (pat.matcher(id).matches()) return true;
      }
      return false;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
	throws IOException {
      String pathStr = file.toString();

      if (StringUtil.endsWithIgnoreCase(pathStr, ".jar")) {
	if (isExcluded(pathStr)) {
	  excluded.add(pathStr);
	} else {
	  res.add(pathStr);
	}
      }
      return CONTINUE;
    }
  }

  private class MyPluginManager extends PluginManager {
    CachedUrl cu;
    protected void processOneRegistryJar(CachedUrl cu, String url,
					 ArchivalUnit au, Map tmpMap) {
      super.processOneRegistryJar(cu, url, au, tmpMap);
    }

    protected Collection<PluginInfo>
      loadPluginsFromJar(File jarFile, String url,
                         ArchivalUnit au, CachedUrl cu,
                         Map<String,PluginInfo> tmpMap) {
      return super.loadPluginsFromJar(jarFile, url, au, cu, tmpMap);
    }
    
    void installPlugins(Map<String,PluginInfo> map) {
      for (Map.Entry<String,PluginInfo> entry : map.entrySet()) {
	PluginInfo info = entry.getValue();
        if (info.isError()) {
          log.warn("Skipping error: {}", info);
        } else {
          String key = entry.getKey();
          log.debug2("Adding to plugin map: " + key);
          Plugin newPlug = info.getPlugin();
          setPlugin(key, newPlug);
        }
      }
    }      
  }

  public static void main(String[] argv) throws Exception {
    List<String> pluginIds = new ArrayList<>();
    List<String> pluginJars = new ArrayList<>();
    log.debug2("args: {}", ListUtil.list(argv));
    PluginWellformednessTests pwt = new PluginWellformednessTests();


    if (argv.length > 0) {
      int ix = 0;
      try {
	for (ix = 0; ix < argv.length; ix++) {
	  String arg = argv[ix];
          if (arg.equals("-e")) {
            pwt.setEmptyJarOk(true);
          } else if (arg.equals("-p")) {
            pluginIds.add(argv[++ix]);
          } else if (arg.equals("-pd")) {
            String plugTree = argv[++ix];
            pluginIds.addAll(findPluginsInTree(plugTree));
          } else if (arg.equals("-pj")) {
            pluginJars.add(argv[++ix]);
          } else if (arg.equals("-pjd")) {
            String plugJarTree = argv[++ix];
            pluginJars.addAll(findPluginJarsInTree(plugJarTree));
          } else {
            log.fatal("Illegal command line: {}", ListUtil.list(argv));
            usage();
	  }
	}
      } catch (ArrayIndexOutOfBoundsException e) {
        log.fatal("Illegal command line: {}", ListUtil.list(argv), e);
	usage();
      }
      if (!pluginIds.isEmpty() && !pluginJars.isEmpty()) {
        usage("Error: Can't specify both plugin names and plugin jars");
      }
      int ret;
      try {
        if (!pluginIds.isEmpty()) {
          pwt.testPlugins(pluginIds);
        } else if (!pluginJars.isEmpty()) {
          pwt.testJars(pluginJars);
        } else {
          log.warn("No plugins specified, exiting.");
        }
      } catch (MalformedPluginException e) {
        System.exit(1);
      }
    }

  }

  private static void usage() {
    usage(null);
  }

  private static void usage(String msg) {
    PrintStream o = System.out;
    if (msg != null) {
      o.println(msg);
    }
    o.println("Usage: java PluginWellformednessTests" +
	      " [-e]" +
	      " [-p <plugin_id>]" +
	      " [-pd <plugin_dir>]" +
	      " [-pj <plugin_jar_path>]" +
              " [-pjd <plugin_jars_dir>] ...");
    o.println("  -e                   no error for jars w/ no plugins");
    o.println("  -p <plugin_id>       id of plugin(s) on classpath");
    o.println("  -pd <plugin_dir>     root of tree of compiled plugins (e.g., target/classes)");
    o.println("  -pj plugin_jar_path  packaged plugin jar");
    o.println("  -pjd plugin_jar_dir  root of tree of packaged plugin jars");
    System.exit(2);
  }

  public static class PluginFailedToLoadException extends Exception {
    public PluginFailedToLoadException(String msg) {
      super(msg);
    }
  }

  public static class MalformedPluginException extends Exception {
    public MalformedPluginException(String msg) {
      super(msg);
    }
  }
}
