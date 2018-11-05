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

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.jar.*;
import java.util.stream.*;
import org.apache.commons.collections.*;
import org.apache.commons.lang3.tuple.*;

import org.lockss.log.*;
import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.plugin.definable.*;
import org.lockss.test.*;

/**
 * Create and optionally sign plugin jar.
 *
 * Includes all parent plugins and all files in plugin and parent plugin
 * dirs.
 */

// Plugin files and dirs are represented by URLs rather than Files in order
// to support jars.  (E.g., a 3rd-party plugin may extend a LOCKSS base
// plugin, which may be distributed in a jar so as not to require
// installing a source distribution.)  Not all aspects of jar:file: URLs
// are supported yet.

// Plugins are loaded from the classpath.  May need to add a -cp arg in
// order to pass in a classpath

public class PluginPackager {
  static L4JLogger log = L4JLogger.getLogger();

  static final String MANIFEST = JarFile.MANIFEST_NAME;
  static final String MANIFEST_DIR = "META-INF/";
  static final String VERSION = "1.0";
  static final char SEPARATOR = File.separatorChar;

  MockLockssDaemon daemon;
  PluginManager pluginMgr;
  File tmpdir;

  List<String> pluginIds;
  List<PData> pds = new ArrayList<>();

  String jarPath;
  JarOutputStream jarOut;

  public PluginPackager(String outputJar, List<String> plugins) {
    this.jarPath = outputJar;
    this.pluginIds = plugins;
  }

  // Necessary setup in order to invoke PluginManager to load plugins
  private void init() throws Exception {
    ConfigManager.makeConfigManager();
    Logger.resetLogs();
    tmpdir = FileUtil.createTempDir("pluginpkg", "");
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir.getAbsolutePath() + File.separator);
    daemon =  new MyMockLockssDaemon();
    pluginMgr = new PluginManager();
    daemon.setPluginManager(pluginMgr);
    pluginMgr.initService(daemon);
    pluginMgr.startService();
  }


  public int makeJar() {
    try {
      init();
      findPlugins();
      initJar();
      // XXX check file dates against jar date, exit if jar is up-to-date
      writeManifest();
      writeFiles();
      jarOut.close();
    } catch (Exception e) {
      log.error("Failed", e);
      return 1;
    } finally {
      FileUtil.delTree(tmpdir);
    }
    return 0;
  }

  // load all the specified plugins, find the URL each plugin and its
  // parents
  void findPlugins() {
    for (String pluginId : pluginIds) {
      Plugin plug = PluginTestUtil.findPlugin(pluginId);
      log.debug("Loaded plugin: " + plug);
      if (plug == null) {
	log.error("Plugin {} not found", pluginId);
	throw new FatalError("Failed");
      }
      List<PkgUrl> plugUrls = getOnePluginUrls(packageOf(pluginId), plug);
      log.debug2("plugin {} URLs: {}", pluginId, plugUrls);
      pds.add(new PData(pluginId, plugUrls));
    }
  }

  // Retrieve the package and URL of the plugin and its parents
  List<PkgUrl> getOnePluginUrls(String pkg, Plugin plug) {
    ArrayList<PkgUrl> res = new ArrayList<>();
    if (plug instanceof DefinablePlugin) {
      DefinablePlugin dplug = (DefinablePlugin)plug;
      for (Pair<String,URL> pair : dplug.getIdsUrls()) {
	res.add(new PkgUrl(packageOf(pair.getLeft()), pair.getRight()));
      }
    } else {
      res.add(new PkgUrl(pkg, findJavaPluginUrl(plug)));
    }
    return res;
  }

  URL findJavaPluginUrl(Plugin plug) {
    ClassLoader loader = this.getClass().getClassLoader();
    String classFileName = plug.getClass().getName().replace('.', '/');
    return loader.getResource(classFileName + ".class");
  }

  void initJar() throws IOException {
    OutputStream out = new BufferedOutputStream(new FileOutputStream(jarPath));
    jarOut = new JarOutputStream(out);
  }

  void writeManifest() throws IOException {
    Manifest manifest = new Manifest();
    Attributes mainAttr = manifest.getMainAttributes();
    mainAttr.put(Attributes.Name.MANIFEST_VERSION, VERSION);
    String javaVendor = System.getProperty("java.vendor");
    String jdkVersion = System.getProperty("java.version");
    mainAttr.put(new Attributes.Name("Created-By"),
	       jdkVersion + " (" + javaVendor + ")");

    // Add entry that marks a LOCKSS plugin file
    for (PData pd : pds) {
      String secName = pd.getPluginPath();
      manifest.getEntries().put(secName, new Attributes());
      Attributes plugAttr = manifest.getAttributes(secName);
      plugAttr.put(new Attributes.Name("Lockss-Plugin"), "true");
    }

    JarEntry e = new JarEntry(MANIFEST_DIR);
    long now = System.currentTimeMillis();
    e.setTime(now);
//     e.setSize(0);
//     e.setCrc(0);
    jarOut.putNextEntry(e);
    e = new JarEntry(MANIFEST);
    e.setTime(now);
    jarOut.putNextEntry(e);
    manifest.write(jarOut);
    jarOut.closeEntry();

  }

  // write all files in plugin dir and parent plugin dirs
  void writeFiles() throws Exception {
    Set<URL> urlsAdded = new HashSet<>();
    Set<String> dirsAdded = new HashSet<>();

    for (PData pd : pds) {
      for (PkgUrl pu : pd.listFiles()) {
	URL url = pu.getUrl();
	if (urlsAdded.add(url)) {
	  if (url.getProtocol().equalsIgnoreCase("file")) {
	    String path = url.getPath();
	    File f = new File(path);
	    if (f.isDirectory()) continue;
	    String relPath = pathOf(pu.getPkg());
	    String dir = f.getParent();
	    if (dirsAdded.add(dir)) {
	      log.debug("Adding dir {}", relPath);
	      String entPath = relPath + "/";
	      JarEntry entry = new JarEntry(entPath);
	      entry.setTime(f.lastModified());
	      jarOut.putNextEntry(entry);
	      jarOut.closeEntry();
	    }
	    String entPath = relPath + "/" + f.getName();
	    log.debug("Adding file {}", entPath);
	    JarEntry entry = new JarEntry(entPath);
	    entry.setTime(f.lastModified());
	    jarOut.putNextEntry(entry);
	    try (InputStream in =
		 new BufferedInputStream(new FileInputStream(path))) {
	      StreamUtil.copy(in, jarOut);
	    }
	    jarOut.closeEntry();
	  }
	}
      }
    }
  }


  public static void main(String[] argv) {
    String jar = null;
    List<String> files = new ArrayList<String>();
    List<String> plugins = new ArrayList<String>();

    if (argv.length == 0) {
      usage();
    }
    try {
      for (int ix = 0; ix < argv.length; ix++) {
	String arg = argv[ix];
	if        (arg.startsWith("-o")) {
	  jar = argv[++ix];
// 	} else if (arg.startsWith("-d")) {
// 	  path = argv[++ix];
	} else if (arg.startsWith("-p")) {
	  plugins.add(argv[++ix]);
	} else if (arg.startsWith("-")) {
	  usage();
	} else {
	  files.add(arg);
	}
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      usage();
    }
    if (jar == null || plugins.isEmpty()) {
      usage();
    }
    PluginPackager it = new PluginPackager(jar, plugins);
    System.exit(it.makeJar());
  }

  static void usage() {
    System.err.println("Usage: PluginPackager -o output_jar -p plugin_name");
    System.err.println("     -o <output_jar>   name of plugin jar file to write");
    System.err.println("     -p <plugin_name>   fq plugin name (must be on classpath)");
    System.exit(1);
  }

  static URL dirOfUrl(URL url) throws URISyntaxException, IOException{
    URI uri = new URI(url.toString());
    URI parent = uri.getPath().endsWith("/")
      ? uri.resolve("..") : uri.resolve(".");
    return new URL(parent.toString());
  }


  /** find all the files in the directory of the supplied URL, recording
   * the package name to make it easier to generate the necessary relative
   * paths for the jar */
  static List<PkgUrl> listFilesInDirOf(PkgUrl pu) {
    try {
      URL dirURL = dirOfUrl(pu.getUrl());
      if (dirURL.getProtocol().equals("file")) {
	/* A file path: easy enough */
	String[] files = new File(dirURL.toURI()).list();
	List<PkgUrl> res = new ArrayList<>();
	for (String f : files) {
	  res.add(new PkgUrl(pu.getPkg(), new URL(dirURL, f)));
	}
	return res;
      }
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }

//     } else if (dirURL.getProtocol().equals("jar")) {
//       /* A JAR path */

    // Code found by google search, not adapted yet.

//       String jarPath = dirURL.getPath().substring(5, dirURL.getPath().indexOf("!")); //strip out only the JAR file
//       JarFile jar = new JarFile(URLDecoder.decode(jarPath, "UTF-8"));
//       Enumeration<JarEntry> entries = jar.entries(); //gives ALL entries in jar
//       Set<String> result = new HashSet<String>(); //avoid duplicates in case it is a subdirectory
//       while(entries.hasMoreElements()) {
// 	String name = entries.nextElement().getName();
// 	if (name.startsWith(path)) { //filter according to the path
// 	  String entry = name.substring(path.length());
// 	  int checkSubdir = entry.indexOf("/");
// 	  if (checkSubdir >= 0) {
// 	    // if it is a subdirectory, we just return the directory name
// 	    entry = entry.substring(0, checkSubdir);
// 	  }
// 	  result.add(entry);
// 	}
//       }
//       return result.toArray(new String[result.size()]);
    throw new UnsupportedOperationException("Cannot list files for URL "+
					    pu.getUrl());
  }

  /** Return the package part of the fq name */
  static String packageOf(String name) {
    return StringUtil.upToFinal(name, ".");
  }

  /** Convert a package name to a file path */
  static String pathOf(String pkg) {
    return pkg.replace('.', '/');
  }

  static String toString(Attributes attr) {
    List l = new ArrayList();
    for (Map.Entry ent : attr.entrySet()) {
      l.add(ent.getKey() + "=" + ent.getValue());
    }
    return StringUtil.separatedString(l, "[", ", ", "]");
  }

  /** Plugin package and URL pair */
  static class PkgUrl {
    String pkg;
    URL url;

    PkgUrl(String pkg, URL url) {
      this.pkg = pkg;
      this.url = url;
    }

    String getPkg() {
      return pkg;
    }

    URL getUrl() {
      return url;
    }
  }

  /** Info about a single plugin */
  static class PData {
    String pluginId;
    List<PkgUrl> pluginUrls;
    String file;

    PData(String id, List<PkgUrl> urls) {
      this.pluginId = id;
      this.pluginUrls = urls;
    }

    String getPackagePath() {
      return pathOf(packageOf(pluginId));
    }

    String getPluginPath() throws IOException {
      String plugPath = pluginUrls.get(0).getUrl().getPath();
      String plugName = new File(plugPath).getName();
      return getPackagePath() + "/" + plugName;
    }

    List<PkgUrl> listFiles() throws URISyntaxException, IOException {
      return pluginUrls.stream()
	.flatMap(pu -> listFilesInDirOf(pu).stream())
	.collect(Collectors.toList());
    }


  }

  public class MyMockLockssDaemon extends MockLockssDaemon {
    protected MyMockLockssDaemon() {
      super();
    }
  }

  static class FatalError extends RuntimeException {
    public FatalError(String message) {
      super(message);
    }

    public FatalError(Exception e) {
      super(e);
    }

    public FatalError(String message, Exception e) {
      super(message, e);
    }
  }

}
