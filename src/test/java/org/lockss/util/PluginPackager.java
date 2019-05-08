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
import java.util.regex.*;
import java.util.stream.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import org.apache.commons.collections.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.*;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.nio.file.FileVisitResult.CONTINUE;

import org.lockss.log.*;
import org.lockss.config.*;
import org.lockss.util.*;
import org.lockss.util.lang.*;
import org.lockss.plugin.*;
import org.lockss.plugin.definable.*;
import org.lockss.test.*;

/**
 * Create and optionally sign plugin jar(s).
 *
 * Includes all parent plugins and all files in plugin and parent plugin
 * dirs. Plugins are loaded from a user-specified classpath, if supplied,
 * or the current classpath.
 *
 * This is a driver class which can be invoked by main() or
 * programmatically to build one or more plugin jars.
 */

// Plugin files and dirs are represented by URLs rather than Files in order
// to support jars.  (E.g., a 3rd-party plugin may extend a LOCKSS base
// plugin, which may be distributed in a jar so as not to require
// installing a source distribution.)  But jar:file: URLs aren't fully
// supported yet.

public class PluginPackager {
  static L4JLogger log = L4JLogger.getLogger();

  static final String MANIFEST = JarFile.MANIFEST_NAME;
  static final String MANIFEST_DIR = "META-INF/";
  static final String VERSION = "1.0";

  // Match the signature file in a signed jar
  static Pattern SIG_FILE_PAT =
    Pattern.compile("META-INF/[^/.]+\\.SF$", Pattern.CASE_INSENSITIVE);


  MockLockssDaemon daemon;
  PluginManager pluginMgr;
  File tmpdir;
  ClassLoader gLoader = null;
  boolean forceRebuild = false;
  List<Result> results = new ArrayList<>();
  boolean nofail = false;
  List<String> excluded = new ArrayList<>();

  List<PlugSpec> argSpecs = new ArrayList<>();
  File argPlugDir;			// root of plugins class tree
  File argOutputDir;			// dir (flat) to write plugin jars
  List<Pattern> argExcludePats = new ArrayList<>();

  List<String> argClasspath = null;
  boolean argForceRebuild = false;

  String argKeystore = null;
  String argKeyPass = "password";
  String argStorePass = "password";
  String argAlias = null;


  public PluginPackager() {
  }

  /** Compatibility with maven plugin.  Remove. */
  public PluginPackager(String outputJar, List<String> plugins) {
    addSpec(new PlugSpec().addPlugs(plugins).setJar(outputJar));
  }

  /** Set the directory to which plugin jars will be written.  Used with
   * {@link #setPluginDir(File)} */
  public PluginPackager setOutputDir(File argOutputDir) {
    this.argOutputDir = argOutputDir;
    return this;
  }

  /** Set the root of the directory tree to search for plugins and compiled
   * classes.  Used with {@link #setOutputDir(File)} */
  public PluginPackager setPluginDir(File argPlugDir) {
    this.argPlugDir = argPlugDir;
    return this;
  }

  public File getPlugDir() {
    return argPlugDir;
  }

  /** Add to list of plugin jars to be built */
  public PluginPackager addSpec(PlugSpec spec) {
    argSpecs.add(spec);
    return this;
  }

  /** Set the classpath to search for and load plugins from.  Will be
   * prepended to classpath used to invoke PluginPackager */
  public PluginPackager setClassPath(List<String> cp) {
    argClasspath = cp;
    return this;
  }

  /** Set the classpath to search for and load plugins from.  Will be
   * prepended to classpath used to invoke PluginPackager */
  public PluginPackager setClassPath(String cpstr) {
    setClassPath(StringUtil.breakAt(cpstr, ":"));
    return this;
  }

  /** Force jars to be rebuilt even if they exist and are up-to-date */
  public PluginPackager addExclusion(String patstr) {
    Pattern pat = Pattern.compile(patstr);
    argExcludePats.add(pat);
    return this;
  }

  /** Force jars to be rebuilt even if they exist and are up-to-date */
  public PluginPackager setForceRebuild(boolean force) {
    argForceRebuild = force;
    return this;
  }

  public PluginPackager setNoFail(boolean val) {
    this.nofail = val;
    return this;
  }

  public boolean isNoFail() {
    return nofail;
  }

  /** Set the pat of the plugin signing keystore.  Plugins will be signed
   * if this is provided */
  public PluginPackager setKeystore(String ks) {
    argKeystore = ks;
    return this;
  }

  /** Set the keystore alias.  Must be supplied if keystore is. */
  public PluginPackager setAlias(String alias) {
    argAlias = alias;
    return this;
  }

  /** Set the password to use with the signing key.  Defaults to
   * "password". */
  public PluginPackager setKeyPass(String val) {
    argKeyPass = val;
    return this;
  }

  /** Set the password to use to unlock the keystore.  Defaults to
   * "password". */
  public PluginPackager setStorePass(String val) {
    argStorePass = val;
    return this;
  }

  public List<Result> getResults() {
    return results;
  }

  public List<String> getExcluded() {
    return excluded;
  }

  /** Check the args and build all the plugins specified */
  public void build() throws Exception {
    List<PlugSpec> specs = argSpecs;

    if (specs.isEmpty()) {
      // No explicit specs, look for plugins dir and output dir
      if (argPlugDir != null && argOutputDir != null) {
	FileUtil.ensureDirExists(argOutputDir);
	// If no classpath specified, use plugin class dir
	if (argClasspath == null || argClasspath.isEmpty()) {
	  String cp = argPlugDir.toString();
	  argClasspath = ListUtil.list(cp.endsWith("/") ? cp : cp + "/");
	}
	// Find plugins in tree
	specs = findSpecsInDir(argPlugDir, argOutputDir);
      } else if (argPlugDir == null) {
	throw new IllegalArgumentException("No plugins or plugins-dir supplied");
      } else if (argOutputDir == null) {
	throw new IllegalArgumentException("Plugins-dir but no output-dir supplied");
      }
    } else if (argPlugDir != null) {
      throw new IllegalArgumentException("Can't specify both a list of plugins and a plugins-dir");
    }
    if (argKeystore != null && argAlias == null) {
      throw new IllegalArgumentException("keystore but no alias supplied");
    }
    if (argClasspath != null && !argClasspath.isEmpty()) {
      log.debug("Prepending classpath: {}", argClasspath);
    }


    init();
    for (PlugSpec ps : specs) {
      JarBuilder it = new JarBuilder(ps);
      try {
	it.setClassLoader(getLoader());
	it.makeJar();
	results.add(new Result(it));
      } catch (Exception e) {
	results.add(new Result(it).setException(e));
      }
    }
  }

  // Daemon uses new ClassLoader for each plugin but I don't think there's
  // any reason that would matter for this use.  This is here to make it
  // easy to switch if necessary.  Currently set to use same loader for all
  // plugins.
  boolean REUSE_LOADER = true;

  /** Return ClassLoader based on the supplied classpath.  If REUSE_LOADER
   * is true, the same instance will be returned on each call, else a new
   * instance each time */
  ClassLoader getLoader() {
    if (argClasspath == null || argClasspath.isEmpty()) {
      return null;
    }
    if (gLoader != null) {
      return gLoader;
    }
    ClassLoader res =
      new LoadablePluginClassLoader(ClassPathUtil.toUrlArray(argClasspath));
//     new URLClassLoader(ClassPathUtil.toUrlArray(argClasspath));
    if (REUSE_LOADER) {
      gLoader = res;
    }
    return res;
  }

  static String[] INFO_LOGS = {"PluginPackager"};

  // Necessary setup in order to invoke PluginManager to load plugins
  private void init() throws Exception {
    ConfigManager.makeConfigManager().setNoNag();
    tmpdir = FileUtil.createTempDir("pluginpkg", "");
    Properties p = new Properties();
//     p.setProperty(ConfigManager.PARAM_ENABLE_JMS_RECEIVE, "false");

    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
		  tmpdir.getAbsolutePath() + File.separator);
    if ("warning".equalsIgnoreCase(System.getProperty(LockssLogger.SYSPROP_DEFAULT_LOCKSS_LOG_LEVEL))) {
      for (String s : INFO_LOGS) {
	p.setProperty("org.lockss.log." + s + ".level", "info");
      }
    }
//     Logger.resetLogs();
    ConfigurationUtil.addFromProps(p);
    daemon =  new MyMockLockssDaemon();
    pluginMgr = new PluginManager();
    daemon.setPluginManager(pluginMgr);
    pluginMgr.initService(daemon);
    pluginMgr.startService();
  }

  /** Builds one plugin jar */
  class JarBuilder {

    PlugSpec spec;			// spec for what should go in jar

    List<PData> pds = new ArrayList<>(); // PData for each plugin in spec
    Collection<PkgUrl> allFiles;	 // all files to be written to jar

    JarOutputStream jarOut;

    ClassLoader oneLoader;

    Exception e = null;			// Exception thrown during processing

    boolean notModified = false;

    JarBuilder(PlugSpec ps) {
      this.spec = ps;
    }

    JarBuilder setClassLoader(ClassLoader loader) {
      this.oneLoader = loader;
      return this;
    }

    PlugSpec getPlugSpec() {
      return spec;
    }

    boolean isNotModified() {
      return notModified;
    }

    JarBuilder setException(Exception e) {
      this.e = e;
      return this;
    }

    public void makeJar() throws Exception {
      try {
	findPlugins();
	// check file dates against jar date, exit if jar is up-to-date
	if (isJarUpToDate() && (argKeystore == null || isJarSigned())) {
	  notModified = true;
	  return;
	}
	initJar();
	writeManifest();
	writeFiles();
	jarOut.close();
	if (argKeystore != null) {
	  signJar();
	  log.info("Wrote and signed {}", spec.getJar());
	} else {
	  log.info("Wrote {}", spec.getJar());
	}
      } catch (Exception e) {
	log.error("Failed", e);
	throw e;
      } finally {
	FileUtil.delTree(tmpdir);
      }
    }

    String SIGN_CMD = "jarsigner -keystore %s -keypass %s -storepass %s %s %s";

    public void signJar() throws IOException {
      String cmd =
	String.format(SIGN_CMD, argKeystore, argKeyPass, argStorePass,
		      spec.getJar(), argAlias);
      log.debug2("cmd: " + cmd);
      String s;
      Reader rdr = null;
      try {
	Process p = Runtime.getRuntime().exec(cmd);
	rdr =
	  new InputStreamReader(new BufferedInputStream(p.getInputStream()),
				EncodingUtil.DEFAULT_ENCODING);
	s = IOUtils.toString(rdr);
	int exit = p.waitFor();
	rdr.close();
	if (exit == 0) {
	  log.debug(s);
	} else {
	  throw new RuntimeException("jarsigner failed: " + s);
	}
      } catch (InterruptedException e) {
	log.error("jarsigner aborted", e);
	throw new RuntimeException("jarsigner aborted", e);
      } finally {
	IOUtil.safeClose(rdr);
      }
    }


    // load all the specified plugins, find the URL of each plugin and its
    // parents
    void findPlugins() throws Exception {
      for (String pluginId : spec.getPluginIds()) {
	log.debug("Loading plugin: " + pluginId);

// 	Plugin plug = PluginTestUtil.findPlugin(pluginId, oneLoader);
	Plugin plug = loadPlugin(pluginId, oneLoader);
	log.debug2("Loaded plugin: " + plug.getPluginId());
	List<PkgUrl> plugUrls = getOnePluginUrls(packageOf(pluginId), plug);
	log.debug2("plugin {} URLs: {}", pluginId, plugUrls);
	pds.add(new PData(pluginId, plugUrls));
      }
    }

    Plugin loadPlugin(String pluginId, ClassLoader loader) throws Exception {
      try {
	String key = pluginMgr.pluginKeyFromName(pluginId);
	PluginManager.PluginInfo pinfo = pluginMgr.loadPlugin(key, loader);
	return pinfo.getPlugin();
      } catch (Exception e) {
	throw e;
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
      ClassLoader loader = plug.getClass().getClassLoader();
      String classFileName = plug.getClass().getName().replace('.', '/');
      return loader.getResource(classFileName + ".class");
    }

    void initJar() throws IOException {
      OutputStream out =
	new BufferedOutputStream(new FileOutputStream(spec.getJarFile()));
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

    // find the entire set of files to be written to the jar
    Collection<PkgUrl> findAllFiles() throws Exception {
      if (allFiles == null) {
	Set<PkgUrl> res = new LinkedHashSet<>();

	for (PData pd : pds) {
	  for (PkgUrl pu : pd.listFiles()) {
	    res.add(pu);
	  }
	}
	allFiles = res;
      }
      return allFiles;
    }

    // return true if the jar exists and is as recent as the most input
    // file
    boolean isJarUpToDate() throws Exception {
      if (argForceRebuild || !spec.getJarFile().exists()) {
	return false;
      }
      long latest = findAllFiles().stream()
	.map(PkgUrl::getUrl)
	.map(u -> urlToFile(u))
	.map(File::lastModified)
	.mapToLong(v -> v)
	.max().orElseThrow(NoSuchElementException::new);
      return latest <= spec.getJarFile().lastModified();
    }


    /** Return true if the jar exists and is signed.  Currently just looks
     * for a file named META-INF/*.SF */
    boolean isJarSigned() throws Exception {
      if (!spec.getJarFile().exists()) {
	return false;
      }
      try (JarFile jf = new JarFile(spec.getJarFile())) {
	for (Enumeration<JarEntry> en = jf.entries(); en.hasMoreElements(); ) {
	  JarEntry ent = en.nextElement();
	  if (ent.isDirectory()) {
	    continue;
	  }
	  Matcher mat = SIG_FILE_PAT.matcher(ent.getName());
	  if (mat.matches()) {
	    return true;
	  }
	}
      }
      return false;
    }

    // write all files in plugin dir and parent plugin dirs
    void writeFiles() throws Exception {
      Set<URL> urlsAdded = new HashSet<>();
      Set<String> dirsAdded = new HashSet<>();

      for (PkgUrl pu : findAllFiles()) {
	URL url = pu.getUrl();
	if (urlsAdded.add(url)) {
	  // xxx check for not jar:file:
	  if (url.getProtocol().equalsIgnoreCase("file")) {
	    String path = url.getPath();
	    File f = new File(path);
	    if (f.isDirectory()) continue;
	    String relPath = pathOfPkg(pu.getPkg());
	    String dir = f.getParent();
	    if (dirsAdded.add(dir)) {
	      log.debug2("Adding dir {}", relPath);
	      String entPath = relPath + "/";
	      JarEntry entry = new JarEntry(entPath);
	      entry.setTime(f.lastModified());
	      jarOut.putNextEntry(entry);
	      jarOut.closeEntry();
	    }
	    String entPath = relPath + "/" + f.getName();
	    log.debug2("Adding file {}", entPath);
	    JarEntry entry = new JarEntry(entPath);
	    entry.setTime(f.lastModified());
	    jarOut.putNextEntry(entry);
	    try (InputStream in =
		 new BufferedInputStream(new FileInputStream(path))) {
	      StreamUtil.copy(in, jarOut);
	    }
	    jarOut.closeEntry();
	  } else {
	    throw new UnsupportedOperationException("Can't handle jar: URLs yet: " + url);
	  }
	}
      }
    }
  }

  /** Result status of one JarBuilder operation, for reporting */
  public class Result {
    JarBuilder bldr;
    Exception e;

    Result(JarBuilder bldr) {
      this.bldr = bldr;
    }

    Result setException(Exception e) {
      this.e = e;
      return this;
    }

    public PlugSpec getPlugSpec() {
      return bldr.getPlugSpec();
    }

    public Exception getException() {
      return e;
    }

    public boolean isError() {
      return e != null;
    }

    public boolean isNotModified() {
      return bldr.isNotModified();
    }
  }

  /** Package and URL pair.  Simpler to keep track of each file's package
   * from the beginning than to re-derive package from path and base
   * path. */
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

    public String toString() {
      return "[PkgUrl: " + pkg + ", " + url + "]";
    }
  }

  /** Spec (supplied by command line or client) for one plugin jar: one or
   * more pluginIds and an output jar name */
  static class PlugSpec {
    List<String> pluginIds = new ArrayList<>();
    String jar;

    PlugSpec addPlug(String plugId) {
      pluginIds.add(plugId);
      return this;
    }

    PlugSpec addPlugs(List<String> plugIds) {
      pluginIds.addAll(plugIds);
      return this;
    }

    PlugSpec setJar(String jar) {
      this.jar = jar;
      return this;
    }

    List<String> getPluginIds() {
      return pluginIds;
    }

    String getJar() {
      return jar;
    }

    File getJarFile() {
      return new File(jar);
    }

    public String toString() {
      return "[" + getPluginIds() + ", " + getJar() + "]";
    }
  }

  /** Info about a single plugin */
  class PData {
    String pluginId;
    List<PkgUrl> pluginUrls;	    // URLs of plugin and parent .xml files
    String file;

    PData(String id, List<PkgUrl> urls) {
      this.pluginId = id;
      this.pluginUrls = urls;
    }

    String getPackagePath() {
      return pathOfPkg(packageOf(pluginId));
    }

    String getPluginPath() throws IOException {
      String plugPath = pluginUrls.get(0).getUrl().getPath();
      String plugName = new File(plugPath).getName();
      return getPackagePath() + "/" + plugName;
    }

    /** Return list of PkgUrl for all files in dir of plugin and its
     * parents */
    List<PkgUrl> listFiles() {
      return pluginUrls.stream()
	.flatMap(pu -> listFilesInDirOf(pu).stream())
	.collect(Collectors.toList());
    }
  }

  /** Visitor for Files.walkFileTree(), makes a PlugSpec for each plugin in
   * tree */
  static class FileVisitor extends SimpleFileVisitor<Path> {
    List<PlugSpec> res;
    Path root;
    Path outDir;
    PathMatcher matcher;
    List<Pattern> excludePats;
    List<String> excluded = new ArrayList<>();

    FileVisitor(List<PlugSpec> res, Path root, Path outDir) {
      this.res = res;
      this.root = root;
      this.outDir = outDir;
      matcher = FileSystems.getDefault().getPathMatcher("glob:" + "*.xml");
    }

    static Pattern PLUG_PAT =
      Pattern.compile("(\\w+)\\.xml$", Pattern.CASE_INSENSITIVE);

    FileVisitor setExclusions(List<Pattern> excludePats) {
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

    List<String> getExcluded() {
      return excluded;
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
	  PlugSpec spec = new PlugSpec()
	    .addPlug(fqPlug)
	    .setJar(outDir.resolve(fqPlug + ".jar").toString());
	  res.add(spec);
	}
      }
      return CONTINUE;
    }
  }

  public class MyMockLockssDaemon extends MockLockssDaemon {
    protected MyMockLockssDaemon() {
      super();
    }
  }

  // Utility methods

  /** Convert URL to File */
  static File urlToFile(URL url) {
    if (url.getProtocol().equalsIgnoreCase("file")) {
      String path = url.getPath();
      return new File(url.getPath());
    }
    throw new IllegalArgumentException("Can't convert non file: URL to File: "
				       + url);
  }

  /** Return the URL for the containing directory of the URL */
  static URL dirOfUrl(URL url) throws URISyntaxException, IOException{
    if (!url.getProtocol().equalsIgnoreCase("file")) {
      throw new IllegalArgumentException("Can't handle non-file: URLs yet: " +
					 url);
    }
    URI uri = new URI(url.toString());
    // XXX if path is relative (e.g., file:target/classes), URI.getPath()
    // returns null.
//     log.debug3("url.getPath: {}", url.getPath());
//     log.debug3("getPath: {}", uri.getPath());
    URI parent = uri.getPath().endsWith("/")
      ? uri.resolve("..") : uri.resolve(".");
    URL res = new URL(parent.toString());
    log.debug2("dirOfUrl: {} -> {}", url, res);
    return res;
  }

  /** Find all the files in the directory of the supplied URL, recording
   * the package name to make it easier to generate the necessary relative
   * paths for the jar */
  List<PkgUrl> listFilesInDirOf(PkgUrl pu) {
    try {
      URL dirURL = dirOfUrl(pu.getUrl());
      switch (dirURL.getProtocol()) {
      case "file":
	return enumerateDir(pu, dirURL);
      case "jar":
	return enumerateJarDir(pu, dirURL);
      default:
	throw new UnsupportedOperationException("Cannot list files for URL "+
						pu.getUrl());
      }
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  List<PkgUrl> enumerateDir(PkgUrl pu, URL dirURL)
      throws URISyntaxException, IOException {
    String[] files = new File(dirURL.toURI()).list();
    List<PkgUrl> res = new ArrayList<>();
    for (String f : files) {
      res.add(new PkgUrl(pu.getPkg(), new URL(dirURL, f)));
    }
    return res;
  }

  List<PkgUrl> enumerateJarDir(PkgUrl pu, URL dirURL) throws IOException {
    if (true) throw new UnsupportedOperationException("nyi");

    JarURLConnection jarConnection = (JarURLConnection)dirURL.openConnection();
    JarFile jf = jarConnection.getJarFile();
    List<PkgUrl> res = new ArrayList<>();

    Enumeration<JarEntry> entries = jf.entries(); //gives ALL entries in jar
    while(entries.hasMoreElements()) {
//       String name = entries.nextElement().getName();
// 	if (name.startsWith(path)) { //filter according to the path
// 	  String entry = name.substring(path.length());
// 	  int checkSubdir = entry.indexOf("/");
// 	  if (checkSubdir >= 0) {
// 	    // if it is a subdirectory, we just return the directory name
// 	    entry = entry.substring(0, checkSubdir);
// 	  }
// 	  result.add(entry);
// 	}
    }
    return res;

  }

  /** Return the package part of the fq name */
  static String packageOf(String name) {
    return StringUtil.upToFinal(name, ".");
  }

  /** Convert a package name to a file path */
  static String pathOfPkg(String pkg) {
    return pkg.replace('.', '/');
  }

  static String toString(Attributes attr) {
    List l = new ArrayList();
    for (Map.Entry ent : attr.entrySet()) {
      l.add(ent.getKey() + "=" + ent.getValue());
    }
    return StringUtil.separatedString(l, "[", ", ", "]");
  }

  /** Walk directory looking for plugins (*.xml), building a PlugSpec for
   * each */
  List<PlugSpec> findSpecsInDir(File indir, File outdir) {
    Path dirPath = indir.toPath();

    List<PlugSpec> res = new ArrayList<>();
    FileVisitor visitor =
      new FileVisitor(res, dirPath, outdir.toPath())
      .setExclusions(argExcludePats);
    try {
      log.debug("starting tree walk ...");
      Files.walkFileTree(dirPath, EnumSet.of(FOLLOW_LINKS), 100, visitor);
      excluded = visitor.getExcluded();
    } catch (IOException e) {
      throw new RuntimeException("unable to walk source tree.", e);
    }
    return res;
  }



  // Command line support

  static String USAGE =
    "Usage:\n" +
    "PluginPackager [common-args] -p <plugin-id> ... -o <output-jar> ...\n" +
    "  or\n" +
    "PluginPackager [common-args] -pd <plugins-class-dir> -od <output-dir>\n" +
    "\n" +
    "     -p <plugin-id>    Fully-qualified plugin id.\n" +
    "     -o <output-jar>   Output jar path/name.\n" +
    "     -pd <plugins-class-dir>  Root of compiled plugins tree.\n" +
    "     -od <output-dir>  Dir to which to write plugin jars.\n" +
    "     -x <exclude-pat>  Used with -pd.  Plugins whose id matches this\n" +
    "                       regexp will be excluded.  May be repeated.\n" +
    " Common args:\n" +
    "     -f                Force rebuild even if jar appears to be up-to-date.\n" +
    "     -nofail           Exit with 0 status even if some plugins can't be built.\n" +
    "     -cp <classpath>   Load plugins from specified colon-separated classpath.\n" +
    "     -keystore <file>  Signing keystore.\n" +
    "     -alias <alias>    Key alias (required if -keystore is used).\n" +
    "     -storepass <pass> Keystore password (def \"password\").\n" +
    "     -keypass <pass>   Key password (def \"password\").\n" +
    "\n" +
    "Builds and optionally signs LOCKSS loadable plugin jars.  Each jar contains\n" +
    "one or more plugins and their dependent files, including parent plugins.\n" +
    "(Currently this is all the files in the dirs of the plugin and its parents.)\n" +
    "\n" +
    "The first form allows this to be specified explicity: each -o <output-jar>\n" +
    "will contain the plugins listed in the -p args immediately preceding it.\n" +
    "More than one -p -o sequence may be used:\n" +
    "\n" +
    "  PluginPackager -p org.lockss.plugin.pub1.Pub1Plugin\n" +
    "                 -o /tmp/Pub1Plugin.jar \n" +
    "                 -p org.lockss.plugin.pub2.Pub2PluginA\n" +
    "                 -p org.lockss.plugin.pub2.Pub2PluginB\n" +
    "                 -o /tmp/Pub2Plugins.jar\n" +
    "\n" +
    "will build two jars, the first containing Pub1Plugin, the second containing\n" +
    "Pub2PluginA and Pub2PluginB.\n" +
    "\n" +
    "The second form traverses the directory tree below <plugins-class-dir>\n" +
    "(which should be a compiled classes hierarchy, e.g., target/classes),\n" +
    "packaging each plugin into a jar named <output-dir>/<plugin-id>.jar.\n" +
    "\n" +
    "  PluginPackager -pd target/classes -od target/pluginjars\n" +
    "\n" +
    "If a keystore and alias are provided, the jar will be signed.\n" +
    "\n" +
    "Plugin jars are not rebuilt or re-signed if they are at least as recent as\n" +
    "all the files and dirs that would be written into them.  This can be\n" +
    "suppressed with -f.\n";

  static void usage(String msg) {
    if (!StringUtil.isNullString(msg)) {
      System.err.println("Error: " + msg);
    }
    usage();
  }

  static void usage() {
    System.err.println(USAGE);
    System.exit(1);
  }

  public static void main(String[] argv) {

    PlugSpec curSpec = new PlugSpec();
    PluginPackager pkgr = new PluginPackager();

    if (argv.length == 0) {
      usage();
    }
    try {
      for (int ix = 0; ix < argv.length; ix++) {
	String arg = argv[ix];
	if        (arg.equals("-cp")) {
	  pkgr.setClassPath(argv[++ix]);
	} else if (arg.equals("-f")) {
	  pkgr.setForceRebuild(true);
	} else if (arg.equals("-nofail")) {
	  pkgr.setNoFail(true);
	} else if (arg.equals("-keystore")) {
	  pkgr.setKeystore(argv[++ix]);
	} else if (arg.equals("-alias")) {
	  pkgr.setAlias(argv[++ix]);
	} else if (arg.equals("-keypass")) {
	  pkgr.setKeyPass(argv[++ix]);
	} else if (arg.equals("-storepass")) {
	  pkgr.setStorePass(argv[++ix]);
	} else if (arg.equals("-p")) {
	  curSpec.addPlug(argv[++ix]);
	} else if (arg.equals("-o")) {
	  curSpec.setJar(argv[++ix]);
	  pkgr.addSpec(curSpec);
	  curSpec = new PlugSpec();
	} else if (arg.equals("-pd")) {
	  pkgr.setPluginDir(new File(argv[++ix]));
	} else if (arg.equals("-od")) {
	  pkgr.setOutputDir(new File(argv[++ix]));
	} else if (arg.equals("-x")) {
	  pkgr.addExclusion(argv[++ix]);
	} else {
	  usage();
	}
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      usage();
    }
    if (!curSpec.getPluginIds().isEmpty()) {
      usage("-p arg(s) without following -o");
    }
    try {
      pkgr.build();
      System.exit(reportResults(pkgr));
    } catch (IllegalArgumentException e) {
      usage(e.getMessage());
    } catch (Exception e) {
      log.error("init() failed", e);
      System.exit(2);
    }

  }

  static int reportResults(PluginPackager pkgr) {
    List<Result> reslst = pkgr.getResults();
    List<Result> failures = new ArrayList<>();
    int success = 0;
    int fail = 0;
    int notModified = 0;
    for (Result res : reslst) {
      PlugSpec spec = res.getPlugSpec();
      if (res.isError()) {
	fail++;;
	failures.add(res);
      } else if (res.isNotModified()) {
	notModified++;
      } else {
	success++;
      }
    }
    String msg;
    int excl = pkgr.getExcluded().size();
    int tot = reslst.size() + excl;
    File pdir = pkgr.getPlugDir();
    if (pdir != null) {
      msg = "Found " + StringUtil.numberOfUnits(tot, "plugin") +
	", (re)built " + StringUtil.numberOfUnits(success, "jar") + ", " +
	notModified + " not modified, " +
	excl + " excluded. " +
	fail + " failed.";
    } else {
      msg = StringUtil.numberOfUnits(success, "jar") + " built, " +
	notModified + " not modified, " +
	excl + " excluded. " +
	fail + " failed.";
    }
    if (failures.isEmpty()) {
      log.info(msg);
    } else {
      log.error(msg);
      for (Result res : failures) {
	PlugSpec spec = res.getPlugSpec();
	log.error(res.getException());
      }
      return pkgr.isNoFail() ? 0 : 1;
    }
    return 0;
  }
}
