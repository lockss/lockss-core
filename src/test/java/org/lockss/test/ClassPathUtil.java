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

package org.lockss.test;
import java.io.*;
import java.nio.file.*;
import java.net.*;
import java.util.*;
import java.util.jar.*;
import java.util.stream.*;
import java.util.zip.*;
import org.apache.commons.collections4.*;
import org.apache.commons.collections4.multimap.*;
import org.lockss.util.*;

/**
 * Utilities that operate on classpath: display, search for qualified or
 * unqualified resource/class, find duplicates. etc.
 *
 * Can be invoked via command line (see usage()) or a java api.  Subject
 * classpath can be:<ul>
 * <li>Explicitly supplied</li>
 * <li>Current thread ContextClassLoader</li>
 * <li>This class's ClassLoader</li>
 * <li>Supplied ClassLoader (api only)</li>
 * </ul>
 */
public class ClassPathUtil {
  private static Logger log = Logger.getLogger();

  /** Determines which classpath is used */
  public enum Which {
    /** Use the classpath of ClassPathUtil.class.getClassLoader() */
    Class,
    /** Use the classpath of Thread.currentThread().getContextClassLoader() */
    Thread,
    /** Use the classpath supplied on the command line or to setClasspath() */
    Arg,
    /** Use the java.class.path System property */
    System,
    /** Use the classpath of the ClassLoader passed to setClassLoader() */
    StoredCL }

  private List m_classpath = new ArrayList();
  private Which whichPath = Which.Arg;
  private ClassLoader cl;
  private StringBuilder out = new StringBuilder();

  private void println(String s) {
    out.append(s);
    out.append("\n");
  }

  private void endPrint() {
    log.info(out.toString());
    out = new StringBuilder();
  }

  /** Find a resource with the given name */
  public ClassPathUtil whichResource(String resourceName, String msg) {
    URL resUrl = findResource(resourceName);

    if (resUrl == null) {
      println(msg + " not found.");
    } else {
      println(msg + " found in \n" + resUrl);
    }
    endPrint();
    return this;
  }

  /** Find a resource with the given name */
  public ClassPathUtil whichResource(String resourceName) {
    resourceName = fixResourceName(resourceName);
    whichResource(resourceName, "Resource " + resourceName);
    return this;
  }

  /** Find a class with the given name */
  public ClassPathUtil whichClass(String className) {
    String resourceName = asResourceName(className);
    whichResource(resourceName, "Class " + className);
    return this;
  }

  private URL findResource(String resourceName) {
    return getClassLoader().getResource(resourceName);
  }

  // ensure no leading /
  private String fixResourceName(String resourceName) {
    if (resourceName.startsWith("/")) {
      return resourceName.substring(1);
    }
    return resourceName;
  }

  // convert class name to resource name
  private String asResourceName(String className) {
    String resource = className;
    resource = resource.replace('.', '/');
    resource = resource + ".class";
    return fixResourceName(resource);
  }

  // build map of resource name -> list of jars that contain it
  private static MultiValuedMap<String,File> m_map =
    new ArrayListValuedHashMap<>();

  private void buildMap() {
    if (m_map.isEmpty()) {
      for (String element : getClasspath()) {
	File file = new File(element);
	if (file.isDirectory()) {
	  processDir(file);
	} else {
	  processJar(file);
	}
      }
    }
  }

  // add all non-directories in dir tree
  private void processDir(File dir) {
    Path root = dir.toPath();
    try {
      Files.walk(root, FileVisitOption.FOLLOW_LINKS)
	.filter(p -> !p.toFile().isDirectory())
	.forEach(p -> m_map.put(root.relativize(p).toString(), dir));
    } catch (IOException e) {
      log.error("processDir(" + dir + ")", e);
    }
  }


  // add the entries for one jar
  private void processJar(File file) {
    try {
      JarFile jf = new JarFile(file);
      for (Enumeration en = jf.entries(); en.hasMoreElements(); ) {
	ZipEntry ent = (ZipEntry)en.nextElement();
	if (ent.isDirectory()) {
	  continue;
	}
	m_map.put(ent.getName(), file);
      }
    } catch (IOException e) {
      log.error("reading jar " + file, e);
    }
  }

  private Collection<String> sortedKeys(MultiValuedMap<String,?> map) {
    return new TreeSet(map.keySet());
  }

  /** Display all the resources that are found in more than one place on
   * the classpath. */
  public ClassPathUtil showConflicts() {
    buildMap();
    List<String> res = new ArrayList<>();
    for (String key : sortedKeys(m_map)) {
      Collection jars = m_map.get(key);
      if (jars.size() > 1) {
	res.add(key + ": " + jars);
      }
    }
    println(StringUtil.numberOfUnits(res.size(), "repeated object"));
    for (String s : res) {
      println(s);
    }
    endPrint();
    return this;
  }

  /** Search for all occurrences of a class given name in any
   * package. */
  public ClassPathUtil searchClass(String className) {
    String resource = className + ".class";
    searchResource(resource);
    return this;
  }

  /** Search for all occurrences of a resource with the given name in any
   * package. */
  public ClassPathUtil searchResource(String resourceName) {
    buildMap();
    List<String> res = new ArrayList<>();
    for (String key : sortedKeys(m_map)) {
      File resFile = new File(key);
      String name = resFile.getName();
      if (resourceName.equals(name)) {
	res.add(key + ": " + m_map.get(key));
      }
    }
    println(StringUtil.numberOfUnits(res.size(), "occurrence")
	    + " of " + resourceName);
    for (String s : res) {
      println(s);
    }
    endPrint();
    return this;
  }

  /**
   * Validate the class path and report any non-existent
   * or invalid class path entries.
   * <p>
   * Valid class path entries include directories, <code>.zip</code>
   * files, and <code>.jar</code> files.
   */
  public ClassPathUtil validate() {
    for (Iterator iter = getClasspath().iterator(); iter.hasNext(); ) {
      String element = (String)iter.next();
      File f = new File(element);

      if (!f.exists()) {
	println("Classpath element " + element +
			   " does not exist.");
	endPrint();
      } else if ( (!f.isDirectory()) &&
		  (!StringUtil.endsWithIgnoreCase(element, ".jar")) &&
		  (!StringUtil.endsWithIgnoreCase(element, ".zip")) ) {
	println("Classpath element " + element +
			   "is not a directory, .jar file, or .zip file.");
	endPrint();
      }
    }
    return this;
  }

  /** Print the classpath */
  public ClassPathUtil printClasspath() {
    println("Classpath:");
    for (Iterator iter = getClasspath().iterator(); iter.hasNext(); ) {
      println((String)iter.next());
    }
    endPrint();
    return this;
  }

  /** Determine which classpath will be used. */
  public ClassPathUtil setWhichPath(Which wh) {
    switch (wh) {
    case System:
      m_classpath =
	StringUtil.breakAt(System.getProperty("java.class.path"),
			   File.pathSeparator);
      whichPath = Which.Arg;
      break;
    default:
      whichPath = wh;
    }
    return this;
  }

  public Which getWhichPath() {
    return whichPath;
  }

  /** Set the classpath from a string */
  public ClassPathUtil setClasspath(String classpath) {
    m_classpath = StringUtil.breakAt(classpath, File.pathSeparator);
    m_map.clear();
    return this;
  }

  /** Set the classpath */
  public ClassPathUtil setClasspath(List<String> classpath) {
    m_classpath = classpath;
    m_map.clear();
    return this;
  }

  /** Add a jar/dir to the classpath */
  public ClassPathUtil addClasspath(String path) {
    m_classpath.addAll(StringUtil.breakAt(path, File.pathSeparator));
    m_map.clear();
    return this;
  }

  /** Set the classpath to be that of the supplied ClassLoader */
  public ClassPathUtil setClassLoader(ClassLoader cl) {
    this.cl = cl;
    setWhichPath(Which.StoredCL);
    return this;
  }

  private List<String> getClasspath() {
    switch (whichPath) {
    case Arg:
      return m_classpath;
    case Class:
    case Thread:
    case StoredCL:
      return classPathOf(getClassLoader());
    }
    throw new IllegalArgumentException();
  }

  private ClassLoader getClassLoader() {
    switch (whichPath) {
    case Arg:
      return new URLClassLoader(toUrlArray(m_classpath));
    case Class:
      return ClassPathUtil.class.getClassLoader();
    case Thread:
      return Thread.currentThread().getContextClassLoader();
    case StoredCL:
      return cl;
    }
    throw new IllegalArgumentException();
  }

  private static URL newURL(String s) {
    try {
      return new URL(s);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e.toString());
    }
  }

  public static URL[] toUrlArray(List<String> strs) {
    return strs.stream()
      .map(u -> UrlUtil.isUrl(u) ? u : "file:" + u)
      .map(u -> newURL(u))
      .toArray(URL[]::new);
  }

  private static List<String> classPathOf(ClassLoader cl) {
    return Arrays.stream(((URLClassLoader)cl).getURLs())
      .map(URL::getFile)
      .collect(Collectors.toList());
  }

  private static void error(String msg) {
    System.err.println(msg);
    usage();
  }

  private static void usage() {
    String progName = ClassPathUtil.class.getName();
    System.out.println("java " + progName + " [ actions ... ]");
    System.out.println("  Actions are executed sequentially:");
    System.out.println("   -cpc                 Use class.getClassLoader() classpath (default)");
    System.out.println("   -cpt                 Use thread.getContextClassLoader() classpath");
    System.out.println("   -cps                 Use java.class.path System property");
    System.out.println("   -cp <classpath>      Set classpath");
    System.out.println("   -ap <classpath       Append to classpath");
    System.out.println("");
    System.out.println("   -c  <className>      Locate fq class on classpath");
    System.out.println("   -r  <resourceName>   Locate fq resource on classpath");
    System.out.println("   -sc <unqualifiedClassName> Search for all occurrences of class on classpath");
    System.out.println("   -sr <unqualifiedResourceName> Ditto for resource");
    System.out.println("   -p                   Print classpath");
    System.out.println("   -x                   Show duplicate resources on classpath");
    System.out.println("   -v                   Validate classpath (check that all jars exist)");
    System.exit(0);
  }

  public static void main(String args[]) {
    if (args.length == 0) {
      usage();
    }
    ClassPathUtil cpu = new ClassPathUtil();

    try {
      for (int ix = 0; ix < args.length; ix++) {
	String a = args[ix];

	if ("-cpc".equals(a)) {
	  cpu.setWhichPath(Which.Class);
	  continue;
	}
	if ("-cpt".equals(a)) {
	  cpu.setWhichPath(Which.Thread);
	  continue;
	}
	if ("-cps".equals(a)) {
	  cpu.setWhichPath(Which.System);
	  continue;
	}
	if ("-cp".equals(a) || "-classpath".equals(a)) {
	  cpu.setClasspath(args[++ix]);
	  cpu.setWhichPath(Which.Arg);
	  continue;
	}
	if ("-ap".equals(a) || "-addpath".equals(a)) {
	  switch (cpu.getWhichPath()) {
	  case Arg:
	    cpu.addClasspath(args[++ix]);
	    break;
	  default:
	    error("-ap legal only after -cp");
	  }
	  continue;
	}
	if ("-r".equals(a)) {
	  cpu.whichResource(args[++ix]);
	  continue;
	}
	if ("-c".equals(a)) {
	  cpu.whichClass(args[++ix]);
	  continue;
	}
	if ("-sc".equals(a)) {
	  cpu.searchClass(args[++ix]);
	  continue;
	}
	if ("-sr".equals(a)) {
	  cpu.searchResource(args[++ix]);
	  continue;
	}
	if ("-p".equals(a)) {
	  cpu.printClasspath();
	  continue;
	}
	if ("-x".equals(a)) {
	  cpu.showConflicts();
	  continue;
	}
	if ("-v".equals(a)) {
	  cpu.validate();
	  continue;
	}
	usage();
      }
    } catch (Exception e) {
      log.error("", e);
      usage();
    }
  }
}
