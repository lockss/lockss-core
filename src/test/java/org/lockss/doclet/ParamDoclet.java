/*

Copyright (c) 2000-2017 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.doclet;

import java.util.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.stream.Collectors;

import com.sun.source.doctree.*;
import com.sun.source.util.DocTrees;
import com.sun.source.util.SimpleDocTreeVisitor;
import com.sun.source.util.TreePath;
import org.apache.commons.collections4.SetValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;

// https://docs.oracle.com/en/java/javase/11/docs/api/jdk.javadoc/jdk/javadoc/doclet/package-summary.html#migration
import javax.lang.model.element.*;

import jdk.javadoc.doclet.DocletEnvironment; // RootDoc
import jdk.javadoc.doclet.Reporter; // DocErrorReporter

import javax.lang.model.util.ElementScanner9;
import javax.tools.Diagnostic;
import javax.lang.model.SourceVersion;

import jdk.javadoc.doclet.Doclet;

import org.lockss.util.*;
import org.lockss.util.time.TimeUtil;

/**
 * A JavaDoc doclet that prints configuration parameter information.
 * <p>
 * static String variables with the prefix <bold><tt>PARAM_</tt></bold>
 * designate configuration parameters.  The value of the variable is the
 * name of the parameter.  The default value of the parameter is the value
 * of the similarly named variable with the prefix
 * <bold><tt>DEFAULT_</tt></bold>, if any.  The variable's javadoc is the
 * parameter's documentation.  That javadoc may contain one or more
 * &#064;ParamCategory tags, whose value is a comma-separated list of
 * category names.  &#064;ParamCategory may also appear in the javadoc for
 * the class, in which case it establishes the default category for all
 * param definitions in the class.  &#064;ParamCategoryDoc <i>name</i>
 * <i>string</i> provides an explanation of the category.
 * <p>
 * The &#064;ParamRelevance tag specifies the relevance/importance of the
 * parameter.  It should be one of:<ul>
 * <li>Required - Must be set in order for daemon to run; there is no
 * sensible default value.</li>
 * <li>Common - Commonly used.</li>
 * <li>LessCommon - Less commonly used.</li>
 * <li>Unknown - Relevance of parameter not specified.  This is the default
 * if no relevance is specified.</li>
 * <li>Testing - Primarily used by unit tests to disable some default
 * behavior that's undesirable when runnning tests, such as connecting to
 * a JMS broker.</li>
 * <li>Rare - Rarely used.</li>
 * <li>BackwardCompatibility - Used to restore some aspect of the daemon's
 * behavior to what is was before some change or new feature was
 * implemented.  Included primarily as a failsafe in case of unforseen
 * consequences of the change.</li>
 * <li>Obsolescent - Enables or controls a feature that is no longer used.</li>
 * <li>Never - Included for some internal purpose, should never be set by
 * users.</li>
 * </ul>
 * <p>
 * Invoke the doclet with <tt>-f Category</tt> to have the parameters
 * grouped by Category, with <tt>-f Relevance</tt> to have the parameters
 * grouped by Relevance.
 */
public class ParamDoclet implements Doclet {

  private Reporter reporter;
  private static File outDir = null;
  private static PrintStream out = null;
  private static boolean closeOut = false;
  private String releaseHeader = null;

  // Maps category name to set of (ParamInfo of) params belonging to it
  SetValuedMap<String, ParamInfo> categoryMap =
      new HashSetValuedHashMap<String, ParamInfo>();

  // Maps category name to its doc string
  Map<String, String> catDocMap = new HashMap<String, String>();

  // One of alpha, category, relevance
  String fmt = "alpha";

  // Maps (key derived from) symbol name (PARAM_XXX) to ParamInfo
  Map<String, ParamInfo> params = new HashMap<String, ParamInfo>();

  // Maps param name to ParamInfo
  TreeMap<String, ParamInfo> sortedParams = new TreeMap<String, ParamInfo>();

  private static final boolean OK = true;
  private static final boolean FAILURE = false;

  private final Set<Option> options = Set.of(
      new Option("-o", true, "Output file", "<string>") {
        @Override
        public boolean process(String opt, List<String> args) {
          String outFile = args.get(0);
          try {
            File f = null;
            if (outDir != null) {
              f = new File(outDir, outFile);
            } else {
              f = new File(outFile);
            }
            if (out == null) {
              out = new PrintStream(new BufferedOutputStream(new FileOutputStream(f)));
              closeOut = true;
            } else {
              reporter.print(Diagnostic.Kind.ERROR, "Output stream is already open");
              return FAILURE;
            }
          } catch (IOException ex) {
            reporter.print(Diagnostic.Kind.ERROR, "Unable to open output file: " + outFile);
            return FAILURE;
          }

          return OK;
        }
      },
      new Option("-d", true, "Output directory", "<string>") {
        @Override
        public boolean process(String opt, List<String> args) {
          outDir = new File(args.get(0));
          return OK;
        }
      },
      new Option("-h", true, "Release header", "<string>") {
        @Override
        public boolean process(String opt, List<String> args) {
          releaseHeader = args.get(0);
          return OK;
        }
      },
      new Option("-f", true, "Group parameters by", "<string>") {
        @Override
        public boolean process(String opt, List<String> args) {
          fmt = args.get(0);
          if (StringUtil.isNullString(fmt)) {
            fmt = "Alpha";
          }
          fmt = fmt.toLowerCase().trim();
          return OK;
        }
      }
  );

  @Override
  public void init(Locale locale, Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public Set<? extends Option> getSupportedOptions() {
    return options;
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.latest();
  }

  @Override
  public boolean run(DocletEnvironment docletEnv) {
    if (out == null) {
      out = System.out;
    }

    treeUtils = docletEnv.getDocTrees();
    ProcessClassDocs pt = new ProcessClassDocs(out);
    pt.show(docletEnv.getSpecifiedElements());

//    processTypeElements();
    printDoc();

    if (closeOut) {
      out.close();
    }

    return OK;
  }

  private DocTrees treeUtils;

  class ProcessClassDocs extends ElementScanner9<Void, String> {
    final PrintStream out;

    public ProcessClassDocs(PrintStream out) {
      this.out = out;
    }

    void show(Set<? extends Element> elements) {
      scan(elements, null);
    }

    @Override
    public Void visitVariable(VariableElement e, String defaultCategory) {
      Name varName = e.getSimpleName();
      if (e.getKind() == ElementKind.FIELD && isParam(varName.toString())) {
        addParam(e, defaultCategory);
      }

      return super.visitVariable(e, null);
    }

    @Override
    public Void visitType(TypeElement e, String defaultCategory) {
      if (e.getKind() == ElementKind.CLASS) {
        TagScanner scanner = new TagScanner(e);
        scanner.process();
        Set<String> cats = scanner.getCategories();
        if (!cats.isEmpty()) {
          defaultCategory = cats.iterator().next();
          out.println("default:" + defaultCategory);
        }
      }

      // Continue scanning enclosed elements
      return super.visitType(e, defaultCategory);
    }

    @Override
    public Void scan(Element e, String s) {
      return super.scan(e, s);
    }
  }

  class TagScanner extends SimpleDocTreeVisitor<Void, Void> {
    private final Element e;
    public Set<String> categories = new HashSet<String>(2);
    public Relevance rel = Relevance.Unknown;

    TagScanner(Element e) {
      this.e = e;
    }

    public void process() {
      DocCommentTree dct = treeUtils.getDocCommentTree(e);

      if (dct != null) {
        visit(dct, null);
      }
    }

    @Override
    public Void visitDocComment(DocCommentTree node, Void unused) {
      return visit(node.getBlockTags(), null);
    }

    @Override
    public Void visitUnknownBlockTag(UnknownBlockTagTree node, Void unused) {
      String name = node.getTagName();
      String text = node.getContent()
          .stream()
          .map(Object::toString)
          .collect(Collectors.joining());

      switch (name) {
        case "ParamCategoryDoc":
          processCategoryDoc(text);
          break;
        case "ParamCategory":
          categories.addAll(StringUtil.breakAt(text, ",", -1, true, true));
          break;
        case "ParamRelevance":
          setRelevance(text);
          break;
        default:
          reporter.print(Diagnostic.Kind.WARNING, "Unknown tag: " + name);
      }

      return null;
    }

    public Set<String> getCategories() {
      return categories;
    }

    public Relevance getRelevance() {
      return rel;
    }

    public void setRelevance(String relevance) {
      try {
        rel = Relevance.valueOf(relevance);
      } catch (IllegalArgumentException ex) {
        reporter.print(Diagnostic.Kind.ERROR, e,
            "@ParamRelevance value must be a Relevance");
      }
    }

    public List<? extends DocTree> getKnownTags() {
      DocCommentTree dct = treeUtils.getDocCommentTree(e);
      if (dct != null) {
        return dct.getBlockTags()
            .stream()
            .filter(tag -> tag.getKind() != DocTree.Kind.UNKNOWN_BLOCK_TAG)
            .collect(Collectors.toList());
      }

      return Collections.emptyList();
    }
  }

  void addParam(VariableElement field, String classParamCategory) {
    Element parent = field.getEnclosingElement();
    String className = parent.getSimpleName().toString();

    String fieldName = field.getSimpleName().toString();
    String key = getParamKey(parent, fieldName);

    ParamInfo info = params.get(key);
    if (info == null) {
      info = new ParamInfo();
      params.put(key, info);
    }

    if (fieldName.startsWith("PARAM_")) {
      Object value = field.getConstantValue();

      // Fields beginning with PARAM_ define LOCKSS configuration parameter names
      // as String constants (e.g., org.lockss.config.XXX). We enforce that here:
      if (!(value instanceof String)) {
        reporter.print(Diagnostic.Kind.WARNING, field, "Non-string value for " + fieldName);
        reporter.print(Diagnostic.Kind.WARNING, field, "value: " + value);
        if (value != null) {
          reporter.print(Diagnostic.Kind.WARNING, field, "value class: " + value.getClass());
        }
        return;
      }

      if (info.definedIn.isEmpty()) {
        // This is the first occurrence we've encountered.
        String paramName = (String) value;
        info.paramName = paramName;

        DocCommentTree dct = treeUtils.getDocCommentTree(field);
        if (dct != null) {
          String comment = dct.getFullBody()
              .stream()
              .map(Object::toString)
              .collect(Collectors.joining());
          info.setComment(comment);
        }

        info.addDefinedIn(className);
        // Add to the sorted map we'll use for printing.
        sortedParams.put(paramName, info);

        // Get this fields tags using the TagScanner
        TagScanner scanner = new TagScanner(field);
        scanner.process();

        // Set categories
        Set<String> categories = scanner.getCategories();
        boolean hasCat = !categories.isEmpty();
        for (String cat : categories) {
          info.addCategory(cat);
        }

        if (!hasCat && classParamCategory != null) {
          info.addCategory(classParamCategory);
        }

        // Set relevance
        info.setRelevance(field, scanner.getRelevance());

        // Set known tags
        for (DocTree dt : scanner.getKnownTags()) {
          info.addTag(dt);
        }

      } else {
        // We've already visited this parameter before, this is
        // just another definer.
        info.addDefinedIn(className);
      }
    } else if (fieldName.startsWith("DEFAULT_")) {
      info.defaultValue = getDefaultValue(field);
    }
  }

  void processCategoryDoc(String catDoc) {
    String[] sa = divideAtWhite(catDoc);
    String cat = sa[0];
    if (catDocMap.containsKey(cat)) {
      reporter.print(Diagnostic.Kind.WARNING, "Duplicate Category doc for " + cat);
    } else {
      catDocMap.put(cat, sa[1]);
    }
  }

  void printDoc() {
    printDocHeader();

    out.println("<h3>" + sortedParams.size() + " total parameters</h3>");

    switch (fmt) {
      case "alpha":
        printDocV1();
        break;
      case "category":
        printDocByCat();
        break;
      case "relevance":
        printDocByRel();
        break;
    }
    printDocFooter();
  }

  private void printDocHeader() {
    out.println("<html>");
    out.println("<head>");
    out.println(" <title>Parameters</title>");
    out.println(" <style type=\"text/css\">");
    out.println("  .sectionName { font-weight: bold; font-family: sans-serif;");
    out.println("      font-size: 16pt; }");
    out.println("  .paramName { font-weight: bold; font-family: sans-serif;");
    out.println("      font-size: 14pt; }");

    out.println("  .defaultValue { font-family: monospace; font-size: 14pt; }");
    out.println("  table { border-collapse: collapse; margin-left: 20px;");
    out.println("      margin-right: 20px; padding: 0px; width: auto; }");
    out.println("  tr { margin: 0px; padding: 0px; border: 0px; }");
    out.println("  td { margin: 0px; padding-left: 6px; padding-right: 6px;");
    out.println("      border: 0px; padding-left: 0px; padding-top: 0px; padding-right: 0px;}");
    out.println("  td.paramHeader { padding-top: 5px; }");
    out.println("  td.sectionHeader { padding-top: 5px; font-size: 16pt; }");
    out.println("  td.comment { }");
    out.println("  td.categoryComment { padding-left: 20px; }");
    out.println("  td.definedIn { font-family: monospace; }");
    out.println("  td.header { padding-left: 30px; padding-right: 10px; font-style: italic; text-align: right; }");

    out.println(" </style>");
    out.println("</head>");
    out.println("<body>");
    out.println("<div align=\"center\">");
    out.println("<h1>LOCKSS Configuration Parameters</h1>");
    switch (fmt) {
      case "category":
        out.println("<h2>by Category</h2>");
        break;
      case "relevance":
        out.println("<h2>by Relevance</h2>");
        break;
      default:
    }
    if (!StringUtil.isNullString(releaseHeader)) {
      out.println("<h2>");
      out.println(releaseHeader);
      out.println("</h2>");
    }
    out.println("<table>");
    out.flush();
  }

  private static void printDocFooter() {
    out.println("</table>");
    out.println("</div>");
    out.println("</body>");
    out.println("</html>");
    out.flush();
  }

  private void printDocV1() {
    for (ParamInfo info : sortedParams.values()) {
      printParamInfo(info);
    }
  }

  private void printDocByCat() {
    Set<ParamInfo> done = new HashSet<ParamInfo>();
//     System.err.println("all cat: " +
// 		       CollectionUtil.asSortedList(categoryMap.keySet()));

    for (String cat : CollectionUtil.asSortedList(categoryMap.keySet())) {
      printCategoryHeader(cat);
      for (Relevance rel : Relevance.values()) {
        Set<ParamInfo> catSet = categoryMap.get(cat);
        for (ParamInfo info :
            CollectionUtil.asSortedList(catSet)) {
          if (info.getRelevance() == rel) {
            printParamInfo(info);
            done.add(info);
          }
        }
      }
    }

    printCategoryHeader("None");
    for (Relevance rel : Relevance.values()) {
      for (ParamInfo info : sortedParams.values()) {
        if (info.getRelevance() == rel && !done.contains(info)) {
          printParamInfo(info);
          done.add(info);
        }
      }
    }
  }

  private void printDocByRel() {
//     System.err.println("all cat: " +
// 		       CollectionUtil.asSortedList(categoryMap.keySet()));

    for (Relevance rel : Relevance.values()) {
      printRelevanceHeader(rel);
      for (ParamInfo info : sortedParams.values()) {
        if (info.getRelevance() == rel) {
          printParamInfo(info);
        }
      }
    }
  }

  private void printCategoryHeader(String cat) {
    out.println("<tr>\n  <td colspan=\"2\" class=\"sectionHeader\">");
    out.print("    <span class=\"sectionName\" id=\"" + cat + "\">" +
        "Category: " + cat + "</span></td>");
    out.println("</tr>");
    if (catDocMap.containsKey(cat)) {
      out.println("<tr>\n  <td colspan=\"2\" class=\"categoryComment\">");
      out.print(catDocMap.get(cat));
      out.print("</td>");
      out.println("</tr>");
    }
  }

  private void printRelevanceHeader(Relevance rel) {
    out.println("<tr>\n  <td colspan=\"2\" class=\"sectionHeader\">");
    out.print("    <span class=\"sectionName\" id=\"" + rel + "\">" +
        "Relevance: " + rel + "</span></td>");
    out.println("</tr>");
    if (true) {
      out.println("<tr>\n  <td colspan=\"2\" class=\"categoryComment\">");
      out.print(rel.getExplanation());
      out.print("</td>");
      out.println("</tr>");
    }
  }

  String htmlParamName(ParamInfo info) {
    // Angle brackets in param names are used for meta-symbols.
    // Turn <foo> into <i>foo</i>
    String res = info.getParamName().replaceAll("<([a-zA-Z_-]+)>", "<i>$1</i>");
    return res;
//     String res = HtmlUtil.htmlEncode(info.getParamName().trim());
  }

  private void printParamInfo(ParamInfo info) {
    String pnameId = HtmlUtil.htmlEncode(info.getParamName().trim());
    String pname = htmlParamName(info);

    out.println("<tr>\n  <td colspan=\"2\" class=\"paramHeader\">");
    out.print("    <span class=\"paramName\" id=\"" + pnameId + "\">" +
        pname + "</span> &nbsp; ");
    out.print("<span class=\"defaultValue\">[");
    out.print(info.defaultValue == null ?
        "" : HtmlUtil.htmlEncode(info.defaultValue));
    out.println("]</span>\n  </td>");
    out.println("</tr>");

    out.println("<tr>");
    out.println("  <td class=\"header\" valign=\"top\">Comment:</td>");
    out.print("  <td class=\"comment\">");
    if (info.comment.trim().length() == 0) {
      out.print("");
    } else {
      out.print(info.comment.trim());
    }
    for (BlockTagTree tag : info.tags) {
      out.print("<br>");
      out.print(tag.getTagName());
      out.print(" ");
      out.print(tag);
    }
    out.println("</td>");
    out.println("</tr>");

    if (!info.getCategories().isEmpty()) {
      out.println("<tr>");
      out.println("  <td class=\"header\" valign=\"top\">Categories:</td>");
      out.print("  <td class=\"categories\">");
      for (String cat : info.getCategories()) {
        out.println(cat + "<br/>");
      }
      out.println("</td>");
      out.println("</tr>");
    }

    if (info.getRelevance() != Relevance.Unknown && !fmt.equals("relevance")) {
      out.println("<tr>");
      out.println("  <td class=\"header\" valign=\"top\">Relevance:</td>");
      out.print("  <td class=\"relevance\">");
      out.println(info.getRelevance());
      out.println("<br/>");
      out.println("</td>");
      out.println("</tr>");
    }

    out.println("<tr>");
    out.println("  <td class=\"header\" valign=\"top\">Defined in:</td>");
    out.println("  <td class=\"definedIn\">");
    for (String def : info.getDefinedIn()) {
      out.println(def + "<br/>");
    }
    out.println("  </td>");
    out.println("</tr>");
    //    out.println("<tr><td colspan=\"3\">&nbsp;</td></tr>");

    out.flush();
  }

  /**
   * Return true if the specified string is a parameter name.
   */
  private static boolean isParam(String s) {
    return (s.startsWith("PARAM_") || s.startsWith("DEFAULT_"));
  }

  /**
   * Given a parameter or default name, return the key used to look up
   * its info object in the unsorted hashmap.
   */
  private static String getParamKey(Element parent, String fieldName) {
    StringBuffer sb = new StringBuffer(parent.getSimpleName() + ".");
    if (fieldName.startsWith("DEFAULT_")) {
      sb.append(fieldName.replaceFirst("DEFAULT_", ""));
    } else if (fieldName.startsWith("PARAM_")) {
      sb.append(fieldName.replaceFirst("PARAM_", ""));
    } else {
      sb.append(fieldName);
    }
    return sb.toString();
  }

  /**
   * Cheesily use reflection to obtain the default value.
   */
  public String getDefaultValue(VariableElement field) {
    String defaultVal = null;
    try {
      Element classDoc = field.getEnclosingElement();

      Class c = Class.forName(classDoc.asType().toString());
      Field fld = c.getDeclaredField(field.getSimpleName().toString());
      fld.setAccessible(true);
      Class cls = fld.getType();
      if (int.class == cls) {
        defaultVal = (Integer.valueOf(fld.getInt(null))).toString();
      } else if (long.class == cls) {
        long timeVal = fld.getLong(null);
        if (timeVal > 0) {
          defaultVal = timeVal + " (" +
              TimeUtil.timeIntervalToString(timeVal) + ")";
        } else {
          defaultVal = Long.toString(timeVal);
        }
      } else if (boolean.class == cls) {
        defaultVal = (Boolean.valueOf(fld.getBoolean(null))).toString();
      } else {
        try {
          // This will throw NPE if the field isn't static; don't know how
          // to get initial value in that case
          Object dval = fld.get(null);
          defaultVal = (dval != null) ? dval.toString() : "(null)";
        } catch (NullPointerException e) {
          defaultVal = "(unknown: non-static default)";
        }
      }
    } catch (Exception e) {
      reporter.print(Diagnostic.Kind.ERROR, field, field.getSimpleName() + ": " + e);
      reporter.print(Diagnostic.Kind.ERROR, StringUtil.stackTraceString(e));
    }

    return defaultVal;
  }

  abstract class Option implements Doclet.Option {
    private final String name;
    private final boolean hasArg;
    private final String description;
    private final String parameters;

    Option(String name, boolean hasArg,
           String description, String parameters) {
      this.name = name;
      this.hasArg = hasArg;
      this.description = description;
      this.parameters = parameters;
    }

    @Override
    public int getArgumentCount() {
      return hasArg ? 1 : 0;
    }

    @Override
    public String getDescription() {
      return description;
    }

    @Override
    public Kind getKind() {
      return Kind.STANDARD;
    }

    @Override
    public List<String> getNames() {
      return List.of(name);
    }

    @Override
    public String getParameters() {
      return hasArg ? parameters : "";
    }
  }

  /**
   * for use by subclasses which have two part tag text.
   */
  String[] divideAtWhite(String text) {
    String[] sa = new String[2];
    int len = text.length();
    // if no white space found
    sa[0] = text;
    sa[1] = "";
    for (int inx = 0; inx < len; ++inx) {
      char ch = text.charAt(inx);
      if (Character.isWhitespace(ch)) {
        sa[0] = text.substring(0, inx);
        for (; inx < len; ++inx) {
          ch = text.charAt(inx);
          if (!Character.isWhitespace(ch)) {
            sa[1] = text.substring(inx, len);
            break;
          }
        }
        break;
      }
    }
    return sa;
  }

  enum Relevance {
    Required("Must be set in order for daemon to run; there is no sensible default value"),
    Common("Commonly used"),
    LessCommon("Less commonly used"),
    Unknown("Relevance of parameter not specified"),
    Testing("Primarily for use by unit tests"),
    Rare("Rarely used"),
    BackwardCompatibility("Used to restore some aspect of the daemon's behavior to what is was before some change or new feature was implemented.  Included primarily as a failsafe in case of unforseen consequences of the change."),
    Obsolescent("Enables or controls a feature that is no longer used"),
    Never("Included for some internal purpose, should never be set by users."),
    ;

    private String expl;

    Relevance(String exp) {
      this.expl = exp;
    }

    String getExplanation() {
      return expl;
    }
  }

  /**
   * Simple wrapper class to hold information about a parameter.
   */
  private class ParamInfo implements Comparable<ParamInfo> {
    public String paramName = "";
    public String defaultValue = null;
    public boolean isDeprecated;
    public String comment = "";
    public List<BlockTagTree> tags = new ArrayList(5);
    public Set<String> categories = new HashSet<String>(2);
    public Relevance rel = Relevance.Unknown;
    // Sorted list of uses.
    public Set<String> definedIn = new TreeSet();

    @Override
    public int compareTo(ParamInfo other) {
      return paramName.compareTo(other.getParamName());
    }

    String getParamName() {
      return paramName;
    }

    ParamInfo setComment(String comment) {
      if (!StringUtil.isNullString(comment)) {
        this.comment = comment.trim();
      }
      return this;
    }

    ParamInfo setDefaultValue(String val) {
      this.defaultValue = val;
      return this;
    }

    ParamInfo setRelevance(VariableElement field, Relevance relevance) {
      this.rel = relevance;
      return this;
    }

    Relevance getRelevance() {
      return rel;
    }

    ParamInfo addDefinedIn(String cls) {
      definedIn.add(cls);
      return this;
    }

    Set<String> getDefinedIn() {
      return definedIn;
    }

    ParamInfo addTag(DocTree tag) {
      tags.add((BlockTagTree) tag);
      return this;
    }

    ParamInfo addCategory(String cat) {
      categories.add(cat);
      categoryMap.put(cat, this);
      return this;
    }

    Set<String> getCategories() {
      return categories;
    }
  }

  /*
   * Common paramdoc strings
   * @ParamRelevance Required
   * @ParamRelevance Common
   * @ParamRelevance Rare
   * @ParamRelevance BackwardCompatibility



   * @ParamCategory Tuning
   * @ParamCategory Crawler
   * @ParamCategory Poller
   * @ParamCategory

   */
}
