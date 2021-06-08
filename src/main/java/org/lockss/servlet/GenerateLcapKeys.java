/*

Copyright (c) 2000-2021 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

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

package org.lockss.servlet;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.List;
import java.util.regex.*;

import javax.servlet.*;

import org.apache.commons.io.*;
import org.mortbay.html.*;

import org.lockss.config.*;
import org.lockss.util.*;
import org.lockss.util.net.*;
import org.lockss.util.io.*;
import org.lockss.keystore.*;
import org.lockss.jetty.*;

/** Simple servlet to invoke EditKeyStores */
public class GenerateLcapKeys extends LockssServlet {
  static Logger log = Logger.getLogger();

  /** Initial dimensions in characters of ExpertConfig textarea:
   * <code><i>width</i>,<i>height</i></code> or min:max range of
   * dimensions:
   * <code><i>min_width</i>,<i>min_height</i>:<i>max_width</i>,<i>max_height</i></code> */
  public static final String PARAM_TEXT_DIMENSIONS =
    Configuration.PREFIX + "generateKeystores.textDimensions";
  public static final String DEFAULT_TEXT_DIMENSIONS = "60,8:100,40";

  private static final String KEY_HOSTS = "hosts";
  private static final String KEY_KSTYPE = "kstype";
  private static final String KEY_FILETYPE = "filetype";
  private static final String KEY_SHARED = "shared";
  private static final String KEY_OLD_KEYSTORE_FILE = "oldKeystore";

  private static final String PUB_KEYSTORE_NAME = "pub-keystore";

  public static final String ACTION_GENERATE = "Generate Keystores";
  public static final String I18N_ACTION_GENERATE = i18n.tr("Generate Keystores");

  private static final String FILE_TYPES[] = {"zip", "tgz"};

  private static final String FOOT_HOSTNAMES =
    "Fully-qualified hostname of each LOCKSS box for which to generate a keystore, space- or newline-separated.";
  private static final String FOOT_OLD_KEYSTORE =
    "Public keystore to which to add certificates for additional hosts.";

  // String read from form
  private String hostsStr;
  private List<String> hosts;

  private boolean shared = true;
  private String ksType;
  private String fileType;

  protected boolean isForm;
  private String displayResult;

  public void init(ServletConfig config) throws ServletException {
    super.init(config);
  }

  // don't hold onto objects after request finished
  protected void resetLocals() {
    hostsStr = null;
    super.resetLocals();
  }

  protected void lockssHandleRequest() throws IOException {
    String action = getParameter("action");
    try {
      getMultiPartRequest();
      if (multiReq != null) {
        action = multiReq.getString("action");
        log.debug("action" + " = " + action);
      }
    } catch (FormDataTooLongException e) {
      errMsg = "Uploaded file too large: " + e.getMessage();
      // leave action null, will call displayAuSummary() below
    }

    isForm = !StringUtil.isNullString(action);

    if (isForm) {
      readForm();
    } else {
    }

    if (errMsg == null && ACTION_GENERATE.equals(action)) {
      doGenerate();
    } else {
      displayPage();
    }
  }

  protected void readForm() {
    hostsStr = getParameter(KEY_HOSTS);
    ksType = getParameter(KEY_KSTYPE);
    fileType = getParameter(KEY_FILETYPE);
//     shared = Boolean.valueOf(getParameter(KEY_SHARED));
    if (StringUtil.isNullString(hostsStr)) {
      errMsg = "At least one hostname must be supplied";
    } else {
      String normHosts = hostsStr.replaceAll("\\s+", " ").trim();
      hosts = StringUtil.breakAt(normHosts, " ");
      log.debug("normHosts: " + normHosts);
      log.debug(hosts.size() + " hosts: " + hosts);
      if (hosts.isEmpty()) {
        errMsg = "At least one hostname must be supplied";
      }
      for (String x : hosts) {
        if (!HostnameUtil.isValidHostname(x)) {
          errMsg = "Invalid hostname: " + x;
          break;
        }
      }
    }
  }

  protected void doGenerate() throws IOException {
    File tmpdir = null;
    File outfile = null;
    File pubStore = new File(tmpdir, PUB_KEYSTORE_NAME);
    try {
      tmpdir = FileUtil.createTempDir("genkeys", null);

      String oldStoreName = multiReq.getFilename(KEY_OLD_KEYSTORE_FILE);
      if (!StringUtil.isNullString(oldStoreName)) {
        File oldStoreFile = new File(tmpdir, oldStoreName);
        try (InputStream ins = multiReq.getInputStream(KEY_OLD_KEYSTORE_FILE);
             OutputStream outs = new FileOutputStream(oldStoreFile)) {
          IOUtils.copy(ins, outs);
          log.debug("Wrote old pub keystore to " + oldStoreFile);
          pubStore = oldStoreFile;
        }
      }
      if (createKeystores(tmpdir, pubStore)) {
        File outFile = null;
        try {
          outFile = createOutputFile(tmpdir);
          serveFile(outFile);
        } finally {
          FileUtils.deleteQuietly(outFile);
        }
      } else {
        displayPage();
      }
    } finally {
      FileUtils.deleteQuietly(tmpdir);
    }
  }

  protected boolean createKeystores(File keyDir, File pubStore)
      throws IOException {
    try {
      EditKeyStores eks = EditKeyStores.newBuilder()
        .setHosts(hosts)
        .setKsType(ksType)
        .setHosts(hosts)
        .setOutDir(keyDir.toString())
        .setPubFile(pubStore.toString())
        .build();
      eks.generate();
      if (eks.isSuccess()) {
        statusMsg = eks.getResult();
        return true;
      } else {
        errMsg = eks.getResult();
        return false;
      }
    } catch (Exception e) {
      log.error("Error generating keys", e);
      errMsg = "Error: Couldn't generate keys: " + e.toString();
      return false;
    }
  }

  private File createOutputFile(File keyDir) throws IOException {
    switch (fileType) {
    case "tgz":
      return createTarFile(keyDir);
    case "zip":
      return createZipFile(keyDir);
    }
    throw new IllegalArgumentException("Unknown file format: " + fileType);
  }

  private File createZipFile(File keyDir) throws IOException {
    File zipFile = FileUtil.createTempFile("keyzip", ".zip");
    DirArchiver dc = DirArchiver.makeZipArchiver()
      .setSourceDir(keyDir)
      .setOutFile(zipFile);
    try {
      dc.build();
    } catch (IOException e) {
      zipFile.delete();
      throw e;
    }
    return zipFile;
  }

  private File createTarFile(File keyDir) throws IOException {
    File tarFile = FileUtil.createTempFile("keytar", ".tgz");
    DirArchiver dc = DirArchiver.makeTarArchiver()
      .setSourceDir(keyDir)
      .setOutFile(tarFile);
    dc.build();
    return tarFile;
  }

  private void serveFile(File keysFile) throws IOException {
    try (InputStream in = new FileInputStream(keysFile)) {
      resp.setContentType("application/binary");
      resp.setHeader("Content-disposition",
                     "attachment; filename=\""
                     + "keystores." + fileType
                     + "\"");
      try (OutputStream out = resp.getOutputStream()) {
        StreamUtil.copy(in, out);
        out.close();
      } catch (FileNotFoundException e) {
        errMsg = "No output file was generated.";
      } catch (IOException e) {
        log.warning("serveFile()", e);
        throw e;
      }
    }
  }

  /**
   * Display the form.
   */
  private void displayPage() throws IOException {
    // Create and start laying out page
    Page page = newPage();
    addJavaScript(page);
    layoutErrorBlock(page);
//     ServletUtil.layoutExplanationBlock(page, "Explanation");
    page.add("<br>");

    // Create and layout form
    Form form = ServletUtil.newForm(srvURL(myServletDescr()));
    form.attribute("enctype", "multipart/form-data");
    Table table = new Table(0, "align=\"center\" cellspacing=\"2\"");
    table.newRow();
    table.newCell("align=center");
    table.add(makeTextArea(("Hostnames"
			    + addFootnote(FOOT_HOSTNAMES)),
			   KEY_HOSTS, hostsStr));
    table.newRow();
    table.newCell("align=center");
    table.add("<b>Keystore type:</b> ");
    boolean first = true;
    for (KeyStoreUtil.KsType kst : KeyStoreUtil.KsType.values()) {
      table.add(ServletUtil.radioButton(this, KEY_KSTYPE, kst.toString(),
                                        first));
      first = false;
    }
    table.newRow();
    table.newCell("align=center");
    table.add("<b>Download file type:</b> ");
    for (String ft : FILE_TYPES) {
      table.add(ServletUtil.radioButton(this, KEY_FILETYPE, ft,
                                        "zip".equals(ft)));
    }
    table.newRow();
    table.newCell("align=center");
    table.add("<b>Existing public keystore:</b>" +
              addFootnote(FOOT_OLD_KEYSTORE) + " ");
    table.add(new Input(Input.File, KEY_OLD_KEYSTORE_FILE));
    table.newRow();
    table.newCell("align=center");
    ServletUtil.layoutSubmitButton(this, table, ACTION_GENERATE, I18N_ACTION_GENERATE);
    form.add(table);
    page.add(form);
    if (displayResult != null) {
      page.add(displayResult);
    }

    // Finish laying out page
    endPage(page);
  }

  void spaceRow(Table table) {
    table.newRow();
    table.newCell();
    table.add("&nbsp;");
  }

  Composite makeTextArea(String title, String fieldName, String value) {

    StringUtil.CharWidthHeight cwh = StringUtil.countWidthAndHeight(value);
    int cols = maxWidth <= 0 ? minWidth : between(cwh.getWidth(),
						  minWidth, maxWidth);
    int rows = maxHeight <= 0 ? minHeight : between(cwh.getHeight(),
						    minHeight, maxHeight);

    Table table = new Table(0, "align=\"center\" cellpadding=\"0\"");
    TextArea urlArea = new MyTextArea(fieldName);
    urlArea.attribute("class", "resize");
    urlArea.setSize(cols, rows);
    urlArea.add(value);
    setTabOrder(urlArea);
    table.newRow();
    table.addHeading(title, "align=\"center\"");
    table.newRow();
    table.newCell("align=center");
    table.add(urlArea);
//     table.newRow();
//     table.newCell("align=center");
    return table;
  }


  int between(int n, int lo, int hi) {
    if (n < lo) return lo;
    if (n > hi) return hi;
    return n;
  }

  /** Return "Expert Config" or "Expert Config (local)" */
  private String getMyServletName() {
    return myServletDescr().getRawHeading();
  }


  // Patterm to match text dimension spec.  ddd,ddd or ddd,ddd:ddd,ddd
  static Pattern DIMENSION_SPEC_PAT =
    Pattern.compile("(\\d+),(\\d+)(?::(\\d+),(\\d+))?");


  /** Called by org.lockss.config.MiscConfig
   */
  public static void setConfig(Configuration config,
			       Configuration oldConfig,
			       Configuration.Differences diffs) {
    if (diffs.contains(PARAM_TEXT_DIMENSIONS)) {
      String whspec =
	config.get(PARAM_TEXT_DIMENSIONS, DEFAULT_TEXT_DIMENSIONS);
      try {
	parseSpec(whspec);
      } catch (NumberFormatException e) {
	parseSpec(DEFAULT_TEXT_DIMENSIONS);
      }
      log.debug2("minWidth: " + minWidth + ", maxWidth: " + maxWidth +
		 ", minHeight: " + minHeight + ", maxHeight: " + maxHeight);
    }

  }

  private static int minWidth;
  private static int maxWidth;
  private static int minHeight;
  private static int maxHeight;

  static void parseSpec(String s) throws NumberFormatException {
    Matcher m1 = DIMENSION_SPEC_PAT.matcher(s);
    if (m1.matches()) {
      minWidth = Integer.parseInt(m1.group(1));
      minHeight = Integer.parseInt(m1.group(2));
      if (m1.start(3) > 0) {
	maxWidth = Integer.parseInt(m1.group(3));
	maxHeight = Integer.parseInt(m1.group(4));
      } else {
	maxWidth = 0;
	maxHeight = 0;
      }
    } else {
      throw new NumberFormatException("Invalied dimension spec: '" + s + "'");
    }
  }
}
