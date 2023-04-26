/*

Copyright (c) 2010-2017 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.exporter;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.*;
import org.archive.io.warc.*;
import org.archive.uid.RecordIDGenerator;
import org.archive.uid.UUIDGenerator;
import org.archive.util.*;
import org.archive.util.anvl.*;
import static org.archive.io.warc.WARCConstants.*;
import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;
import org.lockss.plugin.*;

/**
 * Export to an ARC file
 */
public class WarcExporter extends Exporter {

  private static Logger log = Logger.getLogger();

  protected CIProperties arcProps = null;
  AtomicInteger serialNo = new AtomicInteger(0);
  WARCWriter ww;
  boolean isResponse;

  public WarcExporter(LockssDaemon daemon, ArchivalUnit au,
		      boolean isResponse) {
    super(daemon, au);
    this.isResponse = isResponse;
  }

  protected void start() throws IOException {
    ww = makeWARCWriter();
  }

  protected void finish() throws IOException {
    ww.close();
  }

  private WARCWriter makeWARCWriter() {
    Properties props = new Properties();
    props.put("software", getSoftwareVersion());
    props.put("ip", getHostIp());
    props.put("hostname", getHostName());
    props.put("format","WARC File Format 0.17");
    props.put("conformsTo",
	      "http://crawler.archive.org/warc/0.17/WARC0.17ISO.doc");
    props.put("created", ArchiveUtils.getLog14Date(TimeBase.nowMs()));
    props.put("description", au.getName());
    props.put("robots", "ignore");
    props.put("http-header-user-agent", LockssDaemon.getUserAgent());
    List<String> metadata = new ArrayList<String>();
    for (Map.Entry ent : props.entrySet()) {
      String key = (String)ent.getKey();
      metadata.add((String)ent.getKey() + ": "
		   + (String)ent.getValue() + "\r\n");
    }

    String template = "${prefix}-${timestamp17}-${serialno}";
    RecordIDGenerator generator = new UUIDGenerator();

    WARCWriterPoolSettingsData settings = new WARCWriterPoolSettingsData(
	prefix, template, maxSize >= 0 ? maxSize : Long.MAX_VALUE, compress,
	ListUtil.list(dir), metadata, generator);

    return new WARCWriter(serialNo, settings);
  }

  protected void writeCu(CachedUrl cu) throws IOException {
    String url = cu.getUrl();
    long contentSize = cu.getContentSize();
    CIProperties props = cu.getProperties();
    ANVLRecord headers = new ANVLRecord(5);
    headers.addLabelValue(HEADER_KEY_IP, getHostIp());

    // XXX Temporary for LuKII; replace with ExporterHelper
    TypedEntryMap auProps = au.getProperties();
    if (auProps.containsKey("article_base")) {
      // Add field to WARC header
      String pattern = auProps.getString("article_base");
      Pattern p = Pattern.compile(pattern);
      Matcher m = p.matcher(url);
      if (m.find()) {
	headers.addLabelValue("LOCKSS-Article-Base", m.group(1));
      }
    }

    long fetchTime = Long.parseLong(AuUtil.getFetchTimeString(props));
    String timestamp = ArchiveUtils.getLog14Date(fetchTime);
    InputStream contentIn = cu.getUnfilteredInputStream();
    try {
      // Web Archive Commons now uses a properties file.
      WARCRecordInfo wRInfo = new WARCRecordInfo();
      wRInfo.setUrl(xlateFilename(url));
      wRInfo.setCreate14DigitDate(timestamp);
      wRInfo.setRecordId(new UUIDGenerator().getRecordID());
      wRInfo.setExtraHeaders(headers);
      if (isResponse) {
	String hdrString = getHttpResponseString(cu);
	long size = contentSize + hdrString.length();
	InputStream headerIn =
	  new ReaderInputStream(new StringReader(hdrString));
	InputStream concat = new SequenceInputStream(headerIn, contentIn);
	try {
//HC3	  ww.writeResponseRecord(xlateFilename(url),
//HC3				 timestamp,
//HC3				 HTTP_RESPONSE_MIMETYPE,
//HC3				 new UUIDGenerator().getRecordID(),
//HC3				 headers, concat, size);
	  wRInfo.setType(WARCRecordType.response);
	  wRInfo.setMimetype(HTTP_RESPONSE_MIMETYPE);
	  wRInfo.setContentStream(concat);
	  wRInfo.setContentLength(size);
	  ww.writeRecord(wRInfo);
	} finally {
	  IOUtil.safeClose(concat);
	}
      } else {
	String mimeType =
	  HeaderUtil.getMimeTypeFromContentType(cu.getContentType());
//HC3	ww.writeResourceRecord(xlateFilename(url),
//HC3			       timestamp,
//HC3			       mimeType,
//HC3			       headers, contentIn, contentSize);
	wRInfo.setType(WARCRecordType.resource);
	wRInfo.setMimetype(mimeType);
	wRInfo.setContentStream(contentIn);
	wRInfo.setContentLength(contentSize);
	ww.writeRecord(wRInfo);
      }
      
      // Web Archive Commons now requires this call to roll over to  the next
      // file, if necessary.
      ww.checkSize();
      File openFile = ww.getFile();
      if (openFile.toString().endsWith(WARCConstants.OCCUPIED_SUFFIX)) {
	openFile =
	  new File(StringUtil.removeTrailing(openFile.toString(),
					     WARCConstants.OCCUPIED_SUFFIX));
      }
      recordExportFile(openFile);
    } finally {
      IOUtil.safeClose(contentIn);
    }
  }
}
