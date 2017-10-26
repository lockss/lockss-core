/*

 Copyright (c) 2017 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.rs.multipart;

import java.io.IOException;
import java.util.LinkedHashMap;
import org.junit.Test;
import org.lockss.rs.multipart.TextMultipartResponse.Part;
import org.lockss.test.LockssTestCase4;
import org.springframework.http.HttpHeaders;

/**
 * Test class for org.lockss.rs.multipart.TestTextMultipartResponse.
 */
public class TestTextMultipartResponse extends LockssTestCase4 {
  @Test
  public void testGetBoundaryFromContentTypeHeader() throws IOException {
    assertNull(getBoundaryFromContentTypeHeader("multipart/form-data"));
    assertNull(getBoundaryFromContentTypeHeader("multipart/form-data;"));
    assertNull(getBoundaryFromContentTypeHeader(
	"multipart/form-data boundary="));
    assertEquals("",
	getBoundaryFromContentTypeHeader("multipart/form-data; boundary="));

    String boundary = "RNe0BIAreyG6O17qrdIpjVea1nbzrn460UR5A";
    assertEquals(boundary, getBoundaryFromContentTypeHeader(
	"multipart/form-data; boundary=" + boundary));
    assertEquals(boundary, getBoundaryFromContentTypeHeader(
	"boundary=" + boundary + ";multipart/form-data"));
    assertEquals(boundary, getBoundaryFromContentTypeHeader(
	"multipart/form-data; boundary=" + boundary + "; abcd"));
  }

  @Test
  public void testGetPartNameFromContentDispositionHeader() {
    assertNull(getPartNameFromContentDispositionHeader("form-data"));
    assertNull(getPartNameFromContentDispositionHeader("form-data;"));
    assertNull(getPartNameFromContentDispositionHeader("form-data name="));
    assertEquals("",
	getPartNameFromContentDispositionHeader("form-data; name=\"\""));

    String name = "config-data";
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"form-data; name=\"" + name + "\""));
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"name=\"" + name + "\";form-data"));
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"form-data; name=\"" + name + "\";abcd"));
  }

  @Test
  public void testGetName() {
    assertNull(getName("form-data"));
    assertNull(getName("form-data;"));
    assertNull(getName("form-data name="));
    assertEquals("", getName("form-data; name=\"\""));

    String name = "config-data";
    assertEquals(name, getName("form-data; name=\"" + name + "\""));
    assertEquals(name, getName("name=\"" + name + "\";form-data"));
    assertEquals(name, getName("form-data; name=\"" + name + "\";abcd"));
  }

  @Test
  public void testAddPart() {
    TextMultipartResponse response = new TextMultipartResponse();

    Part part = addPart("form-data", response);
    assertEquals(1, response.getParts().size());
    assertEquals("Part-0", part.getName());

    part = addPart("form-data;", response);
    assertEquals(2, response.getParts().size());
    assertEquals("Part-1", part.getName());

    part = addPart("form-data name=", response);
    assertEquals(3, response.getParts().size());
    assertEquals("Part-2", part.getName());

    part = addPart("form-data; name=\"\"", response);
    assertEquals(4, response.getParts().size());
    assertEquals("", part.getName());

    part = addPart("form-data; name=\" \"", response);
    assertEquals(4, response.getParts().size());
    assertEquals("", part.getName());

    String name = "config-data";

    part = addPart("form-data; name=\"" + name + "\"", response);
    assertEquals(5, response.getParts().size());
    assertEquals("config-data", part.getName());

    part = addPart("name=\"" + name + "\";form-data", response);
    assertEquals(5, response.getParts().size());
    assertEquals("config-data", part.getName());

    part = addPart("form-data; name=\"" + name + "\";abcd", response);
    assertEquals(5, response.getParts().size());
    assertEquals("config-data", part.getName());
  }

  @Test
  public void testParseResponseBody() throws IOException {
    TextMultipartResponse response =
	createTextMultipartResponse("multipart/form-data", "");
    assertEquals(0, response.getParts().size());

    response =
	createTextMultipartResponse("multipart/form-data; boundary=", "");
    assertEquals(0, response.getParts().size());

    response =
	createTextMultipartResponse("multipart/form-data; boundary=", "");
    assertEquals(0, response.getParts().size());

    String boundary = "RNe0BIAreyG6O17qrdIpjVea1nbzrn460UR5A";
    String contentType = "multipart/form-data; boundary=" + boundary;

    response = createTextMultipartResponse(contentType, "");
    assertEquals(0, response.getParts().size());

    String body = "--" + boundary;

    try {
      response = createTextMultipartResponse(contentType, body);
      fail("Should have thrown IOException: Premature end of body");
    } catch (IOException ioe) {
      assertEquals("Premature end of body", ioe.getMessage());
    }

    assertEquals(0, response.getParts().size());

    String payload1 = "testKey1=testValue3" + System.lineSeparator()
    + "org.lockss.config.fileVersion.expert_config=1" + System.lineSeparator()
    + "org.lockss.log.WarcExploder.level=info" + System.lineSeparator()
    + System.lineSeparator();

    String payload2 = "This is a test" + System.lineSeparator();

    body = body + System.lineSeparator()
    + "Content-Disposition: form-data; name=\"config-data\""
    + System.lineSeparator()
    + "Content-Type: text/html" + System.lineSeparator()
    + "Is-Xml: false" + System.lineSeparator()
    + "last-modified: 1508428538000" + System.lineSeparator()
    + "Content-Length: 105" + System.lineSeparator()
    + System.lineSeparator()
    + payload1
    + "--" + boundary + System.lineSeparator()
    + "Content-Disposition: form-data; name=\"test-sig\""
    + System.lineSeparator()
    + "Content-Type: application/octet-stream" + System.lineSeparator()
    + "Content-Length: 14" + System.lineSeparator()
    + System.lineSeparator()
    + payload2;

    try {
      response = createTextMultipartResponse(contentType, body);
      fail("Should have thrown IOException: Premature end of body");
    } catch (IOException ioe) {
      assertEquals("Premature end of body", ioe.getMessage());
    }

    assertEquals(0, response.getParts().size());

    body = body + "--" + boundary + "--" + System.lineSeparator();
    response = createTextMultipartResponse(contentType, body);
    LinkedHashMap<String, Part> parts = response.getParts();
    assertEquals(2, parts.size());

    int i = 0;

    for (String key : parts.keySet()) {
      if (i == 0) {
	assertEquals("config-data", key);
      } else {
	assertEquals("test-sig", key);
      }

      Part part = parts.get(key);
      assertEquals(key, part.getName());

      HttpHeaders headers = part.getHeaders();
      String payload = part.getPayload();

      if (i == 0) {
	assertEquals("text/html", headers.getFirst("Content-Type"));
	assertEquals("false", headers.getFirst("Is-Xml"));
	assertEquals("1508428538000", headers.getFirst("last-modified"));
	assertEquals("105", headers.getFirst("Content-Length"));
	assertEquals(payload1, payload);
      } else {
	assertEquals("application/octet-stream",
	    headers.getFirst("Content-Type"));
	assertFalse(headers.containsKey("Is-Xml"));
	assertFalse(headers.containsKey("last-modified"));
	assertEquals("14", headers.getFirst("Content-Length"));
	assertEquals(payload2, payload);
      }

      i++;
    }
  }

  private String getBoundaryFromContentTypeHeader(String contentType)
      throws IOException {
    return createTextMultipartResponse(contentType, null)
	.getBoundaryFromContentTypeHeader(contentType);
  }

  private TextMultipartResponse createTextMultipartResponse(String contentType,
      String body) throws IOException {
    HttpHeaders headers = new HttpHeaders();
    headers.add("Content-Type", contentType);

    TextMultipartResponse response = new TextMultipartResponse();
    response.setResponseHeaders(headers);

    if (body != null) {
      response.parseResponseBody(body);
    }

    return response;
  }

  private String getPartNameFromContentDispositionHeader(
      String contentDisposition) {
    return createPart(contentDisposition)
	.getPartNameFromContentDispositionHeader(contentDisposition);
  }

  private Part createPart(String contentDisposition) {
    HttpHeaders headers = new HttpHeaders();
    headers.add("Content-Disposition", contentDisposition);

    Part part = new Part();
    part.setHeaders(headers);

    return part;
  }

  private String getName(String contentDisposition) {
    return createPart(contentDisposition).getName();
  }

  private Part addPart(String contentDisposition,
      TextMultipartResponse response) {
    Part part = createPart(contentDisposition);
    response.addPart(part);

    return part;
  }
}
