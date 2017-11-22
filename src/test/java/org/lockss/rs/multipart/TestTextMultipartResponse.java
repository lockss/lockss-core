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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.lockss.rs.multipart.TextMultipartResponse.Part;
import org.lockss.test.LockssTestCase4;

/**
 * Test class for org.lockss.rs.multipart.TestTextMultipartResponse.
 */
public class TestTextMultipartResponse extends LockssTestCase4 {
  /**
   * Tests the extraction of the part name from the Content-Disposition header.
   */
  @Test
  public void testGetPartNameFromContentDispositionHeader() {
    assertNull(getPartNameFromContentDispositionHeader("form-data"));
    assertNull(getPartNameFromContentDispositionHeader("form-data;"));
    assertNull(getPartNameFromContentDispositionHeader("form-data name="));

    String name = "";
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"form-data; name=\"" + name + "\""));

    name = "config-data";
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"form-data; name=\"" + name + "\""));
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"name=\"" + name + "\";form-data"));
    assertEquals(name, getPartNameFromContentDispositionHeader(
	"form-data; name=\"" + name + "\";abcd"));
  }

  /**
   * Provides the part name given a Content-Disposition header.
   * 
   * @param contentDisposition
   *          A String with the Content-Disposition header.
   * @return a String with the part name.
   */
  private String getPartNameFromContentDispositionHeader(
      String contentDisposition) {
    return createPart(contentDisposition)
	.getPartNameFromContentDispositionHeader(contentDisposition);
  }

  /**
   * Creates a part given a Content-Disposition header.
   * 
   * @param contentDisposition
   *          A String with the Content-Disposition header.
   * @return a Part with the created part.
   */
  private Part createPart(String contentDisposition) {
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Disposition", contentDisposition);

    Part part = new Part();
    part.setHeaders(headers);

    return part;
  }

  /**
   * Tests the definition of a part name.
   */
  @Test
  public void testGetName() {
    assertNull(getName("form-data"));
    assertNull(getName("form-data;"));
    assertNull(getName("form-data name="));

    String name = "";
    assertEquals(name, getName("form-data; name=\"" + name + "\""));

    name = "config-data";
    assertEquals(name, getName("form-data; name=\"" + name + "\""));
    assertEquals(name, getName("name=\"" + name + "\";form-data"));
    assertEquals(name, getName("form-data; name=\"" + name + "\";abcd"));
  }

  /**
   * Provides the name of a part given a Content-Disposition header.
   * @param contentDisposition
   *          A String with the Content-Disposition header.
   * @return a String with the part name.
   */
  private String getName(String contentDisposition) {
    return createPart(contentDisposition).getName();
  }

  /**
   * Tests the addition of parts.
   */
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

  /**
   * Adds a part to a TextMultipartResponse.
   * 
   * @param contentDisposition
   *          A String with the Content-Disposition header.
   * @param response
   *          A TextMultipartResponse where to add the part.
   * @return a Part with the part just added.
   */
  private Part addPart(String contentDisposition,
      TextMultipartResponse response) {
    Part part = createPart(contentDisposition);
    response.addPart(part);

    return part;
  }
}
