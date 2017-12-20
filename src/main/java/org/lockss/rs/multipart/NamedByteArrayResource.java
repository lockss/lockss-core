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

import org.springframework.core.io.ByteArrayResource;

/**
 * Named resource useful to create HTTP Multipart requests.
 */
public class NamedByteArrayResource extends ByteArrayResource {
  private String name;
  private String description;

  /**
   * Constructor.
   * @param name A String with the name of the resource.
   * @param byteArray A byte[] with the resource content.
   */
  public NamedByteArrayResource(String name, byte[] byteArray) {
    super(byteArray);
    this.name = name;
  }

  /**
   * Constructor.
   * @param name A String with the name of the resource.
   * @param byteArray A byte[] with the resource content.
   * @param description A String with the description of the resource.
   */
  public NamedByteArrayResource(String name, byte[] byteArray,
      String description) {
    super(byteArray, description);
    this.name = name;
    this.description = (description != null ? description : "");
  }

  @Override
  public String getFilename() {
    return name;
  }

  @Override
  public String getDescription() {
    return "Named byte array resource [name: " + getFilename() + ", byteCount: "
	+ getByteArray().length + ", description: " + description + "]";
  }
}
