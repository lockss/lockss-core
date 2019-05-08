/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.config;

import java.io.InputStream;
import org.springframework.http.MediaType;

/**
 * Result of a read or write operation with optional preconditions on a
 * ConfigFile.
 */
public class ConfigFileReadWriteResult {
  private InputStream inputStream;
  private String lastModified;
  private String etag;
  private boolean preconditionsMet = false;
  private MediaType contentType;
  private long contentLength = 0;

  /**
   * Constructor.
   *
   * @param inputStream
   *          An InputStream to the configuration file to be read.
   * @param lastModified
   *          A String with the last modification timestamp.
   * @param etag
   *          A String with the ETag.
   * @param preconditionsMet
   *          A boolean with <code>true</code> if the preconditions were met,
   *          <code>false</code> otherwise.
   * @param contentType
   *          A MediaType with the configuration file content type.
   * @param contentLength
   *          A long with the configuration file length.
   */
  public ConfigFileReadWriteResult(InputStream inputStream, String lastModified,
      String etag, boolean preconditionsMet, MediaType contentType,
      long contentLength) {
    this.inputStream = inputStream;
    this.lastModified = lastModified;
    this.etag = etag;
    this.preconditionsMet = preconditionsMet;
    this.contentType = contentType;
    this.contentLength = contentLength;
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  public String getLastModified() {
    return lastModified;
  }

  public String getEtag() {
    return etag;
  }

  public boolean getPreconditionsMet() {
    return preconditionsMet;
  }

  public boolean isPreconditionsMet() {
    return preconditionsMet;
  }

  public MediaType getContentType() {
    return contentType;
  }

  public long getContentLength() {
    return contentLength;
  }

  @Override
  public String toString() {
    return "[ConfigFileReadWriteResult: inputStream = " + inputStream
	+ ", lastModified = " + lastModified + ", etag = " + etag
	+ ", preconditionsMet = " + preconditionsMet
	+ ", contentType = " + contentType
	+ ", contentLength = " + contentLength + "]";
  }
}
