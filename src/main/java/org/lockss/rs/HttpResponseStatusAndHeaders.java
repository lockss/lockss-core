/*

 Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.rs;

import java.net.SocketTimeoutException;
import org.lockss.rs.exception.LockssRestException;
import org.lockss.rs.exception.LockssRestHttpException;
import org.lockss.rs.exception.LockssRestNetworkException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

/**
 * Encapsulation of an HTTP status code, message and response headers.
 */
public class HttpResponseStatusAndHeaders {
  private int code;
  private String message;
  private HttpHeaders headers;

  /**
   * Constructor from fields.
   *
   * @param code An int with the response status code.
   * @param message A String with the response status message.
   * @param headers An HttpHeaders with the response headers.
   */
  public HttpResponseStatusAndHeaders(int code, String message,
      HttpHeaders headers) {
    super();
    this.code = code;
    this.message = message;
    this.headers = headers;
  }

  public int getCode() {
    return code;
  }
  public HttpResponseStatusAndHeaders setCode(int code) {
    this.code = code;
    return this;
  }

  public String getMessage() {
    return message;
  }
  public HttpResponseStatusAndHeaders setMessage(String message) {
    this.message = message;
    return this;
  }

  public HttpHeaders getHeaders() {
    return headers;
  }
  public HttpResponseStatusAndHeaders setMessage(HttpHeaders headers) {
    this.headers = headers;
    return this;
  }

  /**
   * Map a LockssRestException into an object of this class.
   * 
   * @param lre
   *          A LockssRestException with the exception to be mapped.
   * @return an HttpStatusCodeAndMessage with the mapping result.
   */
  public static HttpResponseStatusAndHeaders fromLockssRestException(
      LockssRestException lre) {
    if (lre == null) {
      return null;
    }

    // Check whether it is a network exception.
    if (lre instanceof LockssRestNetworkException) {
      // Yes: Check whether it is a timeout exception.
      if (lre.getCause() instanceof SocketTimeoutException) {
	// Yes.
	return new HttpResponseStatusAndHeaders(
	    HttpStatus.GATEWAY_TIMEOUT.value(), lre.getMessage(), null);
      }

      // No.
      return new HttpResponseStatusAndHeaders(HttpStatus.BAD_GATEWAY.value(),
	  lre.getMessage(), null);
      // Check whether it is an HTTP exception.
    } else if (lre instanceof LockssRestHttpException) {
      // Yes.
      LockssRestHttpException lrhe =(LockssRestHttpException)lre;

      return new HttpResponseStatusAndHeaders(lrhe.getHttpStatusCode(),
	  lrhe.getHttpStatusMessage(), lrhe.getHttpResponseHeaders());
    } else {
      // No: It is an unknown exception type.
      return new HttpResponseStatusAndHeaders(
	  HttpStatus.INTERNAL_SERVER_ERROR.value(), "Unknown exception type",
	  null);
    }
  }

  @Override
  public String toString() {
    return "[HttpStatusCodeAndMessage code=" + code + ", message=" + message
	+ ", headers=" + headers + "]";
  }
}
