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

import java.net.URI;
import org.lockss.rs.exception.LockssRestException;
import org.lockss.rs.exception.LockssRestHttpException;
import org.lockss.rs.exception.LockssRestNetworkException;
import org.lockss.util.Logger;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

public class RestUtil {
  private static Logger log = Logger.getLogger();

  /**
   * Performs a call to a REST service.
   * 
   * @param restTemplate
   *          A RestTemplate with the REST template to be used to access the
   *          REST service.
   * @param uri
   *          A String with the URI of the request to the REST service.
   * @param method
   *          An HttpMethod with the method of the request to the REST service.
   * @param requestEntity
   *          An HttpEntity with the entity of the request to the REST service.
   * @param responseType
   *          A {@code Class<T>} with the expected type of the response to the
   *          request to the REST service.
   * @param exceptionMessage
   *          A String with the message to be returned if there are errors.
   * @return a {@code ResponseEntity<T>} with the response to the request to the
   *         REST service.
   * @throws LockssRestException
   *           if there are problems making the request to the REST service.
   */
  public static <T> ResponseEntity<T> callRestService(RestTemplate restTemplate,
      URI uri, HttpMethod method, HttpEntity<?> requestEntity,
      Class<T> responseType, String exceptionMessage)
	  throws LockssRestException {
    if (log.isDebug2()) {
      log.debug2("uri = " + uri);
      log.debug2("method = " + method);
      log.debug2("requestEntity = " + requestEntity);
      log.debug2("responseType = " + responseType);
      log.debug2("exceptionMessage = " + exceptionMessage);
    }

    try {
      // Make the call to the REST service and get the response.
      ResponseEntity<T> response =
	  restTemplate.exchange(uri, method, requestEntity, responseType);

      // Get the response status.
      HttpStatus statusCode = response.getStatusCode();
      if (log.isDebug3()) log.debug3("statusCode = " + statusCode);

      // Check whether the call status code indicated failure.
      if (!isSuccess(statusCode)) {
	// Yes: Report it back to the caller.
	LockssRestHttpException lrhe =
	    new LockssRestHttpException(exceptionMessage);
	lrhe.setHttpStatusCode(statusCode.value());
	// XXX this is the stock reason phrase, not what was received in
	// the response.  How to get the actual reason?
	lrhe.setHttpStatusMessage(statusCode.getReasonPhrase());
	lrhe.setHttpResponseHeaders(response.getHeaders());
	if (log.isDebug2()) log.debug3("lrhe = " + lrhe);

	throw lrhe;
      }

      // No: Return the received response.
      return response;
    } catch (RestClientException rce) {
      // Get the cause, or this exception if there is no cause.
      Throwable cause = rce.getCause();

      if (cause == null) {
	cause = rce;
      }

      // Report the problem back to the caller.
      LockssRestNetworkException lrne =
	new LockssRestNetworkException(exceptionMessage + ": " +
				       ExceptionUtils.getRootCauseMessage(cause),
				       cause);
      if (log.isDebug2()) log.debug3("lrne = " + lrne);

      throw lrne;
    }
  }

  /**
   * Provides an indication of whether a successful response has been obtained.
   * 
   * @param statusCode
   *          An HttpStatus with the response status code.
   * @return a boolean with <code>true</code> if a successful response has been
   *         obtained, <code>false</code> otherwise.
   */
  public static boolean isSuccess(HttpStatus statusCode) {
    return statusCode.is2xxSuccessful();
  }
}
