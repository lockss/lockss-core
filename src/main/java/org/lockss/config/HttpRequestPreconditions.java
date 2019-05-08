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

import java.util.List;
import org.lockss.util.Logger;

/**
 * Preconditions to an HTTP request.
 */
public class HttpRequestPreconditions {
  public static String HTTP_WEAK_VALIDATOR_PREFIX = "W/";
  private static Logger log = Logger.getLogger();

  private boolean allowWeakValidatorTags = false;
  private List<String> ifMatch;
  private String ifModifiedSince;
  private List<String> ifNoneMatch;
  private String ifUnmodifiedSince;

  /**
   * No-argument constructor.
   */
  public HttpRequestPreconditions() {
  }

  /**
   * Weak validator constructor.
   * 
   * @param allowWeakValidatorTags
   *          A boolean with <code>true</code> if weak validator tags are
   *          allowed, <code>false</code> otherwise.
   */
  public HttpRequestPreconditions(boolean allowWeakValidatorTags) {
    this.allowWeakValidatorTags = allowWeakValidatorTags;
  }

  /**
   * Precondition tags constructor.
   * 
   * @param ifMatch
   *          A List<String> with the "If-Match" request header, containing an
   *          asterisk or values equivalent to the "If-Unmodified-Since" request
   *          header but with a granularity of 1 ms.
   * @param ifModifiedSince
   *          A String with "If-Modified-Since" request header.
   * @param ifNoneMatch
   *          A List<String> with the "If-None-Match" request header, containing
   *          an asterisk or values equivalent to the "If-Modified-Since"
   *          request header but with a granularity of 1 ms.
   * @param ifUnmodifiedSince
   *          A String with the "If-Unmodified-Since" request header.
   * @throws IllegalArgumentException
   *           if the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  public HttpRequestPreconditions(List<String> ifMatch, String ifModifiedSince,
      List<String> ifNoneMatch, String ifUnmodifiedSince)
	  throws IllegalArgumentException {
    this(false, ifMatch, ifModifiedSince, ifNoneMatch, ifUnmodifiedSince);
  }

  /**
   * Fully-populating constructor.
   * 
   * @param allowWeakValidatorTags
   *          A boolean with <code>true</code> if weak validator tags are
   *          allowed, <code>false</code> otherwise.
   * @param ifMatch
   *          A List<String> with the "If-Match" request header, containing an
   *          asterisk or values equivalent to the "If-Unmodified-Since" request
   *          header but with a granularity of 1 ms.
   * @param ifModifiedSince
   *          A String with "If-Modified-Since" request header.
   * @param ifNoneMatch
   *          A List<String> with the "If-None-Match" request header, containing
   *          an asterisk or values equivalent to the "If-Modified-Since"
   *          request header but with a granularity of 1 ms.
   * @param ifUnmodifiedSince
   *          A String with the "If-Unmodified-Since" request header.
   * @throws IllegalArgumentException
   *           if the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  public HttpRequestPreconditions(boolean allowWeakValidatorTags,
      List<String> ifMatch, String ifModifiedSince, List<String> ifNoneMatch,
      String ifUnmodifiedSince) throws IllegalArgumentException {
    this(allowWeakValidatorTags);
    this.ifMatch = ifMatch;
    this.ifModifiedSince = ifModifiedSince;
    this.ifNoneMatch = ifNoneMatch;
    this.ifUnmodifiedSince = ifUnmodifiedSince;
    validate();
  }

  public List<String> getIfMatch() {
    return ifMatch;
  }

  /**
   * Sets the "If-Match" precondition.
   * 
   * @param ifMatch
   *          A List<String> with the "If-Match" request header, containing an
   *          asterisk or values equivalent to the "If-Unmodified-Since" request
   *          header but with a granularity of 1 ms.
   * @throws IllegalArgumentException
   *           if the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  public HttpRequestPreconditions setIfMatch(List<String> ifMatch)
      throws IllegalArgumentException {
    this.ifMatch = ifMatch;
    validate();
    return this;
  }

  public String getIfModifiedSince() {
    return ifModifiedSince;
  }

  /**
   * Sets the "If-Modified-Since" precondition.
   * 
   * @param ifModifiedSince
   *          A String with "If-Modified-Since" request header.
   * @throws IllegalArgumentException
   *           if the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  public HttpRequestPreconditions setIfModifiedSince(String ifModifiedSince)
      throws IllegalArgumentException {
    this.ifModifiedSince = ifModifiedSince;
    validate();
    return this;
  }

  public List<String> getIfNoneMatch() {
    return ifNoneMatch;
  }

  /**
   * Sets the "If-None-Match" precondition.
   * 
   * @param ifNoneMatch
   *          A List<String> with the "If-None-Match" request header, containing
   *          an asterisk or values equivalent to the "If-Modified-Since"
   *          request header but with a granularity of 1 ms.
   * @throws IllegalArgumentException
   *           if the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  public HttpRequestPreconditions setIfNoneMatch(List<String> ifNoneMatch)
      throws IllegalArgumentException {
    this.ifNoneMatch = ifNoneMatch;
    validate();
    return this;
  }

  public String getIfUnmodifiedSince() {
    return ifUnmodifiedSince;
  }

  /**
   * Sets the "If-Modified-Since" precondition.
   * 
   * @param ifUnmodifiedSince
   *          A String with the "If-Unmodified-Since" request header.
   * @throws IllegalArgumentException
   *           if the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  public HttpRequestPreconditions setIfUnmodifiedSince(String ifUnmodifiedSince)
      throws IllegalArgumentException {
    this.ifUnmodifiedSince = ifUnmodifiedSince;
    validate();
    return this;
  }

  /**
   * Validates the current combination of preconditions.
   * 
   * @throws IllegalArgumentException
   *           if the syntax of the individual preconditions is invalid or if
   *           the combination of preconditions would result in an undefined
   *           result according to the standard specification at
   *           https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html.
   */
  private void validate() throws IllegalArgumentException {
    final String DEBUG_HEADER = "validate(): ";
    String message = null;

    boolean ifMatchExists = ifMatch != null && !ifMatch.isEmpty();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "ifMatchExists = " + ifMatchExists);

    boolean ifModifiedSinceExists =
	ifModifiedSince != null && !ifModifiedSince.isEmpty();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "ifModifiedSinceExists = " + ifModifiedSinceExists);

    boolean ifNoneMatchExists = ifNoneMatch != null && !ifNoneMatch.isEmpty();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "ifNoneMatchExists = " + ifNoneMatchExists);

    boolean ifUnmodifiedSinceExists =
	ifUnmodifiedSince != null && !ifUnmodifiedSince.isEmpty();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "ifUnmodifiedSinceExists = " + ifUnmodifiedSinceExists);

    // Check whether there are both If-Match and If-None-Match preconditions.
    if (ifMatchExists && ifNoneMatchExists) {
      // Yes: Check whether there is an If-Modified-Since precondition.
      if (ifModifiedSinceExists) {
	// Yes: Report the problem.
	message = "Invalid presence of If-Match, If-None-Match and "
	    + "If-Modified-Since preconditions";
      } else if (ifUnmodifiedSinceExists) {
	// Yes: Report the problem.
	message = "Invalid presence of If-Match, If-None-Match and "
	    + "If-Unmodified-Since preconditions";
      } else {
	// No: Report the problem.
	message =
	    "Invalid presence of both If-Match and If-None-Match preconditions";
      }

      log.error(message);
      throw new IllegalArgumentException(message);
    }

    // Check whether there are both If-Match and If-Modified-Since
    // preconditions.
    if (ifMatchExists && ifModifiedSinceExists) {
      // Yes: Report the problem.
      message = "Invalid presence of both If-Match and If-Modified-Since "
	  + "preconditions";
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    // Check whether there are both If-Modified-Since and If-Unmodified-Since
    // preconditions.
    if (ifModifiedSinceExists && ifUnmodifiedSinceExists) {
      // Yes: Report the problem.
      message = "Invalid presence of both If-Modified-Since and "
	  + "If-Unmodified-Since preconditions";
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    // Check whether there are both If-None-Match and If-Unmodified-Since
    // preconditions.
    if (ifNoneMatchExists && ifUnmodifiedSinceExists) {
      // Yes: Report the problem.
      message = "Invalid presence of both If-None-Match and "
	  + "If-Unmodified-Since preconditions";
      log.error(message);
      throw new IllegalArgumentException(message);
    }

    // Check whether there are any If-Match precondition tags.
    if (ifMatchExists) {
      // Yes: Validate the syntax of the If-Match precondition tags.
      validateTagSyntax(ifMatch, "If-Match");
    }

    // Check whether there are any If-None-Match precondition tags.
    if (ifNoneMatchExists) {
      // Yes: Validate the syntax of the If-None-Match precondition tags.
      validateTagSyntax(ifNoneMatch, "If-None-Match");
    }
  }

  /**
   * Validates the syntax of a list of "If-Match" or "If-None-Match"
   * precondition tags.
   * 
   * @param tagList
   *          A List<String> with the list of tags to be validated.
   * @param tagType
   *          A String with the type of tags tobe validated.
   * @throws IllegalArgumentException
   *           if the syntax of the individual preconditions is invalid.
   */
  private void validateTagSyntax(List<String> tagList, String tagType)
      throws IllegalArgumentException {
    final String DEBUG_HEADER = "validateTagSyntax(): ";
    String message = null;

    // Loop through the tag list precondition tags.
    for (String tag : tagList) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "tag = " + tag);

      // Check whether it is a weak validator tag.
      if (tag.toUpperCase().startsWith(HTTP_WEAK_VALIDATOR_PREFIX)) {
	// Yes: Check whether weak validator tags are not allowed.
	if (!allowWeakValidatorTags) {
	  // Yes: Report the problem.
	  message = "Invalid " + tagType + " entity tag '" + tag + "'";
	  log.error(message);
	  throw new IllegalArgumentException(message);
	}
	// No: Check whether it is an asterisk.
      } else if ("*".equals(tag)) {
	// Yes: Check whether the asterisk does not appear just by itself.
	if (tagList.size() > 1) {
	  // Yes: Report the problem.
	  message = "Invalid " + tagType + " entity tag mix";
	  log.error(message);
	  throw new IllegalArgumentException(message);
	}
	// No: Check whether a normal tag is not delimited by double quotes.
      } else if (!tag.startsWith("\"") || !tag.endsWith("\"")) {
	// Yes: Report the problem.
	message = "Invalid " + tagType + " entity tag '" + tag + "'";
	log.error(message);
	throw new IllegalArgumentException(message);
      }
    }
  }

  @Override
  public String toString() {
    return "[HttpRequestPreconditions allowWeakValidatorTags="
	+ allowWeakValidatorTags + ", ifMatch=" + ifMatch + ", ifModifiedSince="
	+ ifModifiedSince + ", ifNoneMatch=" + ifNoneMatch
	+ ", ifUnmodifiedSince=" + ifUnmodifiedSince + "]";
  }
}
