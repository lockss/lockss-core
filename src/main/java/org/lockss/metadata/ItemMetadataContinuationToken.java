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
package org.lockss.metadata;

import java.util.Vector;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;

/**
 * The continuation token used to paginate through the metadata items of an
 * Archival Unit.
 */
public class ItemMetadataContinuationToken {
  protected static final Logger log = Logger.getLogger();

  private Long auExtractionTimestamp = null;
  private Long lastItemMdItemSeq = null;

  /**
   * Constructor from a web request continuation token.
   * 
   * @param webRequestContinuationToken
   *          A String with the web request continuation token.
   * @throws IllegalArgumentException
   *           if the web request continuation token is not syntactically valid.
   */
  public ItemMetadataContinuationToken(String webRequestContinuationToken)
  	throws IllegalArgumentException {
    if (log.isDebug2()) log.debug2("webRequestContinuationToken = "
	+ webRequestContinuationToken);

    String message = "Invalid web request continuation token '"
	+ webRequestContinuationToken + "'";

    // Check whether a non-empty web request continuation token has been passed.
    if (webRequestContinuationToken != null
	&& !webRequestContinuationToken.trim().isEmpty()) {
      // Yes: Parse it.
      Vector<String> tokenItems = null;

      try {
	tokenItems =
	    StringUtil.breakAt(webRequestContinuationToken.trim(), "-");
	if (log.isDebug3()) log.debug3("tokenItems = " + tokenItems);

	auExtractionTimestamp = Long.valueOf(tokenItems.get(0).trim());
	if (log.isDebug3())
	  log.debug3("auExtractionTimestamp = " + auExtractionTimestamp);

	lastItemMdItemSeq = Long.valueOf(tokenItems.get(1).trim());
	if (log.isDebug3())
	  log.debug3("lastItemMdItemSeq = " + lastItemMdItemSeq);
      } catch (Exception e) {
	log.warning(message, e);
	throw new IllegalArgumentException(message, e);
      }

      // Validate the format of the web request continuation token.
      if (tokenItems.size() != 2) {
	log.warning(message);
	throw new IllegalArgumentException(message);
      }

      validateMembers();
    }
  }

  /**
   * Constructor from members.
   * 
   * @param auExtractionTimestamp
   *          A Long with the Archival Unit last extraction timestamp.
   * @param lastItemMdItemSeq
   *          A Long with the last metadata item database identifier.
   */
  public ItemMetadataContinuationToken(Long auExtractionTimestamp,
      Long lastItemMdItemSeq) {
    this.auExtractionTimestamp = auExtractionTimestamp;
    this.lastItemMdItemSeq = lastItemMdItemSeq;

    validateMembers();
  }

  public Long getAuExtractionTimestamp() {
    return auExtractionTimestamp;
  }

  public Long getLastItemMdItemSeq() {
    return lastItemMdItemSeq;
  }

  public String toWebResponseContinuationToken() {
    if (auExtractionTimestamp != null && lastItemMdItemSeq != null) {
      return auExtractionTimestamp + "-" + lastItemMdItemSeq;
    }

    return null;
  }

  @Override
  public String toString() {
    return "[ItemMetadataContinuationToken auExtractionTimestamp="
	+ auExtractionTimestamp + ", lastItemMdItemSeq=" + lastItemMdItemSeq
	+ "]";
  }

  /**
   * Verifies the validity of the members of this class.
   */
  private void validateMembers() {
    // Validate that both members are both null or both non-null. 
    if ((auExtractionTimestamp == null && lastItemMdItemSeq != null)
	|| (auExtractionTimestamp != null && lastItemMdItemSeq == null)) {
      String message = "Invalid member combination: auExtractionTimestamp = '"
	  + auExtractionTimestamp + "', lastItemMdItemSeq = '"
	  + lastItemMdItemSeq + "'";
      log.warning(message);
      throw new IllegalArgumentException(message);
    }

    // Validate that neither member is negative.
    if (auExtractionTimestamp != null
	&& auExtractionTimestamp.longValue() < 0) {
      String message = "Invalid member: auExtractionTimestamp = '"
	+ auExtractionTimestamp + "'";
      log.warning(message);
      throw new IllegalArgumentException(message);
    }

    if (lastItemMdItemSeq != null && lastItemMdItemSeq.longValue() < 0) {
      String message =
	  "Invalid member: lastItemMdItemSeq = '" + lastItemMdItemSeq + "'";
      log.warning(message);
      throw new IllegalArgumentException(message);
    }
  }
}
