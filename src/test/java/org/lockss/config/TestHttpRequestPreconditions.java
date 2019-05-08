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

import static org.lockss.config.HttpRequestPreconditions.HTTP_WEAK_VALIDATOR_PREFIX;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lockss.test.LockssTestCase4;
import org.lockss.util.ListUtil;

/**
 * Test class for <code>org.lockss.config.HttpRequestPreconditions</code>
 */
public class TestHttpRequestPreconditions extends LockssTestCase4 {
  private static final String EMPTY_STRING = "";
  private static final String ZERO = "0";
  private static final String NUMBER = "9876543210";
  private static final String TEXT = "text";

  // Preconditions.
  private static final String EMPTY_PRECONDITION = "\"\"";
  private static final String NUMERIC_PRECONDITION = "\"1234567890\"";
  private static final String ALPHA_PRECONDITION = "\"ABCD\"";

  private static final String WEAK_PRECONDITION =
      HTTP_WEAK_VALIDATOR_PREFIX + NUMERIC_PRECONDITION;

  private static final String ASTERISK_PRECONDITION = "*";

  private static final List<String> EMPTY_PRECONDITION_LIST =
      new ArrayList<String>();

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testConstructors() throws Exception {
    HttpRequestPreconditions hrp = new HttpRequestPreconditions();
    hrp = new HttpRequestPreconditions(null, null, null, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(false);
    hrp = new HttpRequestPreconditions(null, null, null, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(true);
    hrp = new HttpRequestPreconditions(null, null, null, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp =
	new HttpRequestPreconditions(EMPTY_PRECONDITION_LIST, null, null, null);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(EMPTY_PRECONDITION_LIST, EMPTY_STRING,
	null, null);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(EMPTY_PRECONDITION_LIST, EMPTY_STRING,
	EMPTY_PRECONDITION_LIST, null);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(EMPTY_PRECONDITION_LIST, EMPTY_STRING,
	EMPTY_PRECONDITION_LIST, EMPTY_STRING);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfNoneMatch());
    assertEquals(EMPTY_STRING, hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(false, EMPTY_PRECONDITION_LIST,
	EMPTY_STRING, EMPTY_PRECONDITION_LIST, EMPTY_STRING);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfNoneMatch());
    assertEquals(EMPTY_STRING, hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(true, EMPTY_PRECONDITION_LIST,
	EMPTY_STRING, EMPTY_PRECONDITION_LIST, EMPTY_STRING);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfNoneMatch());
    assertEquals(EMPTY_STRING, hrp.getIfUnmodifiedSince());

    // If-Match.

    List<String> ifMatch = ListUtil.list(EMPTY_STRING);

    try {
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(NUMBER);
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    ifMatch = ListUtil.list(NUMERIC_PRECONDITION);
    hrp = new HttpRequestPreconditions(ifMatch, null, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp = new HttpRequestPreconditions(ifMatch, null, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifMatch = ListUtil.list(WEAK_PRECONDITION);

    try {
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      new HttpRequestPreconditions(false, ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    hrp = new HttpRequestPreconditions(true, ifMatch, null, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifMatch = ListUtil.list(NUMERIC_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(ASTERISK_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(ASTERISK_PRECONDITION, EMPTY_PRECONDITION);
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(ALPHA_PRECONDITION, WEAK_PRECONDITION);
      new HttpRequestPreconditions(ifMatch, null, null, null);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    ifMatch = ListUtil.list(EMPTY_PRECONDITION, NUMERIC_PRECONDITION,
	ALPHA_PRECONDITION);
    hrp = new HttpRequestPreconditions(ifMatch, null, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(false, ifMatch, null, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(true, ifMatch, null, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    // If-Modified-Since.

    String ifModifiedSince = EMPTY_STRING;
    hrp = new HttpRequestPreconditions(ifMatch, ifModifiedSince, null, null);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = ZERO;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = NUMBER;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = TEXT;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = EMPTY_PRECONDITION;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = NUMERIC_PRECONDITION;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = ASTERISK_PRECONDITION;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = WEAK_PRECONDITION;
    hrp = new HttpRequestPreconditions(null, ifModifiedSince, null, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifModifiedSince = ZERO;
      new HttpRequestPreconditions(ifMatch, ifModifiedSince, null, null);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    // If-None-Match.

    List<String> ifNoneMatch = ListUtil.list(EMPTY_STRING);

    try {
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(NUMBER);
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    ifNoneMatch = ListUtil.list(NUMERIC_PRECONDITION);
    hrp = new HttpRequestPreconditions(null, null, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp = new HttpRequestPreconditions(null, null, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifNoneMatch = ListUtil.list(WEAK_PRECONDITION);

    try {
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      new HttpRequestPreconditions(false, null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    hrp = new HttpRequestPreconditions(true, null, null, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifNoneMatch = ListUtil.list(NUMERIC_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION, EMPTY_PRECONDITION);
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(ALPHA_PRECONDITION, WEAK_PRECONDITION);
      new HttpRequestPreconditions(null, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    ifNoneMatch = ListUtil.list(EMPTY_PRECONDITION, NUMERIC_PRECONDITION,
	ALPHA_PRECONDITION);
    hrp = new HttpRequestPreconditions(null, null, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      new HttpRequestPreconditions(ifMatch, null, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      new HttpRequestPreconditions(ifMatch, ifModifiedSince, ifNoneMatch, null);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    hrp =
	new HttpRequestPreconditions(null, ifModifiedSince, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp =
	new HttpRequestPreconditions(null, ifModifiedSince, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifNoneMatch = ListUtil.list(NUMERIC_PRECONDITION);
    hrp =
	new HttpRequestPreconditions(null, ifModifiedSince, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp =
	new HttpRequestPreconditions(null, ifModifiedSince, ifNoneMatch, null);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    // If-Unmodified-Since.

    String ifUnmodifiedSince = EMPTY_STRING;
    hrp = new HttpRequestPreconditions(null, null, ifNoneMatch,
	ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = ZERO;
    hrp = new HttpRequestPreconditions(null, null, null, ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = NUMBER;
    hrp = new HttpRequestPreconditions(null, null, null, ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = TEXT;
    hrp = new HttpRequestPreconditions(null, null, null, ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = EMPTY_PRECONDITION;
    hrp = new HttpRequestPreconditions(null, null, null, ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = NUMERIC_PRECONDITION;
    hrp = new HttpRequestPreconditions(null, null, null, ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    try {
      new HttpRequestPreconditions(null, ifModifiedSince, null,
	  ifUnmodifiedSince);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      new HttpRequestPreconditions(null, null, ifNoneMatch, ifUnmodifiedSince);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      new HttpRequestPreconditions(ifMatch, null, ifNoneMatch,
	  ifUnmodifiedSince);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      new HttpRequestPreconditions(ifMatch, ifModifiedSince, ifNoneMatch,
	  ifUnmodifiedSince);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    ifUnmodifiedSince = EMPTY_STRING;
    hrp = new HttpRequestPreconditions(ifMatch, null, null,
	ifUnmodifiedSince);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = ZERO;
    hrp = new HttpRequestPreconditions(ifMatch, null, null, ifUnmodifiedSince);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = NUMBER;
    hrp = new HttpRequestPreconditions(ifMatch, null, null, ifUnmodifiedSince);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());
  }

  @Test
  public void testSetters() throws Exception {
    HttpRequestPreconditions hrp = new HttpRequestPreconditions();
    hrp.setIfMatch(null).setIfModifiedSince(null).setIfNoneMatch(null)
    .setIfUnmodifiedSince(null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(true);
    hrp.setIfMatch(null).setIfModifiedSince(null).setIfNoneMatch(null)
    .setIfUnmodifiedSince(null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp = new HttpRequestPreconditions(false);
    hrp.setIfMatch(null).setIfModifiedSince(null).setIfNoneMatch(null)
    .setIfUnmodifiedSince(null);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp.setIfMatch(EMPTY_PRECONDITION_LIST);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp.setIfModifiedSince(EMPTY_STRING);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp.setIfNoneMatch(EMPTY_PRECONDITION_LIST);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    hrp.setIfUnmodifiedSince(EMPTY_STRING);
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfMatch());
    assertEquals(EMPTY_STRING, hrp.getIfModifiedSince());
    assertEquals(EMPTY_PRECONDITION_LIST, hrp.getIfNoneMatch());
    assertEquals(EMPTY_STRING, hrp.getIfUnmodifiedSince());

    // If-Match.

    List<String> ifMatch = ListUtil.list(EMPTY_STRING);

    try {
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(NUMBER);
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    ifMatch = ListUtil.list(NUMERIC_PRECONDITION);
    hrp = new HttpRequestPreconditions().setIfMatch(ifMatch);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp = hrp.setIfMatch(ifMatch);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifMatch = ListUtil.list(WEAK_PRECONDITION);
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(WEAK_PRECONDITION);
      new HttpRequestPreconditions(false).setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    ifMatch = ListUtil.list(WEAK_PRECONDITION);
    hrp = new HttpRequestPreconditions(true).setIfMatch(ifMatch);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifMatch = ListUtil.list(NUMERIC_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(ASTERISK_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(ASTERISK_PRECONDITION, EMPTY_PRECONDITION);
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifMatch = ListUtil.list(ALPHA_PRECONDITION, WEAK_PRECONDITION);
      new HttpRequestPreconditions().setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    ifMatch = ListUtil.list(EMPTY_PRECONDITION, NUMERIC_PRECONDITION,
	ALPHA_PRECONDITION);
    hrp = new HttpRequestPreconditions().setIfMatch(ifMatch);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    // If-Modified-Since.

    String ifModifiedSince = EMPTY_STRING;
    hrp = hrp.setIfModifiedSince(ifModifiedSince);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = ZERO;
    hrp = new HttpRequestPreconditions().setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = NUMBER;
    hrp = hrp.setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = TEXT;
    hrp = hrp.setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = EMPTY_PRECONDITION;
    hrp = hrp.setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = NUMERIC_PRECONDITION;
    hrp = hrp.setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifModifiedSince = ZERO;
    hrp = new HttpRequestPreconditions().setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      hrp.setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    // If-None-Match.

    List<String> ifNoneMatch = ListUtil.list(EMPTY_STRING);

    try {
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(NUMBER);
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for an unquoted ETag");
    } catch (IllegalArgumentException iae) {
    }

    ifNoneMatch = ListUtil.list(NUMERIC_PRECONDITION);
    hrp = new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp = hrp.setIfNoneMatch(ifNoneMatch);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifNoneMatch = ListUtil.list(WEAK_PRECONDITION);
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(WEAK_PRECONDITION);
      new HttpRequestPreconditions(false).setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a weak validator ETag");
    } catch (IllegalArgumentException iae) {
    }

    ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp = new HttpRequestPreconditions(true).setIfNoneMatch(ifNoneMatch);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      ifNoneMatch = ListUtil.list(NUMERIC_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION, ASTERISK_PRECONDITION);
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION, EMPTY_PRECONDITION);
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    try {
      ifNoneMatch = ListUtil.list(ALPHA_PRECONDITION, WEAK_PRECONDITION);
      new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a bad If-None-Match "
	  + "precondition set");
    } catch (IllegalArgumentException iae) {
    }

    ifNoneMatch = ListUtil.list(EMPTY_PRECONDITION, NUMERIC_PRECONDITION,
	ALPHA_PRECONDITION);
    hrp = new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      hrp.setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    hrp = new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch)
	.setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    try {
      hrp.setIfMatch(ifMatch);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    ifNoneMatch = ListUtil.list(NUMERIC_PRECONDITION);
    hrp = new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch)
	.setIfModifiedSince(ifModifiedSince);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    ifNoneMatch = ListUtil.list(ASTERISK_PRECONDITION);
    hrp = hrp.setIfNoneMatch(ifNoneMatch);
    assertNull(hrp.getIfMatch());
    assertEquals(ifModifiedSince, hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertNull(hrp.getIfUnmodifiedSince());

    // If-Unmodified-Since.

    String ifUnmodifiedSince = EMPTY_STRING;
    hrp = new HttpRequestPreconditions().setIfNoneMatch(ifNoneMatch)
	.setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertEquals(ifNoneMatch, hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = ZERO;
    hrp =
	new HttpRequestPreconditions().setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = NUMBER;
    hrp = hrp.setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = TEXT;
    hrp = hrp.setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = EMPTY_PRECONDITION;
    hrp = hrp.setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    ifUnmodifiedSince = NUMERIC_PRECONDITION;
    hrp = hrp.setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    try {
      hrp.setIfModifiedSince(ifModifiedSince);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    ifUnmodifiedSince = NUMBER;
    hrp =
	new HttpRequestPreconditions().setIfUnmodifiedSince(ifUnmodifiedSince);
    assertNull(hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    try {
      hrp.setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    ifUnmodifiedSince = ZERO;
    hrp = new HttpRequestPreconditions().setIfUnmodifiedSince(ifUnmodifiedSince)
	.setIfMatch(ifMatch);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());

    try {
      hrp.setIfNoneMatch(ifNoneMatch);
      fail("HttpRequestPreconditions() should throw for a bad precondition "
	  + "set");
    } catch (IllegalArgumentException iae) {
    }

    ifUnmodifiedSince = EMPTY_STRING;
    hrp = new HttpRequestPreconditions().setIfMatch(ifMatch)
	.setIfUnmodifiedSince(ifUnmodifiedSince);
    assertEquals(ifMatch, hrp.getIfMatch());
    assertNull(hrp.getIfModifiedSince());
    assertNull(hrp.getIfNoneMatch());
    assertEquals(ifUnmodifiedSince, hrp.getIfUnmodifiedSince());
  }
}
