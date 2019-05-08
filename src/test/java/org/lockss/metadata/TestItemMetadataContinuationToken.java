/*

Copyright (c) 2018, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

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

import org.junit.Test;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for org.lockss.metadata.ItemMetadataContinuationToken.
 */
public class TestItemMetadataContinuationToken extends LockssTestCase5 {

  @Test
  public void testWebRequestContinuationTokenConstructor() {
    ItemMetadataContinuationToken imct =
	new ItemMetadataContinuationToken(null);
    assertNull(imct.getAuExtractionTimestamp());
    assertNull(imct.getLastItemMdItemSeq());
    assertNull(imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken("");
    assertNull(imct.getAuExtractionTimestamp());
    assertNull(imct.getLastItemMdItemSeq());
    assertNull(imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken(" ");
    assertNull(imct.getAuExtractionTimestamp());
    assertNull(imct.getLastItemMdItemSeq());
    assertNull(imct.toWebResponseContinuationToken());

    try {
      imct = new ItemMetadataContinuationToken("-");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken(" - ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("ABC-");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("1234-");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("1234-XYZ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("1234--5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("-XYZ");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("ABC-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("-1234-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken("1234-9-5678");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    imct = new ItemMetadataContinuationToken("1234-0");
    assertEquals(1234, imct.getAuExtractionTimestamp().longValue());
    assertEquals(0, imct.getLastItemMdItemSeq().longValue());
    assertEquals("1234-0", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken("0-5678");
    assertEquals(0, imct.getAuExtractionTimestamp().longValue());
    assertEquals(5678, imct.getLastItemMdItemSeq().longValue());
    assertEquals("0-5678", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken("1234-5678");
    assertEquals(1234, imct.getAuExtractionTimestamp().longValue());
    assertEquals(5678, imct.getLastItemMdItemSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken(" 1234 - 5678 ");
    assertEquals(1234, imct.getAuExtractionTimestamp().longValue());
    assertEquals(5678, imct.getLastItemMdItemSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken("01234-005678");
    assertEquals(1234, imct.getAuExtractionTimestamp().longValue());
    assertEquals(5678, imct.getLastItemMdItemSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken("0-0");
    assertEquals(0, imct.getAuExtractionTimestamp().longValue());
    assertEquals(0, imct.getLastItemMdItemSeq().longValue());
    assertEquals("0-0", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken("00-000");
    assertEquals(0, imct.getAuExtractionTimestamp().longValue());
    assertEquals(0, imct.getLastItemMdItemSeq().longValue());
    assertEquals("0-0", imct.toWebResponseContinuationToken());
  }

  @Test
  public void testMemberConstructor() {
    ItemMetadataContinuationToken imct =
	new ItemMetadataContinuationToken(null, null);
    assertNull(imct.getAuExtractionTimestamp());
    assertNull(imct.getLastItemMdItemSeq());
    assertNull(imct.toWebResponseContinuationToken());

    try {
      imct = new ItemMetadataContinuationToken(null, 5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken(-1234L, 5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken(1234L, null);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken(1234L, -5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    try {
      imct = new ItemMetadataContinuationToken(-1234L, -5678L);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
    }

    imct = new ItemMetadataContinuationToken(0l, 0L);
    assertEquals(0, imct.getAuExtractionTimestamp().longValue());
    assertEquals(0, imct.getLastItemMdItemSeq().longValue());
    assertEquals("0-0", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken(1234L, 0L);
    assertEquals(1234, imct.getAuExtractionTimestamp().longValue());
    assertEquals(0, imct.getLastItemMdItemSeq().longValue());
    assertEquals("1234-0", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken(0L, 5678L);
    assertEquals(0, imct.getAuExtractionTimestamp().longValue());
    assertEquals(5678, imct.getLastItemMdItemSeq().longValue());
    assertEquals("0-5678", imct.toWebResponseContinuationToken());

    imct = new ItemMetadataContinuationToken(1234L, 5678L);
    assertEquals(1234, imct.getAuExtractionTimestamp().longValue());
    assertEquals(5678, imct.getLastItemMdItemSeq().longValue());
    assertEquals("1234-5678", imct.toWebResponseContinuationToken());
  }
}
