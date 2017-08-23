/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.uiapi.util;

import org.lockss.test.LockssTestCase;


/**
 * Test Transaction components
 */
public class TestKeyedList extends LockssTestCase implements ApiParameters {

  private final static String     N1        = "name1";
  private final static String     N2        = "name2";
  private final static String     N3        = "name3";

  private final static String     V1        = "value1";
  private final static String     V2        = "value2";
  private final static String     V3        = "value3";

  private final static String[]   _names    = {N1, N2, N3};
  private final static String[]   _values   = {V1, V2, V3};

  private KeyedList  _keyedList;


  public void setUp() throws Exception {

    super.setUp();

    _keyedList = new KeyedList();
  }

  private int populateList() {
    int length = _names.length;

    for (int i = 0; i < length; i++) {
      _keyedList.put(_names[i], _values[i]);
    }

    return length;
  }

  /*
   * Verify basic state - can we populate the list?
   */
  public void testPopulate() throws Exception {
    int length;

    assertEquals(0, _keyedList.size());
    length = populateList();
    assertEquals(length, _keyedList.size());
  }

  /*
   * Is list order preserved?
   */
  public void testListOrder() throws Exception {
    int length = populateList();

    for (int i = 0; i < length; i++) {
      assertEquals(_names[i],  (String) _keyedList.getKey(i));
      assertEquals(_values[i], (String) _keyedList.getValue(i));
    }
  }

  /*
   * Fetch values by name?
   */
  public void testGet() throws Exception {
    String value;

    populateList();

    value = (String) _keyedList.get(N1);
    assertEquals(V1, value);

    value = (String) _keyedList.get(N3);
    assertEquals(V3, value);

    value = (String) _keyedList.get("no_such_value");
    assertNull(value);
  }

  /*
   * Verify support for multiple occurances of the same name in the list
   */
  public void testPutMultiple() throws Exception {
    int length;

    /*
     * Add multiple ocurances to the list
     *
     * Duplicate names should not be overwritted
     */
    length = populateList();
    assertEquals(_names.length * 1, length);

    length += populateList();
    assertEquals(_names.length * 2, length);

    assertEquals(_names[0],  (String) _keyedList.getKey(0));
    assertEquals(_values[0], (String) _keyedList.getValue(0));

    assertEquals(_names[0],  (String) _keyedList.getKey(0 + _names.length));
    assertEquals(_values[0], (String) _keyedList.getValue(0 + _names.length));
    /*
     * Order should be preserved
     */
    _keyedList.put("X", "abc");
    _keyedList.put("X", "def");
    _keyedList.put("X", "ghi");

    assertEquals("X",   (String) _keyedList.getKey(length));
    assertEquals("abc", (String) _keyedList.getValue(length));

    assertEquals("X",   (String) _keyedList.getKey(length + 2));
    assertEquals("ghi", (String) _keyedList.getValue(length + 2));
    /*
     * Get() should only fetch the first matching value
     */
    assertEquals("abc", (String) _keyedList.get("X"));
  }

  /*
   * Does put() guard against null names and values?
   */
  public void testPutNull() throws Exception {
    boolean error;

    error = false;
    try {
      _keyedList.put(null, "value");
    } catch (IllegalArgumentException exception) {
      error = true;
    }
    if (!error) {
      fail("put(null, value) should have thrown InvalidArgumentException");
    }
  }

}
