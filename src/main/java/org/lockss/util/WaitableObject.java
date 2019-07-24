/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University
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

package org.lockss.util;

import java.util.Random;

/** Combines an object value with a semaphore that's filled when the value
 * is set, to support processes waiting for asynchronous initialization of
 * some needed object.  Similar to Future but with less baggage. */
public class WaitableObject<V> {

  private V value;
  private OneShotSemaphore sem = new OneShotSemaphore();
  private long waitTime;

  /** Create a WaitableObject whose waitValue() will wait for maxWaitTime
   * by default */
  public WaitableObject(long maxWaitTime) {
    this.waitTime = maxWaitTime;
  }
    
  /** Create a WaitableObject whose waitValue() will wait for 15 seconds by
   * default */
  public WaitableObject() {
    this(15 * Constants.SECOND);
  }
    
  /** Return true if the value has been set */
  public boolean hasValue() {
    return sem.peek();
  }

  /** Return the value if it has been set, else null.  Does not wait. */
  public V getValue() {
    return value;
  }

  /** Return the value, waiting if necessary until it's set.  Wait time is
   * set a construction time */
  public V waitValue() {
    return waitValue(waitTime);
  }

  /** Return the value, waiting if necessary for the specified amount of
   * time until it's set */
  public V waitValue(long wait) {
    if (sem.peek()) {
      return value;
    }
    try {
      if (sem.waitFull(Deadline.in(wait))) {
	return value;
      }
    } catch (InterruptedException e) {
      // no action
    }
    throw new IllegalStateException("LockssApp was not instantiated");
  }

  /** Set the value, triggering any waiting processes */
  public V setValue(V value) {
    this.value = value;
    sem.fill();
    return value;
  }

  /** For testing, reset to an unset state */
  public void reset() {
    sem = new OneShotSemaphore();
    value = null;
  }
}
