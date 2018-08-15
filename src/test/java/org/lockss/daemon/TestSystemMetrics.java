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

package org.lockss.daemon;

import java.io.IOException;
import java.util.Properties;

import org.lockss.test.*;
import org.lockss.util.time.TimeBase;

/**
 * Test class for SystemMetrics.
 */
public class TestSystemMetrics extends LockssTestCase {
  private SystemMetrics metrics;
  private MockLockssDaemon theDaemon;

  public void setUp() throws Exception {
    super.setUp();
    theDaemon = getMockLockssDaemon();
    Properties props = new Properties();
    props.setProperty(SystemMetrics.PARAM_HASH_TEST_DURATION,
                      Long.toString(SystemMetrics.DEFAULT_HASH_TEST_DURATION));
    props.setProperty(SystemMetrics.PARAM_HASH_TEST_BYTE_STEP,
                      Integer.toString(SystemMetrics.DEFAULT_HASH_TEST_BYTE_STEP));
    props.setProperty(SystemMetrics.PARAM_DEFAULT_HASH_SPEED,
                      Integer.toString(SystemMetrics.DEFAULT_DEFAULT_HASH_SPEED));
    ConfigurationUtil.setCurrentConfigFromProps(props);
    theDaemon.getHashService().startService();
    metrics = theDaemon.getSystemMetrics();
    metrics.startService();
  }

  public void testHashEstimation() throws IOException {
    int byteCount = SystemMetrics.DEFAULT_HASH_TEST_BYTE_STEP * 10;
    int estimate = byteCount;
    long duration;
    int expectedMin;

    while (true) {
      MockCachedUrlSetHasher hasher = new MockCachedUrlSetHasher(byteCount);
      hasher.setHashStepDelay(10);

      // wipe out cached estimate
      metrics.estimateTable.clear();
      long startTime = TimeBase.nowMs();
      estimate = metrics.measureHashSpeed(hasher, new MockMessageDigest());
      duration = TimeBase.msSince(startTime);
      expectedMin =
          (byteCount * 10) / SystemMetrics.DEFAULT_HASH_TEST_BYTE_STEP;

      if ((estimate!=byteCount) && (duration != expectedMin)) {
        // non-zero hash time
        break;
      } else {
        // if hash time is 0, estimate equals # of bytes
        // we want to run until hashing takes actual time
        // increase byte count and try again
        byteCount *= 10;
      }
    }

    assertTrue(estimate < byteCount);
    // minimum amount of time would be delay * number of hash steps
    assertTrue(duration > expectedMin);
  }

  public void testBytesPerMsHashEstimate() throws IOException {
    assertEquals(250, metrics.getBytesPerMsHashEstimate());
    ConfigurationUtil.setFromArgs(SystemMetrics.PARAM_DEFAULT_HASH_SPEED,
				  "4437");
    // wipe out cached estimate
    assertEquals(4437, metrics.getBytesPerMsHashEstimate());
  }

  public void testEstimationCaching() throws IOException {
    MockCachedUrlSetHasher hasher = new MockCachedUrlSetHasher(10000);
    int estimate = metrics.getBytesPerMsHashEstimate(hasher, new MockMessageDigest());
    assertEquals(SystemMetrics.DEFAULT_DEFAULT_HASH_SPEED, estimate);
    hasher = new MockCachedUrlSetHasher(10);
    int estimate2 = metrics.getBytesPerMsHashEstimate(hasher, new MockMessageDigest());
    assertEquals(estimate, estimate2);
  }

}
