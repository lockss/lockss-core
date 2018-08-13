/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University,
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

import java.util.*;
import org.junit.jupiter.api.*;
import org.apache.commons.collections4.MapUtils;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.config.*;
import org.lockss.test.*;

/** Runs all the tests in org.lockss.log.TestLogger (in lockss-util)
    against org.lockss.util.Logger, using LOCKSS config params to change
    the log levels.  The static references to the Logger class must be
    repeated here to get the corrent class */
public class TestLogger extends org.lockss.log.TestLogger {

  @BeforeEach
  public void setUp() {
    // TK - LockssTestCase[4] does this, but LockssTestCase5 doesn't,
    // because lockss-util doesn't know about ConfigManager.  What to do?
    ConfigManager.makeConfigManager();
  }

  /** Get the named Logger */
  protected Logger getLogger(String name) {
    Logger res = Logger.getLogger(name);
    return res;
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    commonBeforeAll();
    Logger.forceReload();;
  }

  @Override
  protected void setConfig(Map<String,String> map) {
    ConfigurationUtil.setCurrentConfigFromProps(MapUtils.toProperties(map));
  }
}
