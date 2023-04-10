/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.rs.io.storage.warc;

import org.lockss.log.L4JLogger;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.util.PatternIntMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Version of LocalWarcArtifactDataStore that allows manipulating apparent
 * free space
 */
public class TestingWarcArtifactDataStore extends LocalWarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();
  private PatternIntMap freeSpacePatternMap;

  public TestingWarcArtifactDataStore(File basePath) throws IOException {
    super(basePath);
  }

  public TestingWarcArtifactDataStore(File[] basePath)
      throws IOException {
    super(basePath);
  }

  public TestingWarcArtifactDataStore(Path basePaths)
      throws IOException {
    super(basePaths);
  }

  public TestingWarcArtifactDataStore(Path[] basePaths)
      throws IOException {
    super(basePaths);
  }

  @Override
  protected long getFreeSpace(Path fsPath) {
    if (freeSpacePatternMap != null) {
      long fake = freeSpacePatternMap.getMatch(fsPath.toString(), -1);
      if (fake > 0) {
	log.debug("Returning fake free space for {}: {}", fsPath, fake);
	return fake;
      }
    }
    return super.getFreeSpace(fsPath);
  }

  public void setTestingDiskSpaceMap(PatternIntMap freeSpacePatternMap) {
    this.freeSpacePatternMap = freeSpacePatternMap;
    log.debug2("freeSpacePatternMap: {}", freeSpacePatternMap);
  }

}
