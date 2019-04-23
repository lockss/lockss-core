/*

Copyright (c) 2016-2019 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.doconly;

import org.lockss.config.Configuration;

/**
 * Config params duplicated for ParamDoc purposes from
 * laaws-metadataextractor-common:org.lockss.metadata.extractor.job.JobManager
 */
public class JobManager {
  /**
   * Prefix for configuration properties.
   */
  public static final String PREFIX = Configuration.PREFIX + "jobManager.";

  /**
   * Set to true to allow JobManager to run.
   */
  public static final String PARAM_JOBMANAGER_ENABLED = PREFIX + "enabled";
  public static final boolean DEFAULT_JOBMANAGER_ENABLED = false;

  /**
   * The number of job processing tasks.
   */
  public static final String PARAM_TASK_LIST_SIZE = PREFIX + "taskListSize";

  /** 
   * The default number of job processing tasks.
   */
  public static final int DEFAULT_TASK_LIST_SIZE = 1;

  /**
   * The sleep delay when no jobs are ready.
   */
  public static final String PARAM_SLEEP_DELAY_SECONDS =
      PREFIX + "sleepDelaySeconds";

  /** 
   * The default sleep delay when no jobs are ready.
   */
  public static final long DEFAULT_SLEEP_DELAY_SECONDS = 60;

}
