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

import java.util.Map;
import java.util.Objects;

/**
 * Representation of an Archival Unit configuration.
 */
public class AuConfig {
  /**
   * The Archival Unit identifier.
   */
  private String auid = null;

  /**
   * The Archival Unit configuration.
   */
  private Map<String, String> configuration = null;

  /**
   * Constructor.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   * @param configuration
   *          A Map<String, String> with the Archival Unit configuration.
   */
  public AuConfig(String auid, Map<String, String> configuration) {
    super();
    this.auid = auid;
    this.configuration = configuration;
  }

  /**
   * Provides the Archival Unit identifier.
   * 
   * @return a String with the Archival Unit identifier.
   */
  public String getAuid() {
    return auid;
  }

  /**
   * Populates the Archival Unit identifier.
   * 
   * @param auid
   *          A String with the Archival Unit identifier.
   */
  public void setAuid(String auid) {
    this.auid = auid;
  }

  /**
   * Provides the Archival Unit configuration.
   * 
   * @return a Map<String, String> with the Archival Unit configuration.
   */
  public Map<String, String> getConfiguration() {
    return configuration;
  }

  /**
   * Populates the Archival Unit configuration.
   * 
   * @param auid
   *          A Map<String, String> with the Archival Unit configuration.
   */
  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuConfig other = (AuConfig) o;
    return Objects.equals(this.auid, other.auid) &&
        Objects.equals(this.configuration, other.configuration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(auid, configuration);
  }

  @Override
  public String toString() {
    return "[AuConfig auid=" + auid + ", configuration=" + configuration + "]";
  }
}