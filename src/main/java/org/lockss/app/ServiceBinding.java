/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.app;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Info about binding of listen sockets of known LOCKSS services
 */
public class ServiceBinding {
  private final String restHost;
  private final String uiHost;
  private final int restPort;
  private final int uiPort;

  public ServiceBinding(String host, int restPort, int uiPort) {
    this.restHost = host;
    this.restPort = restPort;
    this.uiHost = host;
    this.uiPort = uiPort;
  }

  public ServiceBinding(String restHost, int restPort,
			String uiHost, int uiPort) {
    this.restHost = restHost;
    this.restPort = restPort;
    this.uiHost = uiHost;
    this.uiPort = uiPort;
  }

  public String getRestHost() {
    return restHost;
  }

  public int getRestPort() {
    return restPort;
  }

  public boolean hasRestPort() {
    return restPort != 0;
  }

  public String getUiHost() {
    return uiHost;
  }

  public int getUiPort() {
    return uiPort;
  }

  public boolean hasUiPort() {
    return uiPort != 0;
  }

  /** Return the URL stem to use to reach the REST port the service with
   * this binding.
   */
  public String getRestStem() {
    StringBuilder sb = new StringBuilder();
    sb.append("http");
    sb.append("://");
    sb.append(getRestHost() != null ? getRestHost() : "localhost");
    sb.append(':');
    sb.append(getRestPort());
    return sb.toString();
  }

  /** Return the URL stem to use to reach the UI of the service with this
   * binding.
   * @param proto must be supplied as not currently part of binding
   */
  public String getUiStem(String proto) {
    StringBuilder sb = new StringBuilder();
    sb.append(proto);
    sb.append("://");
    sb.append(getUiHost() != null ? getUiHost() : "localhost");
    sb.append(':');
    sb.append(getUiPort());
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ServiceBinding) {
      ServiceBinding sb = (ServiceBinding)obj;
      return
	ObjectUtils.equals(restHost, sb.getRestHost())
	&& ObjectUtils.equals(uiHost, sb.getUiHost())
	&& ObjectUtils.equals(restPort, sb.getRestPort())
	&& ObjectUtils.equals(uiPort, sb.getUiPort()) ;
    }
    return false;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(restHost);
    hcb.append(restPort);
    hcb.append(uiHost);
    hcb.append(uiPort);
    return hcb.toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[SB: ");
    if (restHost != null) {
      sb.append(restHost);
    }
    sb.append(":");
    sb.append(restPort);
    sb.append(",");
    if (uiHost != null) {
      sb.append(uiHost);
    }
    sb.append(":");
    sb.append(uiPort);
    sb.append("]");
    return sb.toString();
  }

}
