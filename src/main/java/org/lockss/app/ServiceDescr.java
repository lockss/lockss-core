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

import java.util.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * LOCKSS REST service descriptor
 */
public class ServiceDescr {
  static Map<String,ServiceDescr> abbrevMap = new HashMap<>();

  private final String name;
  private final String abbrev;

  public static final ServiceDescr SVC_CONFIG =
    new ServiceDescr("Config Service", "cfg");
  public static final ServiceDescr SVC_MDX =
    new ServiceDescr("Metadata Extraction Service", "mdx");
  public static final ServiceDescr SVC_MDQ =
    new ServiceDescr("Metadata Query Service", "mdq");
  public static final ServiceDescr SVC_POLLER =
    new ServiceDescr("Poller Service", "poller");
  public static final ServiceDescr SVC_CRAWLER =
    new ServiceDescr("Crawler Service", "crawler");
  public static final ServiceDescr SVC_REPO =
    new ServiceDescr("Repository Service", "repo");

  public ServiceDescr(String name, String abbrev) {
    this.name = name;
    this.abbrev = abbrev;
    abbrevMap.put(abbrev, this);
  }

  public String getName() {
    return name;
  }

  public String getAbbrev() {
    return abbrev;
  }

  public static ServiceDescr fromAbbrev(String abbrev) {
    return abbrevMap.get(abbrev);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ServiceDescr) {
      ServiceDescr sd = (ServiceDescr)obj;
      return ObjectUtils.equals(name, sd.getName())
	&& ObjectUtils.equals(abbrev, sd.getAbbrev());
    }
    return false;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder();
    hcb.append(name);
    hcb.append(abbrev);
    return hcb.toHashCode();
  }

  @Override
  public String toString() {
    return "[SD: " + name + ", " + abbrev + "]";
  }

}
