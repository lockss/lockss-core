/*
 * $Id$
 *

Copyright (c) 2000-2003 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.util;

import java.util.*;
import java.io.*;
import java.net.*;
import java.security.*;
import org.apache.http.*;
import org.apache.http.message.*;
import org.springframework.http.HttpHeaders;
import org.springframework.util.StringUtils;

import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;

import org.lockss.util.*;
import org.lockss.util.StreamUtil.IgnoreCloseInputStream;
import org.lockss.plugin.*;


/** Utilities for V2 repository
 */
public class V2RepoUtil {
  private static final Logger log = Logger.getLogger("V2RepoUtil");

  /** Build a Spring HttpHeaders from CIProperties */
  public static HttpHeaders httpHeadersFromProps(CIProperties props) {
    HttpHeaders res = new HttpHeaders();
    for (String key : (Set<String>) (((Map)props).keySet())) {
      res.set(key, props.getProperty(key));
    }
    return res;
  }

  /** Build a CIProperties from a Spring HttpHeaders */
  // TK should concatenate multi-value keys
  public static CIProperties propsFromHttpHeaders(HttpHeaders hdrs) {
    CIProperties res = new CIProperties();
    for (String key : hdrs.keySet()) {
      res.setProperty(key, StringUtil.separatedString(hdrs.get(key), ","));
    }
    return res;
  }

  public static Artifact storeArt(LockssRepository repo, String coll,
				  String auid, String url, InputStream in,
				  CIProperties props) throws IOException {
    ArtifactIdentifier id = new ArtifactIdentifier(coll, auid,
						   url, null);
    CIProperties propsCopy  = CIProperties.fromProperties(props);
    propsCopy.setProperty(CachedUrl.PROPERTY_NODE_URL, url);
    HttpHeaders metadata = httpHeadersFromProps(propsCopy);

    // tk
    BasicStatusLine statusLine =
      new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    ArtifactData ad =
      new ArtifactData(id, metadata,
		       new IgnoreCloseInputStream(in),
		       statusLine);
    if (log.isDebug2()) {
      log.debug2("Creating artifact: " + ad);
    }
    Artifact uncommittedArt = repo.addArtifact(ad);
    return repo.commitArtifact(uncommittedArt);
  }


}
