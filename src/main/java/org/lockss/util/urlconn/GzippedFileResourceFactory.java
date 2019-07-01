/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

// Some portions of this code are:
/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */
package org.lockss.util.urlconn;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.security.*;
import org.apache.commons.io.*;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.client.cache.*;
import org.lockss.log.*;

/**
 * Light modification of apache.http.impl.client.cache.FileResourceFactory
 * to compress files, as we have large (~200MB) highly compressible tdb
 * files, multiple copies of which tend to exist in the cache (presumably
 * because I haven't figured out how to make it promptly trim old files).
 *
 * Generates {@link Resource} instances whose body is stored, compressed,
 * in a temporary file.
 *
 * @since 4.1
 */
@Contract(threading = ThreadingBehavior.IMMUTABLE)
public class GzippedFileResourceFactory implements ResourceFactory {
  private static L4JLogger log = L4JLogger.getLogger();

  private final File cacheDir;
  private final BasicIdGenerator idgen;

  public GzippedFileResourceFactory(final File cacheDir) {
    super();
    this.cacheDir = cacheDir;
    this.idgen = new BasicIdGenerator();
  }

  private File generateUniqueCacheFile(final String requestId) {
    final StringBuilder buffer = new StringBuilder();
    this.idgen.generate(buffer);
    buffer.append('.');
    final int len = Math.min(requestId.length(), 100);
    for (int i = 0; i < len; i++) {
      final char ch = requestId.charAt(i);
      if (Character.isLetterOrDigit(ch) || ch == '.') {
	buffer.append(ch);
      } else {
	buffer.append('-');
      }
    }
    buffer.append(".gz");
    return new File(this.cacheDir, buffer.toString());
  }

  @Override
  public Resource generate(final String requestId,
			   final InputStream instream,
			   final InputLimit limit) throws IOException {
    final File file = generateUniqueCacheFile(requestId);
    log.debug2("Writing: {}", file);
//     log.fatal("Writing {}", file, new Throwable());
    final OutputStream outstream =
      new GZIPOutputStream(new FileOutputStream(file));
    long total = 0;
    try {
      final byte[] buf = new byte[10*1024];
      int l;
      while ((l = instream.read(buf)) != -1) {
	outstream.write(buf, 0, l);
	total += l;
	if (limit != null && total > limit.getValue()) {
	  limit.reached();
	  break;
	}
      }
    } finally {
      outstream.close();
    }
    return new GzippedFileResource(file, total);
  }

  @Override
  public Resource copy(final String requestId,
		       final Resource resource) throws IOException {
    final File file = generateUniqueCacheFile(requestId);
//     log.fatal("Copying {} to {}", resource, file, new Throwable());

    if (resource instanceof GzippedFileResource) {
      final File src = ((GzippedFileResource) resource).getFile();
      FileUtils.copyFile(src, file);
    } else {
      try (final OutputStream out =
	   new BufferedOutputStream(new FileOutputStream(file))) {
	IOUtils.copy(resource.getInputStream(), out);
      }
    }
    return new GzippedFileResource(file, resource.length());
  }

  static class BasicIdGenerator {
    private final String hostname;
    private final SecureRandom rnd;
    private long count;

    BasicIdGenerator() {
      super();
      String hostname;
      try {
	hostname = InetAddress.getLocalHost().getHostName();
      } catch (final UnknownHostException ex) {
	hostname = "localhost";
      }
      this.hostname = hostname;
      try {
	this.rnd = SecureRandom.getInstance("SHA1PRNG");
      } catch (final NoSuchAlgorithmException ex) {
	throw new Error(ex);
      }
      this.rnd.setSeed(System.currentTimeMillis());
    }

    public synchronized void generate(final StringBuilder buffer) {
      this.count++;
      final int rndnum = this.rnd.nextInt();
      buffer.append(System.currentTimeMillis());
      buffer.append('.');
      final Formatter formatter = new Formatter(buffer, Locale.US);
      formatter.format("%1$016x-%2$08x", Long.valueOf(this.count), Integer.valueOf(rndnum));
      formatter.close();
      buffer.append('.');
      buffer.append(this.hostname);
    }

    public String generate() {
      final StringBuilder buffer = new StringBuilder();
      generate(buffer);
      return buffer.toString();
    }
  }
}
