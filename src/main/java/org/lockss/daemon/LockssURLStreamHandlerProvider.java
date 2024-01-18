/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.daemon;

import org.lockss.plugin.AuUrl;
import org.lockss.util.ResourceURLConnection;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;

public class LockssURLStreamHandlerProvider extends URLStreamHandlerProvider {
  private static final String PROTOCOL_CU = CuUrl.PROTOCOL;
  private static final String PROTOCOL_AU = AuUrl.PROTOCOL;
  private static final String PROTOCOL_RESOURCE = "resource";

  @Override
  public URLStreamHandler createURLStreamHandler(String protocol) {
    if (PROTOCOL_CU.equalsIgnoreCase(protocol)) {
      // locksscu: gets a CuUrlConnection
      return new URLStreamHandler() {
        protected URLConnection openConnection(URL u) throws IOException {
          // passing pluginManager runs into problems with class loaders
          // when running unit tests
// 	      return new CuUrl.CuUrlConnection(u, pluginManager);
          return new CuUrl.CuUrlConnection(u);
        }};
    }
    if (PROTOCOL_AU.equalsIgnoreCase(protocol)) {
      // AuUrls are never opened.
      return new URLStreamHandler() {
        protected URLConnection openConnection(URL u) throws IOException {
          return null;
        }};
    }
    if (PROTOCOL_RESOURCE.equalsIgnoreCase(protocol)) {
      return new URLStreamHandler() {
        protected URLConnection openConnection(URL u) throws IOException {
          return new ResourceURLConnection(u);
        }};
    }
    return null;	 // use default stream handlers for other protocols
  }
}
