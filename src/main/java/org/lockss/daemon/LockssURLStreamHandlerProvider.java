package org.lockss.daemon;

import org.lockss.util.ResourceURLConnection;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;

import static org.lockss.daemon.UrlManager.PROTOCOL_AU;
import static org.lockss.daemon.UrlManager.PROTOCOL_CU;
import static org.lockss.daemon.UrlManager.PROTOCOL_RESOURCE;

public class LockssURLStreamHandlerProvider extends URLStreamHandlerProvider {
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
