package org.lockss.protocol.handlers.locksscu;

import org.lockss.daemon.CuUrl;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 */
public class Handler extends URLStreamHandler {

  // NOTE: in order to be found as a URL protocol handler, this class must be public,
  // must be named Handler and must be in a package ending '.jar'

  @Override
  protected URLConnection openConnection(URL url) throws IOException {
    return new CuUrl.CuUrlConnection(url);
  }
}
