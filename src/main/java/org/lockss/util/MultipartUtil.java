package org.lockss.util;

import java.io.*;

import org.apache.commons.collections4.map.*;
import org.apache.http.*;
import org.apache.http.impl.io.*;

public class MultipartUtil {
  
  public static MultipartResponse parseMultipartResponse(InputStream inputStream)
      throws HttpException, IOException {
    SessionInputBufferImpl sessionInputBuffer = new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 8192);
    sessionInputBuffer.bind(inputStream);
    DefaultHttpResponseParser httpMessageParser = new DefaultHttpResponseParser(sessionInputBuffer);
    HttpResponse httpResponse = httpMessageParser.parse();
    Header[] responseHeaders = httpResponse.getAllHeaders();
    return null;
  }
  
}
