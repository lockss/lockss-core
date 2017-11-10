package org.lockss.util;

import java.io.*;

import javax.activation.DataSource;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

public class MultipartUtil {

  public static final String MULTIPART_FORM_DATA = "multipart/form-data";
  
  public static MimeMultipart parse(InputStream inputStream)
      throws IOException, MessagingException {
    return parse(inputStream, MULTIPART_FORM_DATA);
  }
  
  public static MimeMultipart parse(InputStream inputStream,
                                    String mimeType)
      throws IOException, MessagingException {
    DataSource dataSource = new ByteArrayDataSource(inputStream, mimeType);
    return new MimeMultipart(dataSource);
  }
  
}
