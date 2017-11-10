package org.lockss.util;

import java.io.IOException;
import java.util.*;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;

import org.springframework.http.*;
import org.springframework.http.converter.*;

public class MimeMultipartMessageConverter implements HttpMessageConverter<MimeMultipart> {

  public static final List<MediaType> SUPPORTED_MEDIA_TYPES =
      Collections.unmodifiableList(Arrays.asList(MediaType.MULTIPART_FORM_DATA));
  
  @Override
  public boolean canRead(Class<?> clazz, MediaType mediaType) {
    return clazz.isAssignableFrom(MimeMultipart.class);
  }

  @Override
  public boolean canWrite(Class<?> clazz, MediaType mediaType) {
    return clazz.isAssignableFrom(MimeMultipart.class);
  }

  @Override
  public List<MediaType> getSupportedMediaTypes() {
    return SUPPORTED_MEDIA_TYPES;
  }

  @Override
  public MimeMultipart read(Class<? extends MimeMultipart> clazz, HttpInputMessage inputMessage)
      throws IOException, HttpMessageNotReadableException {
    try {
      return MultipartUtil.parse(inputMessage.getBody());
    }
    catch (MessagingException me) {
      throw new HttpMessageNotReadableException("Error reading HTTP message", me);
    }
  }

  @Override
  public void write(MimeMultipart mimeMultipart, MediaType contentType, HttpOutputMessage outputMessage)
      throws IOException, HttpMessageNotWritableException {
    try {
      mimeMultipart.writeTo(outputMessage.getBody());
    }
    catch (MessagingException me) {
      throw new HttpMessageNotWritableException("Error writing HTTP message", me);
    }
  }

}
