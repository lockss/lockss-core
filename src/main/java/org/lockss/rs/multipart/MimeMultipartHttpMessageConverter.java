/*

 Copyright (c) 2017 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.rs.multipart;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import org.springframework.http.MediaType;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

public class MimeMultipartHttpMessageConverter
    implements HttpMessageConverter<MimeMultipart> {

  public static final List<MediaType> SUPPORTED_MEDIA_TYPES = Collections
      .unmodifiableList(Arrays.asList(MediaType.MULTIPART_FORM_DATA));

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
  public MimeMultipart read(Class<? extends MimeMultipart> clazz,
      HttpInputMessage inputMessage)
      throws IOException, HttpMessageNotReadableException {
    try {
      return MultipartUtil.parse(inputMessage.getBody());
    } catch (MessagingException me) {
      throw new HttpMessageNotReadableException("Error reading HTTP message",
	  me);
    }
  }

  @Override
  public void write(MimeMultipart mimeMultipart, MediaType contentType,
      HttpOutputMessage outputMessage)
      throws IOException, HttpMessageNotWritableException {
    try {
      mimeMultipart.writeTo(outputMessage.getBody());
    } catch (MessagingException me) {
      throw new HttpMessageNotWritableException("Error writing HTTP message",
	  me);
    }
  }
}
