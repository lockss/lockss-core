/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

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

package org.lockss.util;

import java.io.IOException;
import java.util.*;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;

import org.springframework.http.*;
import org.springframework.http.converter.*;

public class MimeMultipartHttpMessageConverter implements HttpMessageConverter<MimeMultipart> {

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
