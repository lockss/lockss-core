/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

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
import java.io.InputStream;
import java.io.Writer;
import java.util.Map;

/**
 * Utility for interpolating templates.
 */
public class TemplateUtil {
  private static final Logger log = Logger.getLogger(TemplateUtil.class);

  /**
   * Writes the contents of a template resource interpolated with the passed
   * values.
   * 
   * @param resourceName
   *          A String with the name of the template resource.
   * @param wrtr
   *          A Writer used to write the output.
   * @param valMap
   *          A Map<String,String> with the values to be interpolated.
   * @throws IOException
   *           if there are problems reading the template or writing the output.
   */
  public static void expandTemplate(String resourceName, Writer wrtr,
      Map<String,String> valMap) throws IOException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    InputStream is = cl.getResourceAsStream(resourceName);
    if (is == null) {
      throw new IllegalArgumentException("No such template file: " +
					 resourceName);
    }
    expandTemplate(is, wrtr, valMap);
  }

  /**
   * Writes the contents of an input stream-provided template interpolated with
   * the passed values.
   * 
   * @param templateStream
   *          An InputStream providing the template contents.
   * @param wrtr
   *          A Writer used to write the output.
   * @param valMap
   *          A Map<String,String> with the values to be interpolated.
   * @throws IOException
   *           if there are problems reading the template or writing the output.
   */
  public static void expandTemplate(InputStream templateStream, Writer wrtr,
      Map<String,String> valMap) throws IOException {
    if (templateStream == null) {
      throw new IllegalArgumentException("Template input stream is null");
    }
    try {
      String template = StringUtil.fromInputStream(templateStream);
      SimpleWriterTemplateExpander t =
	new SimpleWriterTemplateExpander(template, wrtr);
      String token;
      while ((token = t.nextToken()) != null) {
	String val = valMap.get(token);
	if (val != null) {
	  wrtr.write(val);
	} else {
	  log.warning("Unknown token '" + token + "' in template");
	}
      }
      wrtr.flush();
    } finally {
      IOUtil.safeClose(templateStream);
    }
  }
}
