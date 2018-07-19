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

import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import org.lockss.test.LockssTestCase;

/**
 * Test class for org.lockss.util.TemplateUtil
 */
public class TestTemplateUtil extends LockssTestCase {

  private static final String NO_TOKENS_TEMPLATE_NAME =
      "org/lockss/util/testTemplateNoTokens.txt";
  private static final String ONE_TOKEN_TEMPLATE_NAME =
      "org/lockss/util/testTemplateOneToken.txt";
  private static final String TWO_TOKENS_TEMPLATE_NAME =
      "org/lockss/util/testTemplateTwoTokens.txt";

  /**
   * Tests bad arguments.
   * @throws Exception
   */
  public void testExpandTemplateBadArguments() throws Exception {
    try {
      TemplateUtil.expandTemplate(null, null, null);
      fail("null template name should throw");
    } catch (NullPointerException npe) {
    }

    try {
      TemplateUtil.expandTemplate("unknown_template.txt", null, null);
      fail("unknown template name should throw");
    } catch (IllegalArgumentException e) {
    }

    try {
      TemplateUtil.expandTemplate(NO_TOKENS_TEMPLATE_NAME, null, null);
      fail("null writer should throw");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests a template with no interpolation tokens.
   * @throws Exception
   */
  public void testExpandTemplateNoTokens() throws Exception {
    // The result is always the same.
    String noInterpolation = "There are @no tokens@ here.";

    runTestExpandTemplate(NO_TOKENS_TEMPLATE_NAME, null, noInterpolation);

    runTestExpandTemplate(NO_TOKENS_TEMPLATE_NAME,
	new HashMap<String, String>(), noInterpolation);

    Map<String,String> valMap = MapUtil.map("SomeKey", "SomeValue",
	"no", "interpolatedNo",
	"tokens", "interpolatedTokens");

    runTestExpandTemplate(NO_TOKENS_TEMPLATE_NAME, valMap, noInterpolation);
  }

  /**
   * Tests a template with one interpolation token.
   * @throws Exception
   */
  public void testExpandTemplateOneToken() throws Exception {
    try {
      TemplateUtil.expandTemplate(ONE_TOKEN_TEMPLATE_NAME, new StringWriter(),
	  null);
      fail("null map should throw");
    } catch (NullPointerException npe) {
    }

    // Result when no tokens are interpolated.
    String noInterpolation = "There is a @token here: ''.";

    runTestExpandTemplate(ONE_TOKEN_TEMPLATE_NAME,
	new HashMap<String, String>(), noInterpolation);

    Map<String,String> valMap = MapUtil.map("SomeKey", "SomeValue");

    runTestExpandTemplate(ONE_TOKEN_TEMPLATE_NAME, valMap, noInterpolation);

    // Result when the token is interpolated.
    String interpolation = "There is a @token here: 'token value'.";

    valMap = MapUtil.map("token", "token value");

    runTestExpandTemplate(ONE_TOKEN_TEMPLATE_NAME, valMap, interpolation);

    valMap = MapUtil.map("SomeKey", "SomeValue",
	"token", "token value");

    runTestExpandTemplate(ONE_TOKEN_TEMPLATE_NAME, valMap, interpolation);
  }

  /**
   * Tests a template with two interpolation tokens.
   * @throws Exception
   */
  public void testExpandTemplateTwoTokens() throws Exception {
    // Result when no tokens are interpolated.
    String noInterpolation =
	"There is a @token here: ''.\nAnd@ another here: ''.";

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME,
	new HashMap<String, String>(), noInterpolation);

    Map<String,String> valMap = MapUtil.map("SomeKey", "SomeValue",
	"token", "interpolatedToken",
	"and", "interpolatedAnd");

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME, valMap, noInterpolation);

    // Result when the first token is interpolated.
    String firstInterpolation =
	"There is a @token here: 'token1 value'.\nAnd@ another here: ''.";

    valMap = MapUtil.map("token1", "token1 value");

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME, valMap, firstInterpolation);

    valMap = MapUtil.map("SomeKey", "SomeValue",
	"token", "interpolatedToken",
	"and", "interpolatedAnd",
	"token1", "token1 value");

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME, valMap, firstInterpolation);

    // Result when the second token is interpolated.
    String secondInterpolation =
	"There is a @token here: ''.\nAnd@ another here: 'token2 value'.";

    valMap = MapUtil.map("token2", "token2 value");

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME, valMap,
	secondInterpolation);

    // Result when both tokens are interpolated.
    String bothInterpolations = "There is a @token here: 'token1 value'.\n"
	+ "And@ another here: 'token2 value'.";

    valMap = MapUtil.map("token1", "token1 value", "token2", "token2 value");

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME, valMap, bothInterpolations);

    valMap = MapUtil.map("SomeKey", "SomeValue",
	"token", "interpolatedToken",
	"and", "interpolatedAnd",
	"token1", "token1 value",
	"token2", "token2 value");

    runTestExpandTemplate(TWO_TOKENS_TEMPLATE_NAME, valMap, bothInterpolations);
  }

  /**
   * Runs a template interpolation.
   * 
   * @param templateFileName
   *          A String with the name of the template file.
   * @param interpolations
   *          A Map<String, String> with the interpolations to be performed.
   * @param expectedResult
   *          A String with the expected result of the interpolation.
   * @throws Exception
   */
  private void runTestExpandTemplate(String templateFileName,
      Map<String, String> interpolations, String expectedResult)
	  throws Exception {
    Writer writer = new StringWriter();
    TemplateUtil.expandTemplate(templateFileName, writer, interpolations);
    assertEquals(expectedResult, writer.toString());
  }
}
