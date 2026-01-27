/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University

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

package org.lockss.rs;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

import org.lockss.log.L4JLogger;
import org.lockss.test.LockssTestCase4;
import org.lockss.util.*;
import org.lockss.util.rest.repo.model.*;

import static org.lockss.rs.ErrorHarness.*;

/**
 * Test class for {@link ErrorHarness}
 */
public class TestErrorHarness extends LockssTestCase4 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private ArtifactIdentifier ai1 =
    new ArtifactIdentifier("uuid1", "ns1", "auid1", "http://foo/bar", 1);
  private ArtifactIdentifier ai2 =
    new ArtifactIdentifier("uuid1", "ns1", "auid1", "http://bar/foo", 1);
  private ArtifactIdentifier ai3 =
    new ArtifactIdentifier("uuid1", "ns1", "auid1", "http://bar/foo", 2);

  @Test
  public void testMatchArtId() {
    ArtifactIdentifierPattern aip1 = new ArtifactIdentifierPattern()
      .setUriPattern("http://foo/.*");
    assertTrue(aip1.matches(ai1));
    assertFalse(aip1.matches(ai2));
    ArtifactIdentifierPattern aip2 = new ArtifactIdentifierPattern()
      .setUriPattern("http://bar/.*")
      .setVersion(2);
    assertFalse(aip2.matches(ai1));
    assertFalse(aip2.matches(ai2));
    assertTrue(aip2.matches(ai3));
    assertTrue(new ArtifactIdentifierPattern().setNamespacePattern("ns1")
               .matches(ai1));
    assertFalse(new ArtifactIdentifierPattern().setNamespacePattern("ns2")
               .matches(ai1));
    assertTrue(new ArtifactIdentifierPattern().setAuidPattern("auid1")
               .matches(ai1));
    assertFalse(new ArtifactIdentifierPattern().setAuidPattern("auid2")
               .matches(ai1));
    assertTrue(new ArtifactIdentifierPattern().setUuidPattern("uuid1")
               .matches(ai1));
    assertFalse(new ArtifactIdentifierPattern().setUuidPattern("uuid2")
               .matches(ai1));
  }

  @Test
  public void testOrd() {
    TestingCondition cond =
      new TestingCondition()
      .setOrdinals(ListUtil.list(2,4))
      .setTestingErrorOp(TestingErrorOp.AddArtifact);
    // 1
    assertFalse(cond.matches(TestingErrorOp.AddArtifact, ai1));
    // 2
    assertTrue(cond.matches(TestingErrorOp.AddArtifact, ai1));
    // 3, wrong op should not increment counter
    assertFalse(cond.matches(TestingErrorOp.CommitArtifact, ai1));
    // still 3
    assertFalse(cond.matches(TestingErrorOp.AddArtifact, ai1));
    // 4
    assertTrue(cond.matches(TestingErrorOp.AddArtifact, ai1));
    // 5
    assertFalse(cond.matches(TestingErrorOp.AddArtifact, ai1));
  }

  @Test
  public void testThrowsAction() throws IOException {
    ArtifactIdentifierPattern aip = new ArtifactIdentifierPattern()
      .setUriPattern("http://foo/.*");
    TestingCondition cond =
      new TestingCondition()
      .setArtifactIdentifierPattern(aip)
      .setTestingErrorOp(TestingErrorOp.AddArtifact);
    ErrorInjectionRule eir =
      new ErrorInjectionRule(cond, (x,y) -> {throw new IOException();});
    eir.apply(ai2, null);
    assertThrows(IOException.class,
                 () -> {eir.apply(ai1, TestingErrorOp.AddArtifact);});
    ErrorInjectionRule eir2 =
      new ErrorInjectionRule(cond, (x,y) -> {throw new IllegalArgumentException();});
    eir2.apply(ai2, null);
    assertThrows(IllegalArgumentException.class,
                 () -> {eir2.apply(ai1, TestingErrorOp.AddArtifact);});
    
  }

  @Test
  public void testSpecIllAction() throws IOException {
    assertThrowsMatch(IllegalArgumentException.class,
                      "Exception class not found: NoClass",
                      () -> {ErrorHarness.fromOneSpec("""
                                                      {"cond": { "op" : "AddArtifact",
                                                            "uri":"foo.*",
                                                            "version":"2",
                                                            "ords":"1,2"
                                                            },
                                                          "action":{"ex":"NoClass",
                                                            "msg":"a message"}
                                                      }
                                                      """);});
    assertThrowsMatch(IllegalArgumentException.class,
                      "Exception must be either an IOException or RuntimeException: java.lang.Throwable",
                      () -> {ErrorHarness.fromOneSpec("""
                                                      {"cond": {"uri":"foo.*"
                                                            },
                                                          "action":{"ex":"java.lang.Throwable",
                                                            "msg":"a message"}
                                                      }
                                                      """);});
  }

  @Test
  public void testFromOneSpec() throws IOException {
    assertNull(ErrorHarness.fromOneSpec(""));
    ErrorInjectionRule rule =
      ErrorHarness.fromOneSpec("""
                               {"cond": { "op" : "AddArtifact",
                                     "uri":"foo.*",
                                     "version":"2",
                                     "ords":"1,2"
                                     },
                                   "action":{"ex":"IOException",
                                     "msg":"a message"}
                               }
                               """);
    log.debug("rule: {}", rule);
  }

  @Test
  public void testFromSpecs() throws IOException {
    List<ErrorInjectionRule> rules =
      ErrorHarness.fromSpecs("""
                             {"cond": { "op" : "AddArtifact", "uri":"foo.*", "ords":"4,5"},
                                 "action":{"ex":"IOException", "msg":"mmm"}
                             };
                             {"cond": { "op" : "CommitArtifact", "uri":"bar.*"},
                                 "action":{"ex":"java.io.EOFException", "msg":"ddd"}
                             }
                             """);
    log.debug("rules: {}", rules);

    ArtifactIdentifierPattern aip1 = new ArtifactIdentifierPattern()
      .setUriPattern("foo.*");
    TestingCondition c1 = new TestingCondition()
      .setArtifactIdentifierPattern(aip1)
      .setTestingErrorOp(TestingErrorOp.AddArtifact)
      .setOrdinals(ListUtil.list(4,5));
    TestingAction act1 = new ThrowAction()
      .setExceptionClass("IOException")
      .setMessage("mmm");
    ErrorInjectionRule r1 = new ErrorInjectionRule(c1, act1);
                       
    ArtifactIdentifierPattern aip2 = new ArtifactIdentifierPattern()
      .setUriPattern("bar.*");
    TestingCondition c2 = new TestingCondition()
      .setArtifactIdentifierPattern(aip2)
      .setTestingErrorOp(TestingErrorOp.CommitArtifact);
    TestingAction act2 = new ThrowAction()
      .setExceptionClass("java.io.EOFException")
      .setMessage("ddd");
    ErrorInjectionRule r2 = new ErrorInjectionRule(c2, act2);
                       
    assertEquals(ListUtil.list(r1, r2), rules);
  }

}
