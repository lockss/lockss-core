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

import java.io.IOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.ToString;
import lombok.EqualsAndHashCode;

import org.lockss.log.L4JLogger;
import org.lockss.util.*;
import org.lockss.util.rest.repo.model.*;
import java.util.regex.Pattern;

/**
 * Harness for injecting errors into repository operations, for
 * testing client error handling.
 *
 * TODO: currently there's no way to cause a repo method to return a
 * specific value.  (null, at least, would be useful, e.g. to cause a
 * 404 response from getArtifact().)  That would require a more
 * complex invocation mechanism in BaseLockssRepository.
 */
public class ErrorHarness {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** 
   * The operation for which a testing error injection rule is being applied.
   */
  enum TestingErrorOp {
    AddArtifact,
    CommitArtifact,
    GetArtifact
  }

  /** A condition and an action to take if the condition is true */
  @ToString
  @EqualsAndHashCode
  public static class ErrorInjectionRule {
    TestingCondition condition;
    TestingAction action;

    public ErrorInjectionRule(TestingCondition condition,
                              TestingAction action) {
      this.condition = condition;
      this.action = action;
    }

    /** If the condition matches, apply the rule and return true */
    boolean apply(ArtifactIdentifier artifactId, TestingErrorOp op)
        throws IOException {
      if (condition.matches(op, artifactId)) {
        action.apply(artifactId, op);
        return true;
      }
      return false;
    }
  }      

  /** Functional interface similar to BiConsumer but allowed to throw
   * IOException */
  @FunctionalInterface
  public interface CheckedBiConsumer<T,U> {
    void apply(T t, U u) throws IOException;
  }

  /** Condition in which an action should be taken, evaluated in the
   * context of a LockssRepository operation. */
  @ToString
  @EqualsAndHashCode
  public static class TestingCondition {
    private ArtifactIdentifierPattern aip;
    private List<TestingErrorOp> ops;
    private List<Integer> ords;

    private int counter = 0;

    public TestingCondition() {
    }

    public TestingCondition setArtifactIdentifierPattern(ArtifactIdentifierPattern aip) {
      this.aip = aip;
      return this;
    }

    public TestingCondition setTestingErrorOp(TestingErrorOp op) {
      this.ops = Collections.singletonList(op);
      return this;
    }

    public TestingCondition setOrdinals(List<Integer> ords) {
      this.ords = ords;
      return this;
    }

    public boolean matches(TestingErrorOp op, ArtifactIdentifier artifactId) {
      log.debug2("Testing {}", this);
      if (!matchesOp(op)) return false;
      if (!matchesAip(artifactId)) return false;
      if (!matchesOrd()) return false;
      log.debug2("Returning true");
      return true;
    }

    private boolean matchesAip(ArtifactIdentifier artifactId) {
      if (aip == null) {
        return true;
      }
      return aip.matches(artifactId);
    }

    private boolean matchesOp(TestingErrorOp op) {
      if (ops == null || ops.isEmpty()) {
        return true;
      }
      return ops.contains(op);
    }

    private boolean matchesOrd() {
      if (ords == null || ords.isEmpty()) {
        return true;
      }
      return ords.contains(++counter);
    }

  }      

  /** Action to be taken when a condition matches, applied in the
   * context of a LockssRepository operation */
  public interface TestingAction
    extends CheckedBiConsumer<ArtifactIdentifier,TestingErrorOp> {
    void apply(ArtifactIdentifier artifactId, TestingErrorOp op)
        throws IOException;
  }      

  @ToString
  @EqualsAndHashCode
  public static class ThrowAction implements TestingAction {
    private Class exClass;
    private String message;

    ThrowAction setExceptionClass(String clazz) {
      switch (clazz) {
      case "IOException": exClass = IOException.class;
        break;
      case "IllegalArgumentException": exClass = IllegalArgumentException.class;
        break;
      default:
        try {
          exClass = Class.forName(clazz);
          // Verify we can call its String constructor
          try {
            Class[] sig = { String.class };
            Object[] args = {message};
            Constructor cons = exClass.getConstructor(sig);
            if (IOException.class.isAssignableFrom(exClass)) {
              IOException ex = (IOException)cons.newInstance(args);
            } else if (RuntimeException.class.isAssignableFrom(exClass)) {
              RuntimeException rex = (RuntimeException)cons.newInstance(args);
            } else {
              throw new IllegalArgumentException("Exception must be either an IOException or RuntimeException: " + exClass.getName());
            }
          } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Specified exception class doesn't have an invocable String constructor: " + clazz);
          }
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Exception class not found: "
                                             + clazz);
        }
      }
      return this;
    }

    ThrowAction setMessage(String msg) {
      this.message = msg;
      return this;
    }

    public void apply(ArtifactIdentifier artifactId, TestingErrorOp op)
        throws IOException {
      log.debug2("Applying {}", this);
      IOException ex = null;
      RuntimeException rex = null;
      Class[] sig = { String.class };
      Object[] args = {message};
      try {
        Constructor cons = exClass.getConstructor(sig);
        if (IOException.class.isAssignableFrom(exClass)) {
          ex = (IOException)cons.newInstance(args);
        } else if (RuntimeException.class.isAssignableFrom(exClass)) {
          rex = (RuntimeException)cons.newInstance(args);
        } else {
          throw new IllegalArgumentException("Exception must be either an IOException or RuntimeException: " + exClass.getName());
        }
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        // We have already checked that exClass is an IOException
        // with a String constructor
        throw new IllegalArgumentException("Shouldn't happen", e);
      }
      if (ex != null) {
        throw ex;
      } else {
        throw rex;
      }
    }
  }

  /** Pattern to match against an ArtifactIdentifier.  Regexps for the
   * string fields, Integer for the version.  Any field not filled in
   * matches anything. */
  @ToString
  @EqualsAndHashCode
  public static class ArtifactIdentifierPattern {
    private String uuid;
    private String namespace;
    private String auid;
    private String uri;
    @EqualsAndHashCode.Exclude
    private Pattern uuidPat;
    @EqualsAndHashCode.Exclude
    private Pattern namespacePat;
    @EqualsAndHashCode.Exclude
    private Pattern auidPat;
    @EqualsAndHashCode.Exclude
    private Pattern uriPat;
    private Integer version;

    public ArtifactIdentifierPattern() {
    }

    public boolean isEmpty() {
      return uuidPat == null
        && namespacePat == null
        && auidPat == null
        && uriPat == null
        && version == null;
    }

    public ArtifactIdentifierPattern setUuidPattern(String uuidPattern) {
      this.uuid = uuidPattern;
      this.uuidPat = Pattern.compile(uuidPattern);
      return this;
    }

    public ArtifactIdentifierPattern setNamespacePattern(String namespacePattern) {
      this.namespace = namespacePattern;
      this.namespacePat = Pattern.compile(namespacePattern);
      return this;
    }

    public ArtifactIdentifierPattern setAuidPattern(String auidPattern) {
      this.auid = auidPattern;
      this.auidPat = Pattern.compile(auidPattern);
      return this;
    }

    public ArtifactIdentifierPattern setUriPattern(String uriPattern) {
      this.uri = uriPattern;
      this.uriPat = Pattern.compile(uriPattern);
      return this;
    }

    public ArtifactIdentifierPattern setVersion(Integer version) {
      this.version = version;
      return this;
    }

    public boolean matches(ArtifactIdentifier artifactId) {
      if (uuidPat != null && !uuidPat.matcher(artifactId.getUuid()).matches()) {
        return false;
      }
      if (namespacePat != null && !namespacePat.matcher(artifactId.getNamespace()).matches()) {
        return false;
      }
      if (auidPat != null && !auidPat.matcher(artifactId.getAuid()).matches()) {
        return false;
      }
      if (uriPat != null && !uriPat.matcher(artifactId.getUri()).matches()) {
        return false;
      }
      if (version != null) {
        log.fatal("Version: {}, artver: {}", version, artifactId.getVersion());
      }
      if (version != null && artifactId.getVersion() != version) {
        return false;
      }
      return true;
    }
  }

  // Error injection rule specification is a Json structure (matching
  // the POJOs below) consisting of a condition and an action.  All
  // parts of the condition are optional, but at least one must be
  // included.

  /*
    {"cond": { "op" : "AddArtifact",    // TestingErrorOp
               "uri": "foo.*",          // uri regexp
               "auid": "auid",          // auid regexp
               "uuid": "...",           // uuid regexp
               "version": "2",          // Artifact version
               "ords": "1,2"            // (List of) ordinals, matches the Nth
                                        // invocation that matches the other
                                        // criteria
             },
     "action": {"ex":"ExceptionClass",  // Exception class (fqdn or known abbrev)
                "msg":"a message"       // Exception message
                }
    }
  */

  // POJOs into which to deserialize JSON rule specs

  public static class RuleSpec {
    public CondSpec cond;
    public ActionSpec action;
  }
  public static class CondSpec {
    public String op;
    public String uri;
    public String auid;
    public String uuid;
    public String ords;
    public Integer version;
  }
  static class ActionSpec {
    public String ex;
    public String msg;
  }

  /** Read a semicolon-separated list of rule specs */
  public static List<ErrorInjectionRule> fromSpecs(String specs)
      throws IllegalArgumentException {
    List<ErrorInjectionRule> res = new ArrayList<>();
    for (String s : StringUtil.breakAt(specs, ";")) {
      ErrorInjectionRule rule = fromOneSpec(s);
      if (rule != null) {
        res.add(fromOneSpec(s));
      }
    }
    return res;
  }

  /** Read a single json rule spec */
  public static ErrorInjectionRule fromOneSpec(String spec)
      throws IllegalArgumentException {
    
    if (StringUtil.isNullString(spec)) {
      return null;
    }
    try {
      RuleSpec rs = new ObjectMapper().readValue(spec, RuleSpec.class);
      TestingCondition cond = new TestingCondition();
      ArtifactIdentifierPattern aip = new ArtifactIdentifierPattern();

      CondSpec cs = rs.cond;
      if (cs == null) {
        throw new IllegalArgumentException("Condition must be specified");
      }
      ActionSpec as = rs.action;
      if (as == null) {
        throw new IllegalArgumentException("Action must be specified");
      }
      if (!StringUtil.isNullString(cs.op)) {
        cond.setTestingErrorOp(TestingErrorOp.valueOf(cs.op));
      }
      if (!StringUtil.isNullString(cs.uri)) {
        aip.setUriPattern(cs.uri);
      }
      if (!StringUtil.isNullString(cs.auid)) {
        aip.setAuidPattern(cs.auid);
      }
      if (!StringUtil.isNullString(cs.uuid)) {
        aip.setUuidPattern(cs.uuid);
      }
      if (cs.version != null) {
        aip.setVersion(cs.version);
      }
      if (!aip.isEmpty()) {
        cond.setArtifactIdentifierPattern(aip);
      }
      if (!StringUtil.isNullString(cs.ords)) {
        cond.setOrdinals(StringUtil.breakAt(cs.ords, ",").stream()
                         .map(Integer::valueOf)
                         .collect(Collectors.toList()));
      }
      TestingAction act = null;
      if (!StringUtil.isNullString(as.ex)) {
        ThrowAction ta = new ThrowAction();
        ta.setExceptionClass(as.ex);
        ta.setMessage(as.msg);
        act = ta;
      }

      if (act == null) {
        throw new IllegalArgumentException("Action must be specified");
      }
      return new ErrorInjectionRule(cond, act);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Couldn't parse error injection rules", e);
    }

  }
}
