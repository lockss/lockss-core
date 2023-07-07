package org.lockss.rs.util;

import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.*;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.*;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.rs.*;

/**
 * Tests for {@link LockssRepositoryUtil}.
 */
public class TestLockssRepositoryUtil extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Test for {@link LockssRepositoryUtil#validateNamespace(String)}.
   */
  @Test
  public void testValidateNamespace() throws Exception {
    assertFalse(LockssRepositoryUtil.validateNamespace(null));
    assertFalse(LockssRepositoryUtil.validateNamespace(""));
    assertFalse(LockssRepositoryUtil.validateNamespace("."));
    assertFalse(LockssRepositoryUtil.validateNamespace(".foo"));
    assertFalse(LockssRepositoryUtil.validateNamespace(".."));
    assertFalse(LockssRepositoryUtil.validateNamespace("../foo"));
    assertFalse(LockssRepositoryUtil.validateNamespace("foo bar"));
    assertFalse(LockssRepositoryUtil.validateNamespace("-foo"));
    assertFalse(LockssRepositoryUtil.validateNamespace("--foo"));
    assertFalse(LockssRepositoryUtil.validateNamespace("\uD83D\uDCA9"));

    assertTrue(LockssRepositoryUtil.validateNamespace("f"));
    assertTrue(LockssRepositoryUtil.validateNamespace("f."));
    assertTrue(LockssRepositoryUtil.validateNamespace("f-"));
    assertTrue(LockssRepositoryUtil.validateNamespace("foo"));
    assertTrue(LockssRepositoryUtil.validateNamespace("FOO"));
    assertTrue(LockssRepositoryUtil.validateNamespace("f00"));
    assertTrue(LockssRepositoryUtil.validateNamespace("foo.2022"));
    assertTrue(LockssRepositoryUtil.validateNamespace("foo-2022"));
    assertTrue(LockssRepositoryUtil.validateNamespace("foo.bar"));
    assertTrue(LockssRepositoryUtil.validateNamespace("foo-bar"));
  }

  @Test
  public void testIsIdenticalToPreviousVersion() throws Exception {

  LockssRepository repo = new VolatileLockssRepository();
    String auid = "aaauuuiiiddd43";
    String url = "http://a.b/c";
    String ns = "ns1";
    ArtifactSpec spec1 = ArtifactSpec.forNsAuUrl(ns, auid, url)
      .setContent("content42a")
      .setCollectionDate(1234);
    ArtifactSpec spec2 = ArtifactSpec.forNsAuUrl(ns, auid, url)
      .setContent("xontent42a")
      .setCollectionDate(1234);
    Artifact unc1 = repo.addArtifact(spec1.getArtifactData());
    assertFalse(unc1.isCommitted());
    assertFalse(LockssRepositoryUtil.isIdenticalToPreviousVersion(repo, unc1));
    repo.commitArtifact(unc1);
    assertTrue(unc1.isCommitted());
    try {
      LockssRepositoryUtil.isIdenticalToPreviousVersion(repo, unc1);
      fail("isIdenticalToPreviousVersion() on committed Artifact should throw");
    } catch (IllegalStateException e) {
      // expected
    }

    Artifact unc2 = repo.addArtifact(spec2.getArtifactData());
    assertFalse(LockssRepositoryUtil.isIdenticalToPreviousVersion(repo, unc2));
    repo.commitArtifact(unc2);

    Artifact unc3 = repo.addArtifact(spec2.getArtifactData());
    assertTrue(LockssRepositoryUtil.isIdenticalToPreviousVersion(repo, unc3));
  }
}
