package org.lockss.rs.util;

import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.util.LockssRepositoryUtil;
import org.lockss.util.test.LockssTestCase5;

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
}
