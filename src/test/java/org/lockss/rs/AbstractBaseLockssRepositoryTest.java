package org.lockss.rs;

import org.lockss.log.L4JLogger;

/**
 * Common tests and testing infrastructure for subclasses of {@link BaseLockssRepository}.
 */
public abstract class AbstractBaseLockssRepositoryTest extends AbstractLockssRepositoryTest {
  private final static L4JLogger log = L4JLogger.getLogger();

  @Override
  public abstract BaseLockssRepository getLockssRepository() throws Exception;
}
