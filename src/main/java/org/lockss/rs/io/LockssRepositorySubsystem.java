package org.lockss.rs.io;

import org.lockss.rs.BaseLockssRepository;

public interface LockssRepositorySubsystem {
  void setLockssRepository(BaseLockssRepository repository);
  void init();
  void start();
  void stop();
}
