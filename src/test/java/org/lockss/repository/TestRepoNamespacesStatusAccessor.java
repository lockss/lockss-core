/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University.
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
package org.lockss.repository;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.lockss.daemon.status.StatusTable;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.rest.repo.LockssRepository;

/**
 * Tests that the "Repositories and Namespaces" status table gets its AU count
 * without enumerating the AUIDs.
 * <p>
 * That table used to call {@code IterableUtils.size(repo.getAuIds(ns))}, which
 * on a REST-backed repository pages every AUID in the namespace across the
 * network only to discard them — the slowness reported in #733. Asserting on
 * the displayed number alone would not catch a regression to that, since both
 * ways produce the same number, so this asserts on which call was made.
 */
public class TestRepoNamespacesStatusAccessor extends LockssTestCase4 {

  private static final String NAMESPACE = "ns1";
  private static final long AU_COUNT = 17;

  /** Counts which of the two ways of getting a count the table used. */
  private final AtomicInteger getAuIdCountCalls = new AtomicInteger();
  private final AtomicInteger getAuIdsCalls = new AtomicInteger();

  /**
   * The table must ask the repository for the count, and must not ask it for
   * the AUIDs.
   */
  @Test
  public void testAuCountDoesNotEnumerateAuids() throws Exception {
    MockLockssDaemon daemon = getMockLockssDaemon();

    RepoSpec spec = RepoSpec.fromSpec("volatile:" + NAMESPACE)
        .setRepository(makeCountingRepository());

    daemon.setRepositoryManager(new SingleSpecRepositoryManager(spec));

    LockssRepositoryStatus.RepoNamespacesStatusAccessor accessor =
        new LockssRepositoryStatus.RepoNamespacesStatusAccessor(daemon);

    StatusTable table = new StatusTable("RepositoriesTable");
    accessor.populateTable(table);

    assertEquals("the table must ask for the count", 1,
        getAuIdCountCalls.get());
    assertEquals("the table must not enumerate the AUIDs to count them", 0,
        getAuIdsCalls.get());

    // ...and the number it displays is the one the repository reported.
    List rows = table.getSortedRows();
    assertEquals(1, rows.size());

    Map row = (Map)rows.get(0);
    StatusTable.Reference aus = (StatusTable.Reference)row.get("aus");
    assertEquals(AU_COUNT, ((Number)aus.getValue()).longValue());
  }

  /**
   * A {@link LockssRepository} that records which counting call it received.
   * Built as a proxy because the interface is large and this test cares about
   * exactly two of its methods.
   */
  private LockssRepository makeCountingRepository() {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args)
          throws Throwable {
        switch (method.getName()) {
        case "getAuIdCount":
          getAuIdCountCalls.incrementAndGet();
          return AU_COUNT;
        case "getAuIds":
          getAuIdsCalls.incrementAndGet();
          return Collections.emptyList();
        case "getNamespaces":
          return Collections.singletonList(NAMESPACE);
        case "isReady":
          return true;
        case "toString":
          return "countingRepository";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          return null;
        }
      }
    };

    return (LockssRepository)Proxy.newProxyInstance(
        getClass().getClassLoader(),
        new Class<?>[] {LockssRepository.class},
        handler);
  }

  /**
   * A RepositoryManager that reports exactly one repo spec and no disk usage,
   * so the accessor runs against the stub above rather than a real repository.
   */
  private static class SingleSpecRepositoryManager extends RepositoryManager {
    private final RepoSpec spec;

    SingleSpecRepositoryManager(RepoSpec spec) {
      this.spec = spec;
    }

    @Override
    public synchronized Collection<RepoSpec> getAllRepoSpecs() {
      return Collections.singletonList(spec);
    }

    @Override
    public PlatformUtil.DF getRepositoryDF(String repoSpec) {
      return null;
    }
  }
}
