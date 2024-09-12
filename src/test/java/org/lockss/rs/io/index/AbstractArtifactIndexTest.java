/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

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

package org.lockss.rs.io.index;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.EnumSource;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.VariantState;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataUtil;
import org.lockss.rs.io.storage.warc.WarcArtifactState;
import org.lockss.test.LockssCoreTestCase5;
import org.lockss.util.ListUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.test.VariantTest;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractArtifactIndexTest<AI extends ArtifactIndex> extends LockssCoreTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Handle to a {@link BaseLockssRepository} for testing.
   */
  protected BaseLockssRepository repository;

  protected AI index;

  // *******************************************************************************************************************
  // * ABSTRACT METHODS
  // *******************************************************************************************************************

  protected abstract AI makeArtifactIndex() throws Exception;

  public abstract void testInitIndex() throws Exception;
  public abstract void testShutdownIndex() throws Exception;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @BeforeEach
  public void setupCommon() throws Exception {
    ArtifactDataStore ds = mock(ArtifactDataStore.class);
    repository = mock(BaseLockssRepository.class);

    when(ds.auWarcSize(anyString(), anyString()))
        .thenAnswer(args -> variantState.auSize(args.getArgument(0), args.getArgument(1)).getTotalWarcSize());
    when(repository.getArtifactDataStore()).thenReturn(ds);

    index = makeArtifactIndex();
    index.setLockssRepository(repository);

    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    when(repository.getRepositoryStateDirPath()).thenReturn(getTempDir().toPath());
    when(repository.getScheduledExecutorService()).thenReturn(ses);

    index.init();
    index.start();

    beforeVariant();
  }

  @AfterEach
  public void stopArtifactIndex() {
    if (index != null) index.stop();
  }

  // *******************************************************************************************************************
  // * VARIANTS FRAMEWORK
  // *******************************************************************************************************************

  protected VariantState variantState = new VariantState();
  protected String variant = "no_variant";

  public enum TestIndexScenarios {
    empty, commit1, delete1, double_delete, double_commit, commit_delete_2x2
  }

  // Invoked automatically before each test by the @VariantTest mechanism
  @Override
  protected void setUpVariant(String variantName) {
    log.info("setUpVariant: " + variantName);
    variant = variantName;
  }

  protected void beforeVariant() throws Exception {
    // Get artifact specs to populate the index under test with
    List<ArtifactSpec> artifactSpecs = getArtifactSpecsForVariant(variant);

    log.debug("variant: {}, artifactSpecs.size() = {}", variant, artifactSpecs.size());

    populateIndex(index, artifactSpecs);
  }

  protected List<ArtifactSpec> getArtifactSpecsForVariant(String variant) {
    List<ArtifactSpec> specs = new ArrayList<>();

    switch (variant) {
      case "no_variant":
        // Not a variant test
        break;

      case "empty":
        // Empty data store
        break;

      case "commit1":
        // One committed artifact
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "u").thenCommit());
        break;

      case "delete1":
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "u").thenDelete());
        break;

      case "double_delete":
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "u").thenDelete().thenDelete());
        break;

      case "double_commit":
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "u").thenCommit().thenCommit());
        break;

      case "commit_delete_2x2":
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "u").thenDelete().thenCommit());
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "u").thenCommit().thenDelete());
        break;
      case "unicode":
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "111").thenCommit());
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "ZZZ").thenCommit());
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "zzz").thenCommit());
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "\u03BA\u1F79\u03C3\u03BC\u03B5").thenCommit());
        specs.add(ArtifactSpec.forNsAuUrl("c", "a", "Heiz\u00F6lr\u00FCcksto\u00DFabd\u00E4mpfung").thenCommit());
        break;
    }

    return specs;
  }

//  protected void populateIndex(List<ArtifactSpec> artifactSpecs) throws IOException {
//    populateIndex(index, artifactSpecs);
//  }

  protected void populateIndex(AI index, List<ArtifactSpec> artifactSpecs) throws IOException {
    for (ArtifactSpec spec : artifactSpecs) {
      // Assign random artifact ID
      spec.setArtifactUuid(UUID.randomUUID().toString());

      // Add artifact from this ArtifactSpec into index
      indexArtifactSpec(index, spec);
    }
  }

  protected void indexArtifactSpec(ArtifactSpec spec) throws IOException {
    indexArtifactSpec(index, spec);
  }

  protected void indexArtifactSpec(AI index, ArtifactSpec spec) throws IOException {
    // Generate content if needed (Artifact objects must represent)
    if (!spec.hasContent()) {
      spec.generateContent();
      spec.setStorageUrl(URI.create(spec.getArtifactUuid()));
    }

    // Set version if one was not provided
    if (spec.getVersion() < 0) {
      ArtifactSpec highest = variantState.getHighestVerSpec(spec.artButVerKey());
      spec.setVersion((highest == null) ? 1 : highest.getVersion() + 1);
    }

    // Add artifact to index
    ArtifactData ad = spec.getArtifactData();
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(ad));
    variantState.add(spec);
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));

    // Sanity checks
    assertTrue(index.artifactExists(spec.getArtifactUuid()));
    Artifact indexedArtifact = index.getArtifact(spec.getArtifactUuid());
    assertNotNull(indexedArtifact);
    spec.assertArtifactCommon(indexedArtifact);

    // Replay operations on artifact
    for (ArtifactSpec.ArtifactDataStoreOperation op : spec.getDataStoreOperations()) {
      switch (op) {
        case COMMIT:
          index.commitArtifact(spec.getArtifactUuid());
          spec.setCommitted(true);
          variantState.commit(spec.getArtifactUuid());
          log.debug("Committed artifact from artifact specification [spec: {}]", spec);
          break;

        case DELETE:
          index.deleteArtifact(spec.getArtifactUuid());
          spec.setDeleted(true);
          variantState.delFromAll(spec);
          log.debug("Deleted artifact from artifact specification [spec: {}]", spec);
          break;

        default:
          log.warn("Unknown artifact operation in spec: [spec: {}, op: {}]", spec, op);
          continue;
      }
    }
  }

  // *******************************************************************************************************************
  // * TEST METHODS
  // *******************************************************************************************************************

  @Test
  public void testWaitReady() throws Exception {
    // Instantiate a new index (that has not had initIndex() called already)
    ArtifactIndex index = makeArtifactIndex();

    repository = mock(BaseLockssRepository.class);
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    when(repository.getRepositoryStateDirPath()).thenReturn(getTempDir().toPath());
    when(repository.getScheduledExecutorService()).thenReturn(ses);

    index.setLockssRepository(repository);

    // Assert waiting on a deadline that expires immediately results in a TimeoutException thrown
    assertThrows(TimeoutException.class, () -> index.waitReady(Deadline.in(-1L)));

    // Initialize the index in a separate thread
    new Thread(() -> {
      index.init();
      index.start();
    }).start();

    // Assert waiting with a sufficient deadline works
    try {
      log.debug("Calling waitReady()");
      index.waitReady(Deadline.in(TIMEOUT_SHOULDNT)); // Arbitrary - might fail on slow or busy systems!
    } catch (TimeoutException e) {
      fail(String.format("Unexpected TimeoutException thrown: %s", e));
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testIndexArtifact() throws Exception {
    // Assert attempting to index a null Artifact throws an IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null artifact", () -> index.indexArtifact(null));

    // Assert against variant scenario
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (!spec.isDeleted()) {
        assertTrue(index.artifactExists(spec.getArtifactUuid()));
        spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));
      } else {
        assertFalse(index.artifactExists(spec.getArtifactUuid()));
        assertNull(index.getArtifact(spec.getArtifactUuid()));
      }
    }

    // Create an artifact spec
    ArtifactSpec spec1 = ArtifactSpec.forNsAuUrl("c", "a", "u1").thenCommit();
    spec1.setArtifactUuid(UUID.randomUUID().toString());
    spec1.generateContent();
    spec1.setStorageUrl(URI.create("file:///tmp/a"));

    // Index the artifact and assert the Artifact the operation returns matches the spec
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(spec1.getArtifactData()));
    spec1.assertArtifactCommon(index.getArtifact(spec1.getArtifactUuid()));

    // Assert the artifact exists in the index and it matches the spec
    assertTrue(index.artifactExists(spec1.getArtifactUuid()));
    spec1.assertArtifactCommon(index.getArtifact(spec1.getArtifactUuid()));
  }

  /**
   * Asserts that an artifact's repository state is recorded accurately in the index.
   *
   * TODO: Move this to ArtifactSpec#assertArtifact(LockssRepository, Artifact)?
   *
   * @throws Exception
   */
  @Test
  public void testIndexArtfact_artifactRepoState() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace("ns1")
        .setAuid("auid")
        .setUrl("url")
        .setVersion(1)
        .setCollectionDate(TimeBase.nowMs())
        .setStorageUrl(new URI("storageUrl"))
        .setContentLength(1232L)
        .setCommitted(true)
        .setDeleted(false);

    spec.generateContent();

//    WarcArtifactState state = WarcArtifactState.UNKNOWN;
//    if (spec.isCommitted()) state = WarcArtifactState.PENDING_COPY;
//    if (spec.isDeleted()) state = WarcArtifactState.DELETED;

    ArtifactData ad = spec.getArtifactData();
//    ad.setArtifactState(state);
    Artifact artifact = WarcArtifactDataUtil.getArtifact(ad);
    artifact.setCommitted(true);

    index.indexArtifact(artifact);

    assertEquals(spec.isCommitted(), index.getArtifact(spec.getArtifactUuid()).isCommitted());
  }

  public void testIndexUnicodeArtfact_artifactRepoState() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace("namespace")
        .setAuid("auid")
        .setUrl("Heiz\u00F6lr\u00FCcksto\u00DFabd\u00E4mpfung") // Heizölrückstoßabdämpfung
        .setVersion(1)
        .setCollectionDate(TimeBase.nowMs())
        .setStorageUrl(new URI("storageUrl"))
        .setContentLength(1232L)
        .setCommitted(true)
        .setDeleted(false);

    spec.generateContent();

    WarcArtifactState state = WarcArtifactState.UNKNOWN;
    if (spec.isCommitted()) state = WarcArtifactState.PENDING_COPY;
    if (spec.isDeleted()) state = WarcArtifactState.DELETED;

    ArtifactData ad = spec.getArtifactData();
//    ad.setArtifactState(state);

    index.indexArtifact(WarcArtifactDataUtil.getArtifact(ad));

    assertEquals(spec.isCommitted(), index.getArtifact(spec.getArtifactUuid()).isCommitted());
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifact_variants() throws Exception {
    // Assert retrieving an artifact with a null artifact ID (String or UUID) throws IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty artifact UUID",
        () -> index.getArtifact((String) null));
    assertThrowsMatch(IllegalArgumentException.class, "Null UUID",
        () -> index.getArtifact((UUID) null));

    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (spec.isToDelete()) {
        assertTrue(spec.isDeleted());
        assertFalse(index.artifactExists(spec.getArtifactUuid()));
        assertNull(index.getArtifact(spec.getArtifactUuid()));
      } else {
        assertFalse(spec.isDeleted());
        assertTrue(index.artifactExists(spec.getArtifactUuid()));
        spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));
      }
    }

    // Assert retrieving a non-existent artifact ID returns null (String and UUID variants)
    assertNull(index.getArtifact("unknown"));
    assertNull(index.getArtifact(UUID.randomUUID()));

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("c", "a", "u1").thenCommit();
    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Assert conditions expected if the artifact is not in the index
    assertFalse(index.artifactExists(spec.getArtifactUuid()));
    assertNull(index.getArtifact(spec.getArtifactUuid()));
    assertNull(index.getArtifact(UUID.fromString(spec.getArtifactUuid())));

    // Index the artifact
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(spec.getArtifactData()));

    // Sanity check
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));

    // Retrieve artifact from index and assert it matches the spec
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));
    spec.assertArtifactCommon(index.getArtifact(UUID.fromString(spec.getArtifactUuid())));
  }

  @Test
  public void testGetArtifact_latestCommitted() throws Exception {
    String NS = "d";
    String AU_A = "a";
    String AU_B = "b";
    String URL_U = "u";
    String URL_V = "v";

    // Create ArtifactSpecs with multiple versions; keep track of highest version in variant state
    List<ArtifactSpec> specs = new ArrayList<>();
    specs.add(createArtifactSpec(NS, AU_A, URL_U, 1, false));
    specs.add(createArtifactSpec(NS, AU_A, URL_U, 2, false));

    specs.add(createArtifactSpec(NS, AU_B, URL_U, 1, true));
    specs.add(createArtifactSpec(NS, AU_B, URL_U, 2, false));

    specs.add(createArtifactSpec(NS, AU_B, URL_V, 1, true));
    specs.add(createArtifactSpec(NS, AU_B, URL_V, 2, false));
    specs.add(createArtifactSpec(NS, AU_B, URL_V, 3, true));

    // Populate index and update variant state
    populateIndex(index, specs);

    // Used as handle to getArtifact(...) result
    ArtifactSpec spec;

    // A_U

    assertNull(variantState.getLatestArtifactSpec(NS, AU_A, URL_U, false));
    assertNull(index.getArtifact(NS, AU_A, URL_U, false));
//    assertLatestArtifactVersion(NAMESPACE, AU_A, URL_U, false, -1);
    assertLatestArtifactVersion(NS, AU_A, URL_U, true, 2);

    // Commit a_u_1
    spec = specs.get(0);
    index.commitArtifact(spec.getArtifactUuid());
    variantState.commit(spec.getArtifactUuid());

    assertLatestArtifactVersion(NS, AU_A, URL_U, false, 1);
    assertLatestArtifactVersion(NS, AU_A, URL_U, true, 2);

    //// B_U

    assertLatestArtifactVersion(NS, AU_B, URL_U, false, 1);
    assertLatestArtifactVersion(NS, AU_B, URL_U, true, 2);

    // Commit b_u_2
    spec = specs.get(3);
    index.commitArtifact(spec.getArtifactUuid());
    variantState.commit(spec.getArtifactUuid());

    assertLatestArtifactVersion(NS, AU_B, URL_U, false, 2);
    assertLatestArtifactVersion(NS, AU_B, URL_U, true, 2);

    //// B_V

    assertLatestArtifactVersion(NS, AU_B, URL_V, false, 3);
    assertLatestArtifactVersion(NS, AU_B, URL_V, true, 3);

    // Commit b_v_2
    spec = specs.get(5);
    index.commitArtifact(spec.getArtifactUuid());
    variantState.commit(spec.getArtifactUuid());

    assertLatestArtifactVersion(NS, AU_B, URL_V, false, 3);
    assertLatestArtifactVersion(NS, AU_B, URL_V, true, 3);
  }

  public void assertLatestArtifactVersion(String namespace, String auid, String uri, boolean includeUncommitted, int expectedVersion) throws Exception {
    // Get latest version Artifact from variant state and index
    Artifact expected = variantState.getLatestArtifactSpec(namespace, auid, uri, includeUncommitted).getArtifact();
    Artifact actual = index.getArtifact(namespace, auid, uri, includeUncommitted);

    // Assert latest Artifact from variant and index match
    assertEquals(expected, actual);

    // Sanity check
    assertEquals(expectedVersion, (int) expected.getVersion());
    assertEquals(expectedVersion, (int) actual.getVersion());
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifacts_variants() throws Exception {
    List<Artifact> expected;

    // Assert variant state
    for (String ns : variantState.allNamespaces()) {
      for (String auid : variantState.allAuids(ns)) {
        // With includeUncommitted set to false
        expected = variantState.getArtifactsFrom(variantState.getLatestArtifactSpecs(ns, auid, false));
        assertIterableEquals(expected, index.getArtifacts(ns, auid, false));

        // With includeUncommitted set to true
        expected = variantState.getArtifactsFrom(variantState.getLatestArtifactSpecs(ns, auid, true));
        assertIterableEquals(expected, index.getArtifacts(ns, auid, true));
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsAllVersions_forUrl() throws Exception {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(createArtifactSpec("d", "a", "u", 1, false));
    specs.add(createArtifactSpec("d", "a", "u", 2, false));
    specs.add(createArtifactSpec("d", "a", "u1", 1, true));
    specs.add(createArtifactSpec("d", "a", "u2", 1, true));
    specs.add(createArtifactSpec("d", "a", "v", 1, false));
    specs.add(createArtifactSpec("d", "a", "v", 2, true));

    populateIndex(index, specs);

    assertEmpty(index.getArtifactsAllVersions("d", "a", "u"));

    assertIterableEquals(artList(specs, 5), index.getArtifactsAllVersions("d", "a", "v"));

    index.commitArtifact(specs.get(4).getArtifactUuid());

    variantState.commit(specs.get(4).getArtifactUuid());

    log.debug("result = {}", ListUtil.fromIterable(index.getArtifactsAllVersions("d", "a", "v")));
    assertIterableEquals(artList(specs, 5, 4), index.getArtifactsAllVersions("d", "a", "v"));
  }

  @Test
  public void testGetArtifactsAllAus() throws Exception {
    //// ArtifactSpecs for this test
    List<ArtifactSpec> specs = new ArrayList<>();

    /* 0 */ specs.add(createArtifactSpec("d", "a", "u", 2, true));
    /* 1 */ specs.add(createArtifactSpec("d", "a", "u", 1, true));
    /* 2 */ specs.add(createArtifactSpec("d", "a", "v", 1, true));
    /* 3 */ specs.add(createArtifactSpec("d", "b", "u", 1, false));
    /* 4 */ specs.add(createArtifactSpec("d", "b", "v", 1, true));
    /* 5 */ specs.add(createArtifactSpec("d", "a", "u", 3, false));
    /* 6 */ specs.add(createArtifactSpec("d", "b", "u", 2, false));
    /* 7 */ specs.add(createArtifactSpec("d", "a", "v", 2, false));
    /* 8 */ specs.add(createArtifactSpec("d", "a", "v", 3, true));
    /* 9 */ specs.add(createArtifactSpec("d", "a", "v", 4, false));

    //// Populate artifact index from ArtifactSpecs
    populateIndex(index, specs);

    //// Assert unknown or null namespaces and URLs result in an empty set
    for (ArtifactVersions versions : ArtifactVersions.values()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> index.getArtifactsWithUrlFromAllAus(null, null, versions),
          "Namespace or URL is null");

      assertThrows(
          IllegalArgumentException.class,
          () -> index.getArtifactsWithUrlFromAllAus("d", null, versions),
          "Namespace or URL is null");

      assertThrows(
          IllegalArgumentException.class,
          () -> index.getArtifactsWithUrlFromAllAus(null, "u", versions),
          "Namespace or URL is null");
    }

    //// Demonstrate deleting an artifact affects the result

    assertIterableEquals(
        artList(specs, 8, 2, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 8, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.LATEST)
    );

    index.commitArtifact(specs.get(7).getArtifactUuid());
    variantState.commit(specs.get(7).getArtifactUuid());

    assertIterableEquals(
        artList(specs, 8, 7, 2, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 8, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.LATEST)
    );

    index.deleteArtifact(specs.get(7).getArtifactUuid());

    /// ...result should not change...

    assertIterableEquals(
        artList(specs, 8, 2, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 8, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.LATEST)
    );

    index.deleteArtifact(specs.get(9).getArtifactUuid());

    /// ...result should not change...

    assertIterableEquals(
        artList(specs, 8, 2, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 8, 4),
        index.getArtifactsWithUrlFromAllAus("d", "v", ArtifactVersions.LATEST)
    );

    //// Demonstrate committing an artifact affects the result
    assertIterableEquals(
        artList(specs, 0, 1),
        index.getArtifactsWithUrlFromAllAus("d", "u", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 0),
        index.getArtifactsWithUrlFromAllAus("d", "u", ArtifactVersions.LATEST)
    );

    // Commit all uncommitted artifacts
    for (ArtifactSpec spec : specs) {
      if (!spec.isCommitted()) {
        index.commitArtifact(spec.getArtifactUuid());
        variantState.commit(spec.getArtifactUuid());
      }
    }

    // Verify proper order
    assertIterableEquals(
        artList(specs, 5, 0, 1, 6, 3),
        index.getArtifactsWithUrlFromAllAus("d", "u", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 5, 6),
        index.getArtifactsWithUrlFromAllAus("d", "u", ArtifactVersions.LATEST)
    );
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsWithPrefix_variants() throws Exception {
    List<ArtifactSpec> specs = new ArrayList<>();

    specs.add(createArtifactSpec("d", "a", "u", 1, true));
    specs.add(createArtifactSpec("d", "a", "u", 2, false));
    specs.add(createArtifactSpec("d", "a", "u1", 1, true));
    specs.add(createArtifactSpec("d", "a", "u2", 1, true));
    specs.add(createArtifactSpec("d", "a", "v", 1, false));
    specs.add(createArtifactSpec("d", "a", "v", 2, true));

    populateIndex(index, specs);

    // Assert an unknown
    assertEmpty(index.getArtifactsWithPrefix("c", "a", "w"));

    assertEquals(4, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "")));
    assertEquals(3, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u")));
    assertEquals(1, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "v")));

    assertEquals(1, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u1")));
    assertEquals(0, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "ux")));

    assertEquals(1, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u2")));
    assertEquals(0, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u2x")));

    ArtifactSpec spec = specs.get(1);
    spec.setCommitted(true);
    index.commitArtifact(spec.getArtifactUuid());
    variantState.commit(spec.getArtifactUuid());

    assertEquals(3, IterableUtils.size(index.getArtifactsWithPrefix("d", "a", "u")));

  }

  @Test
  public void testGetArtifactsWithPrefixAllVersionsAllAus() throws Exception {
    //// ArtifactSpecs for this test
    List<ArtifactSpec> specs = new ArrayList<>();

    /* 00 */ specs.add(createArtifactSpec("d", "a", "u", 2, true));
    /* 01 */ specs.add(createArtifactSpec("d", "a", "u", 1, true));
    /* 02 */ specs.add(createArtifactSpec("d", "a", "u1", 3, true));
    /* 03 */ specs.add(createArtifactSpec("d", "a", "u1", 2, true));
    /* 04 */ specs.add(createArtifactSpec("d", "a", "u1", 1, true));
    /* 05 */ specs.add(createArtifactSpec("d", "a", "u2", 2, true));
    /* 06 */ specs.add(createArtifactSpec("d", "a", "u2", 1, false));
    /* 07 */ specs.add(createArtifactSpec("d", "a", "v", 1, true));
    /* 08 */ specs.add(createArtifactSpec("d", "b", "u", 1, false));
    /* 09 */ specs.add(createArtifactSpec("d", "b", "v", 1, true));
    /* 10 */ specs.add(createArtifactSpec("d", "a", "u", 3, false));
    /* 11 */ specs.add(createArtifactSpec("d", "b", "u", 2, false));

    //// Populate artifact index from ArtifactSpecs
    populateIndex(index, specs);

    //// Assert unknown or null namespaces and URLs result in an empty set
    for (ArtifactVersions versions : ArtifactVersions.values()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> index.getArtifactsWithUrlPrefixFromAllAus(null, null, versions),
          "Namespace or URL is null");

      assertThrows(
          IllegalArgumentException.class,
          () -> index.getArtifactsWithUrlPrefixFromAllAus(null, "u", versions),
          "Namespace or URL is null");
    }

    // Assert a null prefix returns all the committed artifacts in the namespace
    assertIterableEquals(
        specs.stream()
            .filter(ArtifactSpec::isCommitted)
            .map(ArtifactSpec::getArtifact)
            .collect(Collectors.toList()),
        index.getArtifactsWithUrlPrefixFromAllAus("d", null, ArtifactVersions.ALL)
    );

    // Assert a null prefix returns all the committed artifacts in the namespace
    assertIterableEquals(
        artList(specs, 0, 2, 5, 7, 9),
        index.getArtifactsWithUrlPrefixFromAllAus("d", null, ArtifactVersions.LATEST)
    );

    assertGetArtifactsWithUrlPrefixFromAllAus(specs, "d", "u");
    assertGetArtifactsWithUrlPrefixFromAllAus(specs, "d", "u1");

    //// Assert affect of committing an artifact on result
    assertIterableEquals(
        artList(specs, 5),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u2", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 5),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u2", ArtifactVersions.LATEST)
    );

    // Commit all uncommitted artifacts
    for (ArtifactSpec spec : specs) {
      if (!spec.isCommitted()) {
        index.commitArtifact(spec.getArtifactUuid());
        variantState.commit(spec.getArtifactUuid());
      }
    }

    // Verify proper order
    assertIterableEquals(
        artList(specs, 10, 0, 1, 11, 8, 2, 3, 4, 5, 6),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 10, 11, 2, 5),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u", ArtifactVersions.LATEST)
    );

    assertIterableEquals(
        artList(specs, 5, 6),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u2", ArtifactVersions.ALL));

    assertIterableEquals(
        artList(specs, 5),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u2", ArtifactVersions.LATEST));

    //// Assert affect of deleting an artifact on result
    assertIterableEquals(
        artList(specs, 2, 3, 4),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u1", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 2),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u1", ArtifactVersions.LATEST)
    );

    index.deleteArtifact(specs.get(3).getArtifactUuid());
    variantState.delFromAll(specs.get(3));

    assertIterableEquals(
        artList(specs, 2, 4),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u1", ArtifactVersions.ALL)
    );

    assertIterableEquals(
        artList(specs, 2),
        index.getArtifactsWithUrlPrefixFromAllAus("d", "u1", ArtifactVersions.LATEST)
    );
  }

  private void assertGetArtifactsWithUrlPrefixFromAllAus(List<ArtifactSpec> specs, String namespace, String prefix) throws IOException {
    assertIterableEquals(
        specs.stream()
            .filter(spec -> !spec.isDeleted())
            .filter(ArtifactSpec::isCommitted)
            .filter(spec -> spec.getNamespace().equals(namespace))
            .filter(spec -> spec.getUrl().startsWith(prefix))
            .map(ArtifactSpec::getArtifact)
            .collect(Collectors.toList()),
        index.getArtifactsWithUrlPrefixFromAllAus(namespace, prefix, ArtifactVersions.ALL)
    );

    assertIterableEquals(
        specs.stream()
            .filter(spec -> !spec.isDeleted())
            .filter(ArtifactSpec::isCommitted)
            .filter(spec -> spec.getNamespace().equals(namespace))
            .filter(spec -> spec.getUrl().startsWith(prefix))
            .map(ArtifactSpec::getArtifact)

            .collect(Collectors.groupingBy(
                artifact -> artifact.getIdentifier().getArtifactStem(),
                Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)

            .collect(Collectors.toList()),
        index.getArtifactsWithUrlPrefixFromAllAus(namespace, prefix, ArtifactVersions.LATEST)
    );
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactVersion() throws Exception {
    // Assert unknown artifact version keys causes getArtifactVersion(...) to return null
    assertNull(index.getArtifactVersion(null, null, null, 0));
    assertNull(index.getArtifactVersion("d", "a", "v", 1));

    Integer[] versions = {1, 2, 3, 41};
    Boolean[] commits = {true, true, false, true};

    // Populate the index with fixed multiple versions of the same artifact stem
    for (Pair<Integer, Boolean> pair : zip(versions, commits)) {
      indexArtifactSpec(createArtifactSpec("d", "a", "v", pair.getKey(), pair.getValue()));
    }

    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      Artifact expected = spec.getArtifact();
      Artifact actual = index.getArtifactVersion(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion());

      if (spec.isCommitted()) {
        assertNotNull(actual);
        spec.assertArtifactCommon(actual);
        assertEquals(expected, actual);
      } else {
        assertNull(actual);
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testUpdateStorageUrl_variants() throws Exception {
    // Attempt to update the storage URL of a null artifact ID
    assertThrowsMatch(IllegalArgumentException.class, "Invalid artifact UUID", () -> index.updateStorageUrl(null,
        "xxx"));

    // Attempt to update the storage URL of an unknown artifact ID
    // assertThrowsMatch(IOException.class, "Artifact not found", () -> index.updateStorageUrl("xyzzy", "xxx"));
    assertThrows(IOException.class, () -> index.updateStorageUrl("xyzzy", "xxx"));

    // Assert we're able to update the storage URLs of all existing artifacts
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      // Ensure the current storage URL matches the artifact specification in the variant state
      Artifact indexed1 = index.getArtifact(spec.getArtifactUuid());
      assertEquals(spec.getArtifact().getStorageUrl(), indexed1.getStorageUrl());

      // Update the storage URL
      String nsURL = UUID.randomUUID().toString();
      Artifact updated = index.updateStorageUrl(spec.getArtifactUuid(), nsURL);
      spec.setStorageUrl(URI.create(nsURL));

      // Assert the artifact returned by updateStorageUrl() reflects the new storage URL
      assertEquals(spec.getArtifact(), updated);
      assertEquals(spec.getArtifact().getStorageUrl(), updated.getStorageUrl());

      // Fetch the artifact from index and assert it matches
      Artifact indexed2 = index.getArtifact(spec.getArtifactUuid());
      assertEquals(spec.getArtifact(), indexed2);
      assertEquals(spec.getArtifact().getStorageUrl(), indexed2.getStorageUrl());
    }

    // Assert updating the storage URL of a deleted artifact returns null (unknown artifact)
    ArtifactSpec spec = variantState.anyDeletedSpec();
    if (spec != null) {
      assertNull(index.updateStorageUrl(spec.getArtifactUuid(), "xxx"));
    }
  }

  private static final int MAXIMUM_URL_LENGTH = 32000;

  @Test
  public void testLongUrls() throws Exception {
    // Build successively longer URLs (not really necessary but good for debugging)
    List<String> urls = IntStream.range(1, 14)
        .mapToObj(i -> RandomStringUtils.randomAlphanumeric((int) Math.pow(2, i)))
        .collect(Collectors.toList());

    // Add URL with maximum length
    urls.add(RandomStringUtils.randomAlphanumeric(MAXIMUM_URL_LENGTH));

    // Create and index artifact specs for each URL
    for (String url : urls) {
      ArtifactSpec spec = createArtifactSpec("c", "a", url, 1, false);
      indexArtifactSpec(spec);
    }

    // Ensure the URL of artifacts in the index matches the URL in their specification
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      Artifact indexed = index.getArtifact(spec.getArtifactUuid());
      assertEquals(spec.getUrl(), indexed.getUri());
    }

    /*
    try {
      // Create a URL one byte longer than the maximum length
      String url = RandomStringUtils.randomAscii(MAXIMUM_URL_LENGTH + 1);

      // Attempt to index the artifact - should throw
      ArtifactSpec spec = createArtifactSpec("c", "a", url, 1, false);
      indexArtifactSpec(spec);

      // Fail if we reach here
      fail("Expected createArtifactSpec(...) to throw an Exception");
    } catch (SolrException e) {
      // OK
    }
    */
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testAuSize() throws Exception {
    AuSize zero = new AuSize();

    // Check AU size of non-existent AUs
    assertEquals(zero, index.auSize(null, null));
    assertEquals(zero, index.auSize("ns1", null));
    assertEquals(zero, index.auSize(null, "auid"));
    assertEquals(zero, index.auSize("ns1", "auid"));

    // Assert variant state
    for (String ns : variantState.allNamespaces()) {
      for (String auid : variantState.allAuids(ns)) {
        log.debug("index.auSize() = {}", index.auSize(ns, auid));
        log.debug("variantState.auSize() = {}", variantState.auSize(ns, auid));

        AuSize auSize = index.auSize(ns, auid);

        assertEquals(variantState.auSize(ns, auid), auSize);
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testCommitArtifact_variants() throws Exception {
    // Assert committing an artifact with a null artifact ID (String or UUID) throws IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty artifact UUID",
        () -> index.commitArtifact((String) null));
    assertThrowsMatch(IllegalArgumentException.class, "Null UUID", () -> index.commitArtifact((UUID) null));

    // Assert committing an artifact of unknown artifact ID returns null
    assertNull(index.commitArtifact("unknown"));
    assertNull(index.commitArtifact(UUID.randomUUID()));

    // Assert against variant scenario
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {

      // Assert index
      if (!spec.isToDelete()) {
        // Sanity check
        assertFalse(spec.isDeleted());

        // Assert artifact exists in the index and that it matches the spec
        assertTrue(index.artifactExists(spec.getArtifactUuid()));
        Artifact artifact = index.getArtifact(spec.getArtifactUuid());
        spec.assertArtifactCommon(artifact);

        // Assert committed state of artifact against spec
        if (spec.isToCommit()) {
          assertTrue(spec.isCommitted());
          assertTrue(artifact.getCommitted());
        } else {
          assertFalse(spec.isCommitted());
          assertFalse(artifact.getCommitted());
        }
      } else {
        // Assert deleted artifact states
        assertTrue(spec.isDeleted());
        assertFalse(index.artifactExists(spec.getArtifactUuid()));
        assertNull(index.getArtifact(spec.getArtifactUuid()));
      }

      // Commit (again)
      Artifact indexed = index.commitArtifact(spec.getArtifactUuid());
      spec.setCommitted(true);

      if (!spec.isDeleted()) {
        // Assert artifact is committed
        assertNotNull(indexed);
        spec.assertArtifactCommon(indexed);
        assertTrue(indexed.getCommitted());
      } else {
        // Assert commit operation on a non-existent, deleted artifact returns null
        assertNull(indexed);
        assertFalse(index.artifactExists(spec.getArtifactUuid()));
        assertNull(index.getArtifact(spec.getArtifactUuid()));
      }
    }
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testDeleteArtifact_variants() throws Exception {
    // Assert deleting an artifact with null artifact ID (String or UUID) throws IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty UUID",
        () -> index.deleteArtifact((String) null));
    assertThrowsMatch(IllegalArgumentException.class, "Null UUID", () -> index.deleteArtifact((UUID) null));

    // Assert removing a non-existent artifact ID returns false (String and UUID)
    assertFalse(index.deleteArtifact("unknown"));
    assertFalse(index.deleteArtifact(UUID.randomUUID()));

    // Assert variant state
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      assertEquals(spec.isDeleted(), !index.artifactExists(spec.getArtifactUuid()));
    }

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("c", "a", "u1").thenCommit();
    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Assert conditions expected if the artifact is not in the index
    assertFalse(index.artifactExists(spec.getArtifactUuid()));
    assertNull(index.getArtifact(spec.getArtifactUuid()));
    assertFalse(index.deleteArtifact(spec.getArtifactUuid()));

    // Index the artifact data from spec and assert the return matches spec
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(spec.getArtifactData()));
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));

    // Assert conditions expected if the artifact is in the index
    assertTrue(index.artifactExists(spec.getArtifactUuid()));
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));

    // Delete from index and assert operation was successful (returns true)
    assertTrue(index.deleteArtifact(spec.getArtifactUuid()));

    // Assert conditions expected if the artifact is not in the index
    assertFalse(index.artifactExists(spec.getArtifactUuid()));
    assertNull(index.getArtifact(spec.getArtifactUuid()));
    assertFalse(index.deleteArtifact(spec.getArtifactUuid()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testArtifactExists() throws Exception {
    // Attempt calling artifactExists() with null artifact ID; should throw IllegalArgumentException
    assertThrowsMatch(IllegalArgumentException.class, "Null or empty artifact UUID", () -> index.artifactExists(null));

    // Assert artifactExists() returns false for an artifact with an unknown artifact ID
    assertFalse(index.artifactExists("unknown"));

    // Assert result of artifactExists() depending on variant state and spec
    for (ArtifactSpec spec : variantState.getArtifactSpecs()) {
      if (spec.isToDelete()) {
        assertTrue(spec.isDeleted());
        assertFalse(index.artifactExists(spec.getArtifactUuid()));
        assertNull(index.getArtifact(spec.getArtifactUuid()));
      } else {
        assertFalse(spec.isDeleted());
        assertTrue(index.artifactExists(spec.getArtifactUuid()));
        spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));
      }
    }

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("c", "a", "u1").thenCommit();
    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Assert that an artifact by the spec's artifact ID does not exist yet
    assertFalse(index.artifactExists(spec.getArtifactUuid()));

    // Add the artifact to the index
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(spec.getArtifactData()));

    // Assert that an artifact by the spec's artifact ID now exists
    assertTrue(index.artifactExists(spec.getArtifactUuid()));
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));

    // Remove the artifact from the index
    index.deleteArtifact(spec.getArtifactUuid());

    // Assert that an artifact by the spec's artifact ID no longer exists
    assertFalse(index.artifactExists(spec.getArtifactUuid()));
    assertNull(index.getArtifact(spec.getArtifactUuid()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetNamespaces_variants() throws Exception {
    // Assert that the namespaces from the index matches the variant scenario state
    assertIterableEquals(variantState.activeNamespaces(), index.getNamespaces());

    // Create an artifact spec
    ArtifactSpec spec1 = ArtifactSpec.forNsAuUrl("xyzzy", "foo", "bar");
    spec1.setArtifactUuid(UUID.randomUUID().toString());
    spec1.generateContent();
    spec1.setStorageUrl(URI.create("file:///tmp/a"));

    // Sanity check
    assertFalse(spec1.isToCommit());
    assertFalse(spec1.isCommitted());

    // Index artifact from artifact spec
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(spec1.getArtifactData()));

    // Sanity check
    assertFalse(index.getArtifact(spec1.getArtifactUuid()).isCommitted());
    Artifact retrieved = index.getArtifact(spec1.getArtifactUuid());
    assertFalse(retrieved.getCommitted());

    // Assert the uncommitted artifact's namespace is included
    assertTrue(IterableUtils.contains(index.getNamespaces(), spec1.getNamespace()));

    // Commit artifact
    index.commitArtifact(spec1.getArtifactUuid());

    // Assert the committed artifact's namespace is now included
    assertTrue(IterableUtils.contains(index.getNamespaces(), spec1.getNamespace()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetAuIds_variants() throws Exception {
    // Assert calling getAuIds() with a null or unknown namespace returns an empty set
    assertTrue(IterableUtils.isEmpty(index.getAuIds(null)));
    assertTrue(IterableUtils.isEmpty(index.getAuIds("unknown")));

    // Assert variant scenario
    for (String ns : variantState.activeCommittedNamespaces()) {
      assertIterableEquals(variantState.activeAuids(ns), index.getAuIds(ns));
    }

    // Create an artifact spec
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl("xyzzy", "foo", "bar");
    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.generateContent();
    spec.setStorageUrl(URI.create("file:///tmp/a"));

    // Index the artifact
    index.indexArtifact(WarcArtifactDataUtil.getArtifact(spec.getArtifactData()));
    spec.assertArtifactCommon(index.getArtifact(spec.getArtifactUuid()));

    // Assert that set of AUIDs for the namespace is not empty until the artifact is committed
    assertFalse(IterableUtils.isEmpty(index.getAuIds(spec.getNamespace())));

    // Commit the artifact
    Artifact committed = index.commitArtifact(spec.getArtifactUuid());

    // Sanity check
    assertTrue(committed.getCommitted());
    spec.setCommitted(true);
    spec.assertArtifactCommon(committed);

    // Assert that set of AUIDs for the namespace is non-empty
    assertFalse(IterableUtils.isEmpty(index.getAuIds(spec.getNamespace())));
    assertIterableEquals(Collections.singleton(spec.getAuid()), index.getAuIds(spec.getNamespace()));
  }

  @VariantTest
  @EnumSource(TestIndexScenarios.class)
  public void testGetArtifactsAllVersions_inAU() throws Exception {
    String NS = "d";
    String AU_A = "a";
    String AU_B = "b";

    // Assert null and unknown namespaces/AUIDs return an empty result (i.e., no matches)
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, null, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, null, false)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(NS, null, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, AU_A, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(null, AU_B, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(NS, AU_A, true)));
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(NS, AU_B, true)));

    List<ArtifactSpec> specs = new ArrayList<>();
    ArtifactSpec spec_a_1 = createArtifactSpec(NS, AU_A, "u", 1, false);
    ArtifactSpec spec_b_1 = createArtifactSpec(NS, AU_B, "u", 1, false);
    ArtifactSpec spec_b_2 = createArtifactSpec(NS, AU_B, "v", 1, true);

    specs.add(spec_a_1);
    specs.add(spec_b_1);
    specs.add(spec_b_2);

    // Index the artifact from specs
    populateIndex(index, specs);

    // Used as handle to results from getArtifactsAllVersions(...)
    List<Artifact> result;

    // Used as handle to single Artifact result
    Artifact artifact;

    //// FIRST AU

    // Assert empty result if includeUncommitted is set to false
    assertTrue(IterableUtils.isEmpty(index.getArtifactsAllVersions(NS, AU_A, false)));

    // Assert we get back one artifact if we set includeUncommitted to true
    result = IterableUtils.toList(index.getArtifactsAllVersions(NS, AU_A, true));
    assertEquals(1, result.size());
    spec_a_1.assertArtifactCommon(result.get(0));

    // Commit artifact
    index.commitArtifact(spec_a_1.getArtifactUuid());
    spec_a_1.setCommitted(true);

    // Sanity check
    artifact = index.getArtifact(spec_a_1.getArtifactUuid());
    assertTrue(artifact.getCommitted());

    // Assert we now get back one artifact even if includeUncommitted is false
    result = IterableUtils.toList(index.getArtifactsAllVersions(NS, AU_A, false));
    assertEquals(1, result.size());
    spec_a_1.assertArtifactCommon(result.get(0));

    //// SECOND AU

    // Assert we get back the committed artifact with includeUncommitted set to false
    result = IterableUtils.toList(index.getArtifactsAllVersions(NS, AU_B, false));
    assertEquals(1, result.size());
    spec_b_2.assertArtifactCommon(result.get(0));

    // Assert we get back both artifacts with includeUncommitted set to true
    result = IterableUtils.toList(index.getArtifactsAllVersions(NS, AU_B, true));
    assertEquals(2, result.size());
    assertIterableEquals(ListUtil.list(spec_b_1.getArtifact(), spec_b_2.getArtifact()), result);

    // Commit artifact
    index.commitArtifact(spec_b_1.getArtifactUuid());
    spec_b_1.setCommitted(true);

    // Sanity check
    artifact = index.getArtifact(spec_b_1.getArtifactUuid());
    assertTrue(artifact.getCommitted());

    // Assert we now get back both artifact with includeUncommitted set to false
    result = IterableUtils.toList(index.getArtifactsAllVersions(NS, AU_B, false));
    assertEquals(2, result.size());
    assertIterableEquals(ListUtil.list(spec_b_1.getArtifact(), spec_b_2.getArtifact()), result);

    // Assert variant state
    for (String ns : variantState.allNamespaces()) {
      for (String auid : variantState.allAuids(ns)) {
        result = IterableUtils.toList(index.getArtifactsAllVersions(ns, auid, false));
        assertIterableEquals(variantState.getArtifactsFrom(variantState.getArtifactsAllVersions(ns, auid, false)), result);

        result = IterableUtils.toList(index.getArtifactsAllVersions(ns, auid, true));
        assertIterableEquals(variantState.getArtifactsFrom(variantState.getArtifactsAllVersions(ns, auid, true)), result);
      }
    }

    // Get any uncommitted ArtifactSpec from variant state (or null if one doesn't exist / isn't available)
    ArtifactSpec spec = variantState.anyUncommittedSpec();

    if (spec != null) {
      // Sanity check
      assertFalse(spec.isDeleted());
      assertFalse(spec.isCommitted());

      // Assert the uncommitted artifact *is not* included in the results for its AU with includeUncommitted set to false
      result = IterableUtils.toList(index.getArtifactsAllVersions(spec.getNamespace(), spec.getAuid(), false));
      assertFalse(result.contains(spec.getArtifact()));

      // Assert the uncommitted artifact *is* included in the results for its AU with includeUncommitted set to true
      result = IterableUtils.toList(index.getArtifactsAllVersions(spec.getNamespace(), spec.getAuid(), true));
      assertTrue(result.contains(spec.getArtifact()));

      // Commit the artifact
      index.commitArtifact(spec.getArtifactUuid());
      spec.setCommitted(true);

      // Assert the uncommitted artifact *is now* included in the results for its AU with includeUncommitted set to false
      result = IterableUtils.toList(index.getArtifactsAllVersions(spec.getNamespace(), spec.getAuid(), false));
      assertTrue(result.contains(spec.getArtifact()));
    }
  }

  // *******************************************************************************************************************
  // * LONG URL TESTS
  // *******************************************************************************************************************

  private static final int LONG_URL_THRESHOLD = 2500;
  private static final String BASE_URL = "http://www.lockss.org/";
  private static final int SHORT_URL_LENGTH = BASE_URL.length();
  private static final int EXACT_THRESHOLD_LENGTH = LONG_URL_THRESHOLD - BASE_URL.length();
  private static final int REALLY_LONG_URL_LENGTH = 32764 - BASE_URL.length(); // Max that still works with Solr

  private static ArtifactSpec makeArtifactSpec(String ns, String auid, String url, int version) {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(version)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(TimeBase.nowMs());

    log.debug2("spec = " + spec);
    return spec;
  }

  private static List<Artifact> getArtifactsFromSpecs(ArtifactSpec... specs) {
    List<Artifact> expected = Stream.of(specs)
        .map(ArtifactSpec::getArtifact)
        .toList();

    return expected;
  }

  private static void commitSpecs(ArtifactIndex index, ArtifactSpec specs[], int... idx) throws IOException {
    for (int i : idx) {
      index.commitArtifact(specs[i].getArtifactUuid());
      specs[i].setCommitted(true);
    }
  }

  @Test
  public void testAddArtifact() throws Exception {
    runTestAddArtifact(SHORT_URL_LENGTH);
    runTestAddArtifact(EXACT_THRESHOLD_LENGTH);
    runTestAddArtifact(REALLY_LONG_URL_LENGTH);
  }

  private void runTestAddArtifact(int len) throws Exception {
    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl(longUrl)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    index.indexArtifact(spec.getArtifact());

    // Assert against artifact specs
    Artifact artifact = index.getArtifact(spec.getArtifactUuid());
    assertNotNull(artifact);
    spec.assertArtifactCommon(artifact);
  }

  @Test
  public void testCommitArtifact() throws Exception {
    runTestCommitArtifact(SHORT_URL_LENGTH);
    runTestCommitArtifact(EXACT_THRESHOLD_LENGTH);
    runTestCommitArtifact(REALLY_LONG_URL_LENGTH);
  }

  private void runTestCommitArtifact(int len) throws Exception {
    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    ArtifactSpec reallyLong = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl(longUrl)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Add artifact to database
    index.indexArtifact(reallyLong.getArtifact());

    // Assert artifact not committed
    Artifact pre = index.getArtifact(reallyLong.getArtifactUuid());
    assertFalse(pre.isCommitted());

    // Commit artifact
    index.commitArtifact(reallyLong.getArtifactUuid());
    reallyLong.setCommitted(true);

    // Assert artifact now committed
    Artifact post = index.getArtifact(reallyLong.getArtifactUuid());
    assertTrue(post.isCommitted());
    reallyLong.assertArtifactCommon(post);
  }

  @Test
  public void testDeleteArtifact() throws Exception {
    runTestDeleteArtifact(SHORT_URL_LENGTH);
    runTestDeleteArtifact(EXACT_THRESHOLD_LENGTH);
    runTestDeleteArtifact(REALLY_LONG_URL_LENGTH);
  }

  private void runTestDeleteArtifact(int len) throws Exception {
    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl(longUrl)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Sanity check
    assertNull(index.getArtifact(spec.getArtifactUuid()));

    // Add artifact to database
    index.indexArtifact(spec.getArtifact());

    // Assert artifact pre-delete
    Artifact pre = index.getArtifact(spec.getArtifactUuid());
    spec.assertArtifactCommon(pre);

    // Delete artifact
    index.deleteArtifact(spec.getArtifactUuid());

    // Assert null post-delete
    assertNull(index.getArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void testGetArtifactsWithPrefixAllVersions() throws Exception {
    runTestGetArtifactsWithPrefixAllVersions(SHORT_URL_LENGTH);
    runTestGetArtifactsWithPrefixAllVersions(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifactsWithPrefixAllVersions(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifactsWithPrefixAllVersions(int len) throws Exception {
    String ns = "ns1";
    String auid = "a1";

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec[] specs = {
        makeArtifactSpec("ns1", "a1", url1, 1),
        makeArtifactSpec("ns1", "a1", url1, 2),
        makeArtifactSpec("ns1", "a1", url2, 1),
        makeArtifactSpec("ns1", "a1", url1, 3),
        makeArtifactSpec("ns1", "a2", url1, 1),
        makeArtifactSpec("ns2", "a1", url1, 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(index.getArtifactsWithPrefixAllVersions(ns, auid, longUrl));

    // Commit artifacts
    commitSpecs(index, specs, 0, 3);

    // Assert we get back the correct committed artifacts for the namespace and AUID
    List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[0]);
    Iterable<Artifact> result = index.getArtifactsWithPrefixAllVersions(ns, auid, longUrl);
    assertIterableEquals(expected, result);
  }

  @Test
  public void testGetArtifactsWithUrlPrefixFromAllAus() throws Exception {
    runTestGetArtifactsWithUrlPrefixFromAllAus(SHORT_URL_LENGTH);
    runTestGetArtifactsWithUrlPrefixFromAllAus(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifactsWithUrlPrefixFromAllAus(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifactsWithUrlPrefixFromAllAus(int len) throws Exception {
    String ns1 = "ns1";
    String ns2 = "ns2";
    String auid1 = "auid1";
    String auid2 = "auid2";

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, auid1, url1, 1),
        makeArtifactSpec(ns1, auid1, url1, 2),
        makeArtifactSpec(ns1, auid1, url1, 3),
        makeArtifactSpec(ns1, auid2, url1, 4),
        makeArtifactSpec(ns2, auid1, url1, 5),
        makeArtifactSpec(ns1, auid1, url2, 6),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty results (no committed artifacts)
    assertEmpty(index.getArtifactsWithUrlPrefixFromAllAus(ns1, url1, ArtifactVersions.ALL));
    assertEmpty(index.getArtifactsWithUrlPrefixFromAllAus(ns1, url1, ArtifactVersions.LATEST));

    // Commit artifacts
    commitSpecs(index, specs, 0, 3, 4, 5);

    // Assert with ALL
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[3]);
      Iterable<Artifact> result =
          index.getArtifactsWithUrlPrefixFromAllAus(ns1, url1, ArtifactVersions.ALL);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(index, specs, 1);

    // Assert with LATEST
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[3]);
      Iterable<Artifact> result =
          index.getArtifactsWithUrlPrefixFromAllAus(ns1, url1, ArtifactVersions.LATEST);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetArtifactsWithUrlFromAllAus() throws Exception {
    runTestGetArtifactsWithUrlFromAllAus(SHORT_URL_LENGTH);
    runTestGetArtifactsWithUrlFromAllAus(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifactsWithUrlFromAllAus(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifactsWithUrlFromAllAus(int len) throws Exception {
    String ns1 = "ns1";
    String ns2 = "ns2";
    String auid1 = "auid1";
    String auid2 = "auid2";

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, auid1, url1, 1),
        makeArtifactSpec(ns1, auid1, url1, 2),
        makeArtifactSpec(ns1, auid1, url1, 3),
        makeArtifactSpec(ns1, auid2, url1, 4),
        makeArtifactSpec(ns2, auid1, url1, 5),
        makeArtifactSpec(ns1, auid1, url2, 6),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty results (no committed artifacts)
    assertEmpty(index.getArtifactsWithUrlFromAllAus(ns1, url1, ArtifactVersions.ALL));
    assertEmpty(index.getArtifactsWithUrlFromAllAus(ns1, url1, ArtifactVersions.LATEST));

    // Commit artifacts
    commitSpecs(index, specs, 0, 3, 4, 5);

    // Assert with ALL
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[3]);
      Iterable<Artifact> result =
          index.getArtifactsWithUrlFromAllAus(ns1, url1, ArtifactVersions.ALL);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(index, specs, 1);

    // Assert with LATEST
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[3]);
      Iterable<Artifact> result =
          index.getArtifactsWithUrlFromAllAus(ns1, url1, ArtifactVersions.LATEST);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetArtifactsAllVersions() throws Exception {
    runTestGetArtifactsAllVersions(SHORT_URL_LENGTH);
    runTestGetArtifactsAllVersions(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifactsAllVersions(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifactsAllVersions(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";
    String url = url1;

    // Sanity check
    assertEmpty(index.getAuIds(ns));

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, a1, url1, 1),
        makeArtifactSpec(ns1, a1, url1, 2),
        makeArtifactSpec(ns1, a1, url2, 1),
        makeArtifactSpec(ns1, a1, url1, 3),
        makeArtifactSpec(ns1, a2, url1, 1),
        makeArtifactSpec(ns2, a1, url1, 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(index.getArtifactsAllVersions(ns, auid, url));

    // Commit artifacts
    commitSpecs(index, specs, 0, 3, 4);

    // Assert we get back the correct committed artifacts for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[0]);
      Iterable<Artifact> result = index.getArtifactsAllVersions(ns, auid, url);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetArtifactsAllVersions2_longUrl() throws Exception {
    runTestGetArtifactsAllVersions2_longUrl(SHORT_URL_LENGTH);
    runTestGetArtifactsAllVersions2_longUrl(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifactsAllVersions2_longUrl(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifactsAllVersions2_longUrl(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    // Sanity check
    assertEmpty(index.getAuIds(ns));

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, a1, url1, 1),
        makeArtifactSpec(ns1, a1, url1, 2),
        makeArtifactSpec(ns1, a1, url2, 1),
        makeArtifactSpec(ns1, a1, url1, 3),
        makeArtifactSpec(ns1, a2, url1, 1),
        makeArtifactSpec(ns2, a1, url1, 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(index.getArtifactsAllVersions(ns, auid, false));

    // Assert all versions of all URLs for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[1], specs[0], specs[2]);
      Iterable<Artifact> result = index.getArtifactsAllVersions(ns, auid, true);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(index, specs, 0, 3, 4);

    // Assert we get back the correct committed artifacts for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[0]);
      Iterable<Artifact> result = index.getArtifactsAllVersions(ns, auid, false);
      assertIterableEquals(expected, result);
    }

    // Assert all versions of all URLs for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[1], specs[0], specs[2]);
      Iterable<Artifact> result = index.getArtifactsAllVersions(ns, auid, true);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetArtifactsWithPrefix() throws Exception {
    runTestGetArtifactsWithPrefix(SHORT_URL_LENGTH);
    runTestGetArtifactsWithPrefix(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifactsWithPrefix(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifactsWithPrefix(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, a1, url1, 1), //0
        makeArtifactSpec(ns1, a1, url1, 2),
        makeArtifactSpec(ns1, a1, url1, 3),
        makeArtifactSpec(ns1, a2, url1, 4), // 3
        makeArtifactSpec(ns2, a1, url1, 5), // 4
        makeArtifactSpec(ns1, a1, url2, 6), // 5
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty results (no committed artifacts)
    assertEmpty(index.getArtifactsWithPrefix(ns1, a1, url1));
    assertEmpty(index.getArtifactsWithPrefix(ns1, a1, url1));

    // Commit artifacts
    commitSpecs(index, specs, 0, 3, 4, 5);

    // Assert results with (ns1, auid1, url1)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0]);
      Iterable<Artifact> result =
          index.getArtifactsWithPrefix(ns1, a1, url1);
      assertIterableEquals(expected, result);
    }

    // Assert results with (ns1, auid2, url1)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3]);
      Iterable<Artifact> result =
          index.getArtifactsWithPrefix(ns1, a2, url1);
      assertIterableEquals(expected, result);
    }

    // Assert results with (ns2, auid1, url1)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[4]);
      Iterable<Artifact> result =
          index.getArtifactsWithPrefix(ns2, a1, url1);
      assertIterableEquals(expected, result);
    }

    // Assert results with (ns1, auid1, url2)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[5]);
      Iterable<Artifact> result =
          index.getArtifactsWithPrefix(ns1, a1, url2);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(index, specs, 1);

    // Assert result with second set of commits
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1]);
      Iterable<Artifact> result =
          index.getArtifactsWithPrefix(ns1, a1, url1);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetAuIds() throws Exception {
    runTestGetAuIds(SHORT_URL_LENGTH);
    runTestGetAuIds(EXACT_THRESHOLD_LENGTH);
    runTestGetAuIds(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetAuIds(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url1)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Sanity check
    assertEmpty(index.getAuIds(ns));

    // Add artifact to database
    index.indexArtifact(spec.getArtifact());

    assertIterableEquals(List.of(auid), index.getAuIds(ns));
  }

  @Test
  public void testGetArtifacts() throws Exception {
    runTestGetArtifacts(SHORT_URL_LENGTH);
    runTestGetArtifacts(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifacts(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifacts(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    // Sanity check
    assertEmpty(index.getAuIds(ns));

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, a1, url1, 1),
        makeArtifactSpec(ns1, a1, url1, 2),
        makeArtifactSpec(ns1, a1, url2, 1),
        makeArtifactSpec(ns1, a2, url1, 1),
        makeArtifactSpec(ns2, a1, url1, 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      index.indexArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(index.getArtifacts(ns, auid, false));

    // Assert results match expected when including uncommitted artifacts
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[2]);
      Iterable<Artifact> result = index.getArtifacts(ns, auid, true);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(index, specs, 0, 2);

    // Assert results match expected when excluding uncommitted artifacts
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[2]);
      Iterable<Artifact> result = index.getArtifacts(ns, auid, false);
      assertIterableEquals(expected, result);
    }

    // Assert results match expected when including uncommitted artifacts
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[2]);
      Iterable<Artifact> result = index.getArtifacts(ns, auid, true);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetArtifact() throws Exception {
    runTestGetArtifact(SHORT_URL_LENGTH);
    runTestGetArtifact(EXACT_THRESHOLD_LENGTH);
    runTestGetArtifact(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetArtifact(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl(url1)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L)
        .setVersion(1);

    // Sanity check / enforce contract
    assertNull(index.getArtifact(spec.getArtifactUuid()));
    assertNull(index.getArtifactVersion(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true));

    // Add artifact to database
    index.indexArtifact(spec.getArtifact());

    Artifact byUuid = index.getArtifact(spec.getArtifactUuid());
    Artifact byTuple = index.getArtifactVersion(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true);

    spec.assertArtifactCommon(byUuid);
    spec.assertArtifactCommon(byTuple);
  }

  @Test
  public void testGetLatestArtifact() throws Exception {
    runTestGetLatestArtifact(SHORT_URL_LENGTH);
    runTestGetLatestArtifact(EXACT_THRESHOLD_LENGTH);
    runTestGetLatestArtifact(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetLatestArtifact(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec spec1 = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url1)
        .setVersion(1)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    ArtifactSpec spec2 = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url1)
        .setVersion(2)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Sanity check / enforce contract
    assertNull(index.getArtifact(ns, auid, url1, false));
    assertNull(index.getArtifact(ns, auid, url1, true));

    // Add artifacts to database
    index.indexArtifact(spec1.getArtifact());
    index.indexArtifact(spec2.getArtifact());

    // Assert no max committed artifact but max uncommitted is v2
    assertNull(index.getArtifact(ns, auid, url1, false));
    spec2.assertArtifactCommon(index.getArtifact(ns, auid, url1, true));

    // Commit v1 artifact
    index.commitArtifact(spec1.getArtifactUuid());
    spec1.setCommitted(true);

    // Assert latest committed is v1 and latest uncommitted remains v2
    spec1.assertArtifactCommon(index.getArtifact(ns, auid, url1, false));
    spec2.assertArtifactCommon(index.getArtifact(ns, auid, url1, true));

    // Commit v2 artifact
    index.commitArtifact(spec2.getArtifactUuid());
    spec2.setCommitted(true);

    // Assert latest committed and latest uncommitted is v2
    spec2.assertArtifactCommon(index.getArtifact(ns, auid, url1, false));
    spec2.assertArtifactCommon(index.getArtifact(ns, auid, url1, true));
  }

  @Test
  public void testGetNamespaces() throws Exception {
    runTestGetNamespaces(SHORT_URL_LENGTH);
    runTestGetNamespaces(EXACT_THRESHOLD_LENGTH);
    runTestGetNamespaces(REALLY_LONG_URL_LENGTH);
  }

  private void runTestGetNamespaces(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setUrl(url1)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Sanity check
    assertEmpty(index.getNamespaces());

    // Add artifact to database
    index.indexArtifact(spec.getArtifact());

    assertIterableEquals(List.of(ns), index.getNamespaces());

    // Remove artifact for next call to this method
    index.deleteArtifact(spec.getArtifactUuid());
  }

  @Test
  public void testUpdateStorageUrl() throws Exception {
    runTestUpdateStorageUrl(SHORT_URL_LENGTH);
    runTestUpdateStorageUrl(EXACT_THRESHOLD_LENGTH);
    runTestUpdateStorageUrl(REALLY_LONG_URL_LENGTH);
  }

  private void runTestUpdateStorageUrl(int len) throws Exception {
    String ns1 = RandomStringUtils.randomAlphanumeric(16);
    String ns2 = RandomStringUtils.randomAlphanumeric(16);
    String ns = ns1;

    String a1 = RandomStringUtils.randomAlphanumeric(16);
    String a2 = RandomStringUtils.randomAlphanumeric(16);
    String auid = a1;

    String longUrl = BASE_URL + RandomStringUtils.randomAlphanumeric(len);

    String url1 = longUrl + "/1";
    String url2 = longUrl + "/2";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl(url1)
        .setStorageUrl(URI.create("storage_url_1"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Add artifact to database
    index.indexArtifact(spec.getArtifact());

    // Assert URL pre-update
    Artifact pre = index.getArtifact(spec.getArtifactUuid());
    assertEquals("storage_url_1", pre.getStorageUrl());

    // Update URL
    index.updateStorageUrl(spec.getArtifactUuid(), "storage_url_2");

    // Assert URL pre-update
    Artifact post = index.getArtifact(spec.getArtifactUuid());
    assertEquals("storage_url_2", post.getStorageUrl());
  }

  // *******************************************************************************************************************
  // * STATIC UTILITY METHODS
  // *******************************************************************************************************************

  public static <A, B> List<Pair<A, B>> zip(A[] a, B[] b) {
    return zip(ListUtil.list(a), ListUtil.list(b));
  }

  public static <A, B> List<Pair<A, B>> zip(List<A> a, List<B> b) {
    return IntStream.range(0, Math.min(a.size(), b.size()))
        .mapToObj(i -> Pair.of(a.get(i), b.get(i)))
        .collect(Collectors.toList());
  }

  private static ArtifactSpec createArtifactSpec(String namespace, String auid, String uri, long version, boolean commit) {
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl(namespace, auid, uri);

    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.setVersion((int) version);

    if (commit) {
      spec.thenCommit();
    }

    return spec;
  }

  /** Return list of Artifacts correcponding to the selected elements of
   * the spec list */
  private static List<Artifact> artList(List<ArtifactSpec> specs,
                                        int... indices) {
      List<Artifact> res = new ArrayList<>();
      for (int ix : indices) {
        res.add(specs.get(ix).getArtifact());
      }
      return res;
    }

}
