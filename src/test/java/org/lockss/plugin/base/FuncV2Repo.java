/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.plugin.base;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.security.MessageDigest;
import junit.framework.*;
import org.lockss.plugin.*;
import org.lockss.config.Configuration;
import org.lockss.daemon.*;
import org.lockss.test.*;
import org.lockss.util.*;

import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;
import org.apache.http.*;
import org.apache.http.message.*;
import org.springframework.http.HttpHeaders;


/** A place to experiment with the new repository, and document its
 * behavior
 */
public class FuncV2Repo extends LockssTestCase {
  private static final Logger log = Logger.getLogger();

  protected LockssRepository repo;
  protected MockArchivalUnit mau;
  protected MockLockssDaemon theDaemon;
  protected MockPlugin plugin;

  String COLL = "Collection 6A";
  String AUID = "AUID17";

  String url1 = "http://www.example.com/testDir/leaf7";
  String url2 = "http://www.example.com/testDir/leaf2";
  String url3 = "http://www.example.com/testDir/leaf3";
  String url4 = "http://www.example.com/testDir/leaf4";
  String urlparent = "http://www.example.com/testDir";
  String content1 = "test content 1";
  String content2 = "test content 2 longer";
  String badcontent = "this is the wrong content string";


  public void setUp() throws Exception {
    super.setUp();
    repo = LockssRepositoryFactory.createVolatileRepository();
//     repo = LockssRepositoryFactory.createLocalRepository(getTempDir());
  }

  private ArtifactData createArtifact(String collection, String auid,
				      String url, String content) {

    ArtifactIdentifier id =
      new ArtifactIdentifier(collection, auid, url, null);

    HttpHeaders metadata = new HttpHeaders();
    metadata.set("key1", "val1");
    metadata.set("key2", "val2");
    metadata.set("key3", "val3");

    BasicStatusLine statusLine =
      new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    return new ArtifactData(id, metadata, new StringInputStream(content), statusLine);
  }

  public void testVersion() throws IOException {
    ArtifactData ad1 = createArtifact(COLL, AUID, url1, "content 11111");
    Artifact art1 = repo.addArtifact(ad1);
    assertNull(repo.getArtifactVersion(COLL, AUID, url1, 1));
    assertNull(repo.getArtifactVersion(COLL, AUID, url1, 1, false));
    Artifact uncArt = repo.getArtifactVersion(COLL, AUID, url1, 1, true);
    assertEquals(art1.getId(), uncArt.getId());

    repo.commitArtifact(art1);
    Artifact r1 = repo.getArtifact(COLL, AUID, url1);

    assertArtifactCommitted(art1, r1);

    assertEquals(1, (int)r1.getVersion());
    Artifact aa = repo.getArtifactVersion(COLL, AUID, url1, 1);

    assertArtifactCommitted(art1, aa);

    assertEquals(r1, aa);

    aa = repo.getArtifactVersion(COLL, AUID, url1, 1, false);

    assertArtifactCommitted(art1, aa);
    assertEquals(r1, aa);

    Artifact aa2 = repo.getArtifactVersion(COLL, AUID, url1, 1, true);

    assertArtifactCommitted(art1, aa2);

    assertEquals(r1, aa2);

    ArtifactData ad2 = createArtifact(COLL, AUID, url1, "content 22222");
    Artifact art2 = repo.addArtifact(ad2);
    repo.commitArtifact(art2);
    aa = repo.getArtifact(COLL, AUID, url1);

    assertArtifactCommitted(art2, aa);
    assertEquals(2, (int)aa.getVersion());

    aa = repo.getArtifactVersion(COLL, AUID, url1, 2);

    assertArtifactCommitted(art2, aa);
    assertEquals(2, (int)aa.getVersion());

    aa = repo.getArtifactVersion(COLL, AUID, url1, 2, false);

    assertArtifactCommitted(art2, aa);
    assertEquals(2, (int)aa.getVersion());

    aa = repo.getArtifactVersion(COLL, AUID, url1, 2, true);

    assertArtifactCommitted(art2, aa);
    assertEquals(2, (int)aa.getVersion());

    ArtifactData ad3 = createArtifact(COLL, AUID, url1, "content 33333");
    Artifact art3 = repo.addArtifact(ad3);
    assertEquals(3, (int)art3.getVersion());
    repo.deleteArtifact(art3);
    ArtifactData ad4 = createArtifact(COLL, AUID, url1, "content 44444");
    Artifact art4 = repo.addArtifact(ad4);
    assertEquals(3, (int)art4.getVersion());

    ArtifactData ad5 = createArtifact(COLL, AUID, url1, "content 55555");
    Artifact art5 = repo.addArtifact(ad5);
    assertEquals(4, (int)art5.getVersion());

    ArtifactData ad4a = repo.getArtifactData(art4);
    assertInputStreamMatchesString("content 44444", ad4a.getInputStream());
    uncArt = repo.getArtifactVersion(COLL, AUID, url1, 3);
    assertNull(uncArt);
    repo.commitArtifact(art4);
    uncArt = repo.getArtifactVersion(COLL, AUID, url1, 3);
    assertNotNull(uncArt);

  }

  /**
   * Asserts two Artifacts are the same, modulo the committed status and storage URL.
   *
   * @param expected
   * @param actual
   */
  private void assertArtifactCommitted(Artifact expected, Artifact actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getCollection(), actual.getCollection());
    assertEquals(expected.getAuid(), actual.getAuid());
    assertEquals(expected.getUri(), actual.getUri());
    assertEquals(expected.getContentLength(), actual.getContentLength());
    assertEquals(expected.getContentDigest(), actual.getContentDigest());

    assertEquals(expected.getVersion(), actual.getVersion());

    assertNotEquals(expected.getCommitted(), actual.getCommitted());
    assertNotEquals(expected.getStorageUrl(), actual.getStorageUrl());
  }

  Artifact storeArt(String url, String content,
		    CIProperties props) throws IOException {
    return storeArt(url, new StringInputStream(content), props);
  }

  Artifact storeArt(String url, InputStream in,
		    CIProperties props) throws IOException {
    if (props == null) props = new CIProperties();
    return V2RepoUtil.storeArt(repo, COLL, AUID, url, in, props);
  }

  public void testGetVersions() throws IOException {
    ArtifactData ad1 = createArtifact(COLL, AUID, url1, "content 11111");
    Artifact art1 = repo.addArtifact(ad1);
    repo.commitArtifact(art1);
    List<Artifact> l0 =
      ListUtil.fromIterator(repo.getArtifacts(COLL, AUID).iterator());

    assertEquals(url1, l0.get(0).getUri());
    log.critical("testVersion all: " + l0);
    List l = ListUtil.fromIterator(repo.getArtifactsAllVersions(COLL, AUID,
							        url1).iterator());
    assertEquals(1, l.size());
  }


  public void testSlash() throws IOException {
    String uslash = url1 + "/";
    Artifact tst;
    ArtifactData ad;

    Artifact a1 = storeArt(url1, "content 11111", null);
    assertEquals(url1, a1.getUri());
    assertEquals(1, (int)a1.getVersion());

    tst = repo.getArtifact(COLL, AUID, uslash);
    assertNull(tst);

    Artifact a2 = storeArt(uslash, "content 222", null);
    assertEquals(uslash, a2.getUri());
    tst = repo.getArtifact(COLL, AUID, uslash);
    assertNotNull(tst);
    ad = repo.getArtifactData(tst);
    assertInputStreamMatchesString("content 222", ad.getInputStream());

    tst = repo.getArtifact(COLL, AUID, url1);
    assertNotNull(tst);
    ad = repo.getArtifactData(tst);
    assertInputStreamMatchesString("content 11111", ad.getInputStream());
}


  public void testRepo() throws IOException {
    Artifact art = repo.getArtifact(COLL, AUID, url1);
    assertNull(art);
    ArtifactData art1 = createArtifact(COLL, AUID, url1, "content 11111");
    art = repo.getArtifact(COLL, AUID, url1);
    assertNull(art);
    log.critical("adding: " + art1);
    Artifact newArt = repo.addArtifact(art1);
    log.critical("added: " + newArt);
    assertNotNull(newArt);
    log.critical("new artData meta: " + repo.getArtifactData(newArt).getMetadata());
    assertCompareIsEqualTo(art1.getIdentifier(), newArt.getIdentifier());
    List<Artifact> aids = 
      ListUtil.fromIterator(repo.getArtifacts(COLL, AUID).iterator());
    log.critical("foo: " + aids);

    Artifact committedArt = repo.commitArtifact(newArt);
    log.critical("committedArt ver: " + committedArt.getVersion());

    aids = ListUtil.fromIterator(repo.getArtifacts(COLL, AUID).iterator());
    log.critical("foo: " + aids);
    ArtifactData a1 = repo.getArtifactData(committedArt);
    assertInputStreamMatchesString("content 11111", a1.getInputStream());
    try {
      a1.getInputStream();
      fail("Attempt to call getInputStream() twice should throw");
    } catch (IllegalStateException e) {
    }

//     assertInputStreamMatchesString("content 2222", a1.getInputStream());

    ArtifactData ad2 = createArtifact(COLL, AUID, url2, "content xxxxx");
    Artifact art2 = repo.addArtifact(ad2);
    Artifact datas = repo.commitArtifact(art2);
    aids = ListUtil.fromIterator(repo.getArtifacts(COLL, AUID).iterator());
    log.critical("foo: " + aids);


//     art = 
// 	return ArtifactFactory.fromHttpResponseStream(response.getBody().getInputStream());

  }

  public void testFoo() throws IOException {
    String coll = "COLL";
    String auid = "AUID";
    String url = "http://foo/bar";

    ArtifactIdentifier id = new ArtifactIdentifier(coll, auid, url, null);

    HttpHeaders metadata = new HttpHeaders();
    metadata.set("key1", "val1");
    metadata.set("key2", "val2");

    BasicStatusLine statusLine =
      new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    log.critical("Creating ArtData with metadata: " + metadata);
    ArtifactData ad1 =
      new ArtifactData(id,
		       metadata,
		       new StringInputStream("bytes"),
		       statusLine);

    log.critical("ArtData has metadata: " + ad1.getMetadata());
    Artifact art1 = repo.addArtifact(ad1);
    log.critical("art1 metadata: " +
		 repo.getArtifactData(art1).getMetadata());
    log.critical("committing: " + art1);
    Artifact art2 = repo.commitArtifact(art1);
    log.critical("committed: " + art2);

    Artifact art3 = repo.getArtifact(coll, auid, url);
    log.critical("found: " + art3);

    log.critical("deleting: " + art1);
    repo.deleteArtifact(art1);
    log.critical("deleted: " + art1);
    Artifact art4 = repo.getArtifact(coll, auid, url);
    if (art4 == null) {
      log.critical("successfully deleted: " + art1);
    } else {
      log.critical("not deleted: " + art4);
    }

    ArtifactIdentifier id2 = new ArtifactIdentifier(coll, auid, url + "xxx", null);

    ArtifactData ad2 =
      new ArtifactData(id2,
		       metadata,
		       new StringInputStream("bytes"),
		       statusLine);

    Artifact artu = repo.addArtifact(ad2);
    //    assertTrue(artu.
    ArtifactData adu = repo.getArtifactData(artu);
    log.critical("adu: " + adu);

    log.critical("deleting: " + artu);
    repo.deleteArtifact(artu);


  }

}
