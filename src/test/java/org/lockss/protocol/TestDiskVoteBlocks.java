/*

Copyright (c) 2000-2024 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.protocol;

import java.io.*;
import java.util.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.Parameter;

import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.lockss.test.LockssTestCase4;
import org.lockss.log.*;
import org.lockss.util.*;


@RunWith(Parameterized.class)
public class TestDiskVoteBlocks extends LockssTestCase4 {
  L4JLogger log = L4JLogger.getLogger();

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { {true}, {false} });
  }

  @Parameter
  public boolean keepOpen;

  File tempDir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    tempDir = this.getTempDir();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /*
   * Test method for 'org.lockss.protocol.DiskVoteBlocks.DiskVoteBlocks(int, InputStream, File)'
   */
  @Test
  public void testDiskVoteBlocks() throws Exception {
//  Construct a byte array in memory to read in.
    int blockCount = 20;
    
    List voteBlockList = V3TestUtils.makeVoteBlockList(blockCount);

    UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    
    for (Iterator iter = voteBlockList.iterator(); iter.hasNext(); ) {
      VoteBlock vb = (VoteBlock)iter.next();
      dos.write(vb.getEncoded());
    }
    
    byte[] encodedBlocks = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(encodedBlocks);

    DiskVoteBlocks dvb = new MyDiskVoteBlocks(blockCount, bis, tempDir);
    
    assertEquals(dvb.size(), blockCount);
  }
  
  @Test
  public void testAddVoteBlock() throws Exception {
    VoteBlocksIterator iter;

    DiskVoteBlocks dvb = new MyDiskVoteBlocks(tempDir, keepOpen);
    assertEquals(0, dvb.size());

    List voteBlockList = V3TestUtils.makeVoteBlockList(3);
    
    dvb.addVoteBlock((VoteBlock)voteBlockList.get(0));
    assertEquals(1, dvb.size());
    
    dvb.addVoteBlock((VoteBlock)voteBlockList.get(1));
    assertEquals(2, dvb.size());
    
    dvb.addVoteBlock((VoteBlock)voteBlockList.get(2));
    assertEquals(3, dvb.size());
    
    dvb.close();
    // Compare the iterator against the vote block.
    iter = dvb.iterator();
    assertEquals((VoteBlock) voteBlockList.get(0), iter.next());
    assertEquals((VoteBlock) voteBlockList.get(1), iter.next());
    assertEquals((VoteBlock) voteBlockList.get(2), iter.next());
  }

  @Test
  public void testAddVoteBlocksBenchmark() throws Exception {
    // Set high to benchmark
//     int n = 1000000;
    int n = 1;
    VoteBlocksIterator iter;

    DiskVoteBlocks dvb = new MyDiskVoteBlocks(tempDir, keepOpen);
    assertEquals(0, dvb.size());

    List voteBlockList = V3TestUtils.makeVoteBlockList(3);
    long start = TimeBase.nowMs();

    for (int x = 0; x < n; x++) {
      dvb.addVoteBlock((VoteBlock)voteBlockList.get(0));
    }
    dvb.close();
    log.info("Adding {} VoteBlocks {}reopening each time took {}",
             n, keepOpen ? "without " : "",
             StringUtil.timeIntervalToString(TimeBase.msSince(start)));
    assertEquals(n, dvb.size());
  }

  /*
   * Test method for 'org.lockss.protocol.DiskVoteBlocks.iterator()'
   */
  @Test
  public void testIterator() throws Exception {
    
    List<VoteBlock> voteBlockList = V3TestUtils.makeVoteBlockList(3);
    
    DiskVoteBlocks dvb = makeDiskVoteBlocks(voteBlockList);
    
    VoteBlocksIterator iter = dvb.iterator();

    // First
    assertTrue(iter.hasNext());
    // Be sure peek works.
    VoteBlock p1 = (VoteBlock)iter.peek();
    VoteBlock p2 = (VoteBlock)iter.peek();
    VoteBlock p3 = (VoteBlock)iter.peek();
    VoteBlock vb0 = (VoteBlock)iter.next();
    assertEquals(p1, vb0);
    assertEquals(p2, vb0);
    assertEquals(p3, vb0);
    assertNotNull(vb0);
    assertEquals(vb0, (VoteBlock)voteBlockList.get(0));
    
    // Second
    assertTrue(iter.hasNext());
    VoteBlock vb1 = (VoteBlock)iter.next();
    assertNotNull(vb1);
    assertEquals(vb1, (VoteBlock)voteBlockList.get(1));
    
    // Third
    assertTrue(iter.hasNext());
    VoteBlock vb2 = (VoteBlock)iter.next();
    assertNotNull(vb2);
    assertEquals(vb2, (VoteBlock)voteBlockList.get(2));
    
    // Shouldn't be a next.
    assertFalse(iter.hasNext());

    // Shouldn't increment
    // IMPORTANT: This was the previous way to indicate that iter was empty.
    // It now throws a "NoSuchElementException".
//    assertNull(iter.next());
//    assertNull(iter.next());
//    assertNull(iter.next());
  }
  
  private static final int k_maxLength = 20;
  
  /*
   * Second, more intensive test method for DiskVoteBlocks.Iterator 
   */
  @Test
  public void testIterator2() throws Exception {
    DiskVoteBlocks dvb;
    int iCountLength;
    int iLength;
    VoteBlocksIterator iter;
    VoteBlock vbNext;
    VoteBlock vbPeek;
    List<VoteBlock> voteBlockList;
    
    // TEST: For a vote block list that's 1, 2, ..., 20 long,
    // test hasNext, next, and peek.
    for (iLength = 0; iLength < k_maxLength; iLength++) {
      voteBlockList = V3TestUtils.makeVoteBlockList(iLength);
      dvb = makeDiskVoteBlocks(voteBlockList);
    
      iter = dvb.iterator();
      
      iCountLength = 0;
      while (iter.hasNext()) {
        
        vbPeek = iter.peek();
        vbNext = iter.next();
        
        assertEquals(vbPeek, voteBlockList.get(iCountLength));
        assertEquals(vbPeek, vbNext);
        
        iCountLength++;
        
        // Verify that we're not reading more than iCountLength records.
        assertEquals(iCountLength,
		     ((MyDiskVoteBlocks.MyIterator) iter).getReadCount());
      }
      
      assertEquals(iLength, iCountLength);
    }
  }
  
  /*
   * Third test: Verify that peek() and next() will throw "NoSuchElementException"
   * when no elements are left.
   */
  @Test
  public void testIterator3() throws Exception {
    DiskVoteBlocks dvb;
    int iLength;
    VoteBlocksIterator iter;
    List<VoteBlock> voteBlockList;
    
    for (iLength = 0; iLength < k_maxLength; iLength++) {
      voteBlockList = V3TestUtils.makeVoteBlockList(iLength);
      dvb = makeDiskVoteBlocks(voteBlockList);
    
      iter = dvb.iterator();
      
      while (iter.hasNext()) {
	assertNotNull(iter.peek());
        iter.next();
      }
      
      assertNull(iter.peek());
      assertNull(iter.peek());

      try {
        iter.next();
        fail("testIterator3: next after end of 'hasNext' should have thrown a NoSuchElementException.");
      } catch (NoSuchElementException e) {
        /* This, too, shall pass */
      }

      assertNull(iter.peek());

    }
  }
  
  
  /*
   * Test method for 'org.lockss.protocol.DiskVoteBlocks.size()'
   */
  @Test
  public void testSize() throws Exception {
    List voteBlockList = V3TestUtils.makeVoteBlockList(10);
    DiskVoteBlocks dvb = makeDiskVoteBlocks(voteBlockList);
    assertEquals(dvb.size(), 10);
  }

  /*
   * Test method for 'org.lockss.protocol.DiskVoteBlocks.delete()'
   */
  @Test
  public void testDelete() {
    // XXX:  To do.
  }

  /*
   * Test method for 'org.lockss.protocol.DiskVoteBlocks.getInputStream()'
   */
  @Test
  public void testGetInputStream() throws Exception {
    List voteBlockList = V3TestUtils.makeVoteBlockList(10);
    DiskVoteBlocks dvb = makeDiskVoteBlocks(voteBlockList);
    InputStream is = dvb.getInputStream();
    
    assertNotNull(is);
    assertTrue(is instanceof InputStream);
  }
  
  private DiskVoteBlocks makeDiskVoteBlocks(List voteBlockList)
      throws Exception {
    DiskVoteBlocks dvb = new MyDiskVoteBlocks(tempDir, false);
    for (Iterator iter = voteBlockList.iterator(); iter.hasNext(); ) {
      VoteBlock vb = (VoteBlock)iter.next();
      dvb.addVoteBlock(vb);
    }
    dvb.close();
    return dvb;
  }

  @Test
  public void testToString() throws Exception {
    DiskVoteBlocks dvb = new MyDiskVoteBlocks(tempDir, false);

    List voteBlockList = V3TestUtils.makeVoteBlockList(3);
    
    dvb.addVoteBlock((VoteBlock)voteBlockList.get(0));
    dvb.addVoteBlock((VoteBlock)voteBlockList.get(1));
    dvb.addVoteBlock((VoteBlock)voteBlockList.get(2));
    dvb.close();
    String str = dvb.toString();
    assertMatchesRE("VoteBlock: Content, /test-0\\.html, 1 version\\(s\\)",
		    str);
    assertMatchesRE("VoteBlock: Content, /test-2\\.html, 1 version\\(s\\)",
		    str);
    assertEquals(3, StringUtil.countOccurences(str, "\n"));
  }

  class MyDiskVoteBlocks extends DiskVoteBlocks {
    public MyDiskVoteBlocks(int blocksToRead, InputStream from, File toDir)
	throws IOException {
      super(blocksToRead, from, toDir);
    }

    public MyDiskVoteBlocks(File toDir, boolean keepOpen) throws IOException {
      super(toDir, keepOpen);
    }

    public VoteBlocksIterator iterator() throws FileNotFoundException {
      return new MyIterator();
    }

    class MyIterator extends DiskVoteBlocks.Iterator {
      private int m_countDiskReads = 0;

      public MyIterator() throws FileNotFoundException {
	super();
      }

      protected void readVB() throws IOException {
	super.readVB();
	m_countDiskReads++;
      }
    
      public int getReadCount() {
	return m_countDiskReads;
      }
    }
  }

}
