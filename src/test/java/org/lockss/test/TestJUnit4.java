/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.*;

import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * <p>
 * Tests designed to illustrate what JUnit 4 does and does not do, for
 * reference and for comparison with JUnit 3.
 * </p>
 */
@RunWith(Enclosed.class)
public class TestJUnit4 {

  /**
   * <p>
   * Designed to illustrate how the {@link Suite} test runner works. This
   * demonstrates that the tests in {@link SuiteDemonstration.Foo},
   * {@link SuiteDemonstration.Foo.Bar} and
   * {@link OtherStuff.SuiteDemonstration2} are part of the suite but those of
   * {@link SuiteDemonstration.Foo.Bar.Baz} or directly inside
   * {@link SuiteDemonstration} (for instance
   * {@link SuiteDemonstration#testFooShouldNot()}) are not. The logic of this
   * test is in {@link #tearDownClass()} using an array of strings set up in
   * {@link #setUpClass()}.
   * </p>
   */
  @RunWith(Suite.class)
  @Suite.SuiteClasses({SuiteDemonstration.Foo.class,
                       SuiteDemonstration.Foo.Bar.class,
                       OtherStuff.SuiteDemonstration2.class})
  public static class SuiteDemonstration {
    
    public static List<String> testNames;
    
    @BeforeClass
    public static void setUpClass() {
      testNames = new ArrayList<String>();
    }
    
    @AfterClass
    public static void tearDownClass() {
      assertEquals(3, testNames.size());
      assertThat(testNames, hasItems("testFoo", "testBar", "testSuiteDemonstration2"));
      assertThat(testNames, not(hasItems("testFooShouldNot", "testBazShouldNot")));
    }
    
    public static class Foo {
      
      public static class Bar {
      
        public static class Baz {
          
          @Test
          public void testBazShouldNot() {
            fail("testBazShouldNot() should not have run");
          }
          
        }
        
        @Test
        public void testBar() {
          testNames.add("testBar");
        }
        
      }
 
      @Test
      public void testFoo() {
        testNames.add("testFoo");
      }
      
    }
    
    @Test
    public void testFooShouldNot() {
      fail("testFooShouldNot() should not have run");
    }
    
  }

  /**
   * <p>
   * Designed to illustrate how the {@link Enclosed} test runner works. This
   * demonstrates that the tests in {@link EnclosedDemonstration.Foo} are part
   * of the suite but those in {@link EnclosedDemonstration.Baz} (because it is
   * abstract), in {@link EnclosedDemonstration.Foo.Bar} (because it is nested
   * deeper), or directly inside {@link EnclosedDemonstration} (for instance
   * {@link EnclosedDemonstration#testFooShouldNot()}) are not. The logic of
   * this test is in {@link #tearDownClass()} using an array of strings set up
   * in {@link #setUpClass()}.
   * </p>
   */
  @RunWith(Enclosed.class)
  public static class EnclosedDemonstration {

    public static List<String> testNames;
    
    @BeforeClass
    public static void setUpClass() {
      testNames = new ArrayList<String>();
    }
    
    @AfterClass
    public static void tearDownClass() {
      assertEquals(1, testNames.size());
      assertThat(testNames, hasItems("testFoo"));
      assertThat(testNames, not(hasItems("testBarShouldNot", "testBazShouldNot")));
    }
    
    public static class Foo {
      
      public static class Bar {
        
        @Test
        public void testBarShouldNot() {
          fail("testBarShouldNot() should not have run");
        }
        
      }
      
      @Test
      public void testFoo() {
        testNames.add("testFoo");
      }
      
    }
    
    public static abstract class Baz {
      
      @Test
      public void testBazShouldNot() {
        fail("testBarShouldNot() should not have run");
      }
      
    }
    
  }
  
  /**
   * <p>
   * A container class for ancillary code used here.
   * </p>
   */
  public static abstract class OtherStuff {
    
    /**
     * <p>
     * An ostensibly external class used in {@link TestJUnit4.SuiteDemonstation}.
     * </p>
     * @see TestJUnit4.SuiteDemonstration
     */
    public static class SuiteDemonstration2 {
      
      @Test
      public void testSuiteDemonstration2() {
        SuiteDemonstration.testNames.add("testSuiteDemonstration2");
      }
      
    }

  }
  
}
