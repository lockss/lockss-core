/*
 * $Id$
 */

/*

Copyright (c) 2000-2003 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.test;
import junit.framework.*;

/**
 * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
 *             used by plugins); use
 *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
 */
@Deprecated
public class TestPrivilegedAccessor extends LockssTestCase {

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public TestPrivilegedAccessor(String name) {
    super(name);
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testParent() throws Exception {
    MyMockParent parent = new MyMockParent("Charlie");
    assertEquals("Charlie", PrivilegedAccessor.getValue(parent, "m_name"));
    PrivilegedAccessor.invokeMethod(parent, "setName", "Timmah!");
    assertEquals("Timmah!", PrivilegedAccessor.getValue(parent,"m_name"));
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testChild() throws Exception {
    MyMockChild child = new MyMockChild("Charlie", 8);
    assertEquals("Charlie", PrivilegedAccessor.getValue(child, "m_name"));
    assertEquals(Integer.valueOf(8),
		 PrivilegedAccessor.getValue(child, "m_number"));

    PrivilegedAccessor.invokeMethod(child, "setName", "Timmah!");
    PrivilegedAccessor.invokeMethod(child, "setNumber", Integer.valueOf(3));

    assertEquals("Timmah!", PrivilegedAccessor.getValue(child,"m_name"));
    assertEquals(Integer.valueOf(3),
		 PrivilegedAccessor.getValue(child, "m_number"));
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testChildWithParentReference() throws Exception {
    MyMockParent parent = new MyMockChild("Charlie", 8);
    assertEquals("Charlie", PrivilegedAccessor.getValue(parent, "m_name"));
    assertEquals(Integer.valueOf(8),
		 PrivilegedAccessor.getValue(parent, "m_number"));

    Object args[] = {"Timmah!", Integer.valueOf(3)};
    PrivilegedAccessor.invokeMethod(parent, "setData", args);

    assertEquals("Timmah!", PrivilegedAccessor.getValue(parent,"m_name"));
    assertEquals(Integer.valueOf(3),
		 PrivilegedAccessor.getValue(parent, "m_number"));

    PrivilegedAccessor.invokeMethod(parent, "setName", "prashant");
    assertEquals("prashant", PrivilegedAccessor.getValue(parent,"m_name"));
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testInvalidField() throws Exception {
    MyMockParent parent = new MyMockParent("Charlie");
    try {
      Object value = PrivilegedAccessor.getValue(parent, "zzz");
      fail("Should throw NoSuchFieldException");
    } catch (NoSuchFieldException e) {
    }
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testInvalidMethodName() throws Exception {
    MyMockChild child = new MyMockChild("Charlie", 8);
    try {
      PrivilegedAccessor.invokeMethod(child, "zzz", "Timmah!");
      fail("Should throw NoSuchMethodException");
    } catch(NoSuchMethodException e) {
    }
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testInvalidArguments() throws Exception {
    MyMockChild child = new MyMockChild("Charlie", 8);
    try {
      PrivilegedAccessor.invokeMethod(child, "setData", "Timmah!");
      fail("Should throw NoSuchMethodException");
    } catch (NoSuchMethodException e) {
    }
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testInstanceParam() throws Exception {
    try {
      new PrivilegedAccessor.Instance(String.class, Float.valueOf(5));
      fail("PrivilegedAccessor.Instance should have thrown ClassCastException");
    } catch (ClassCastException e) {
    }
    MyMockParent parent = new MyMockParent();
    Object nullString = new PrivilegedAccessor.Instance(String.class, null);
    Boolean bool =
      (Boolean)PrivilegedAccessor.invokeMethod(parent, "isNullString",
					       nullString);
    assertTrue(bool.booleanValue());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testUnambiguousNullArg() throws Exception {
    MyMockParent parent = new MyMockParent();
    Object[] args = {null};
    Boolean bool =
      (Boolean)PrivilegedAccessor.invokeMethod(parent, "isNullString", args);
    assertTrue(bool.booleanValue());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testAmbiguousNullArg() throws Exception {
    MyMockChild child = new MyMockChild("Charlie", 8);
    Object[] args1 = {"foo"};
    Object[] args2 = {null};
    assertEquals("child.string",
		 PrivilegedAccessor.invokeMethod(child, "over", args1));
    try {
      PrivilegedAccessor.invokeMethod(child, "over", args2);
      fail("invokeMethod should have thrown an AmbiguousMethodException for null parameter");
    } catch (PrivilegedAccessor.AmbiguousMethodException e) {
    }
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testUnambiguousArg() throws Exception {
    MyMockParent parent = new MyMockParent();
    MyMockChild child = new MyMockChild("Charlie", 8);
    Object[] args1 = {Integer.valueOf(1), Float.valueOf(2)};
    Object[] args2 = {Float.valueOf(1), Integer.valueOf(2)};
    Object[] args3 = {Float.valueOf(1), new PrivilegedAccessor.Instance(Number.class, Float.valueOf(2.0F))};
    assertEquals("parent.string",
		 PrivilegedAccessor.invokeMethod(parent, "over", "foo"));
    assertEquals("child.string",
		 PrivilegedAccessor.invokeMethod(child, "over", "foo"));
    assertEquals("child.number",
		 PrivilegedAccessor.invokeMethod(child, "over",
						 Integer.valueOf(1)));
    assertEquals("child.float",
		 PrivilegedAccessor.invokeMethod(child, "over", Float.valueOf(1.2F)));
    assertEquals("child.number",
		 PrivilegedAccessor.invokeMethod(child, "over",
						 new PrivilegedAccessor.
						   Instance(Number.class,
							    Float.valueOf(1.2F))));

    assertEquals("child.number.float",
		 PrivilegedAccessor.invokeMethod(child, "over", args1));
    assertEquals("child.float.number",
		 PrivilegedAccessor.invokeMethod(child, "over", args2));
    assertEquals("child.float.number",
		 PrivilegedAccessor.invokeMethod(child, "over", args3));
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testAmbiguousArg() throws Exception {
    MyMockChild child = new MyMockChild("Charlie", 8);
    Object[] args1 = {Float.valueOf(1), Float.valueOf(2)};
    try {
      PrivilegedAccessor.invokeMethod(child, "over", args1);
      fail("invokeMethod should have thrown an AmbiguousMethodException");
    } catch (PrivilegedAccessor.AmbiguousMethodException e) {
    }
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testStatic() throws Exception {
    MyMockParent parent = new MyMockParent();
    MyMockChild child = new MyMockChild("Charlie", 8);
    assertEquals("parent.static",
		 PrivilegedAccessor.invokeMethod(parent, "stat", null));
    assertEquals("child.static",
		 PrivilegedAccessor.invokeMethod(child, "stat", null));
    assertEquals("parent.static",
		 PrivilegedAccessor.invokeMethod(new PrivilegedAccessor.
		   Instance(MyMockParent.class, child),
						 "stat", null));
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testNoArgConstructor() throws Exception {
    ClassWithPrivateConstructor c =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor(ClassWithPrivateConstructor.class);
    assertEquals(0, c.getN());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testOneArgConstructor() throws Exception {
    ClassWithPrivateConstructor c =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor(ClassWithPrivateConstructor.class,
					   Integer.valueOf(7));
    assertEquals(1, c.getN());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testUnambiguousConstructor() throws Exception {
    Object[] args1 = {Integer.valueOf(7),
		      new ClassWithPrivateConstructor.Sub()};
    Object[] args2 = {Integer.valueOf(7),
		      new ClassWithPrivateConstructor.Super()};

    ClassWithPrivateConstructor c1 =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor(ClassWithPrivateConstructor.class,
					   args1);
    assertEquals(3, c1.getN());
    ClassWithPrivateConstructor c2 =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor(ClassWithPrivateConstructor.class,
					   args2);
    assertEquals(2, c2.getN());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testNoArgConstructorByName() throws Exception {
    ClassWithPrivateConstructor c =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor("org.lockss.test.ClassWithPrivateConstructor");
    assertEquals(0, c.getN());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testOneArgConstructorByName() throws Exception {
    ClassWithPrivateConstructor c =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor("org.lockss.test.ClassWithPrivateConstructor",
					   Integer.valueOf(7));
    assertEquals(1, c.getN());
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public void testUnambiguousConstructorByName() throws Exception {
    Object[] args1 = {Integer.valueOf(7),
		      new ClassWithPrivateConstructor.Sub()};
    Object[] args2 = {Integer.valueOf(7),
		      new ClassWithPrivateConstructor.Super()};

    ClassWithPrivateConstructor c1 =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor("org.lockss.test.ClassWithPrivateConstructor",
					   args1);
    assertEquals(3, c1.getN());
    ClassWithPrivateConstructor c2 =
      (ClassWithPrivateConstructor)
      PrivilegedAccessor.invokeConstructor("org.lockss.test.ClassWithPrivateConstructor",
					   args2);
    assertEquals(2, c2.getN());
  }

  // Test utility classes

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static class MyMockParent {
    private String m_name;

    public MyMockParent() {
    }
    public MyMockParent(String name) {
      m_name = name;
    }

    public String getName() {
      return m_name;
    }

    protected void setName(String newName) {
      m_name = newName;
    }

    public boolean isNullString(String str) {
      return (str == null);
    }
    protected String over(String s) {
      return "parent.string";
    }
    private static String stat() {
      return "parent.static";
    }
  }

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static class MyMockChild extends MyMockParent {
    private int m_number;

    public MyMockChild(String name, int number) {
      super(name);
      m_number = number;
    }

    public int getNumber() {
      return m_number;
    }

    private void setNumber(Integer number) {
      m_number = number.intValue();
    }
    private void setData(String name, Integer number) {
      setName(name);
      m_number = number.intValue();
    }

    protected String over(String s) {
      return "child.string";
    }
    protected String over(Float s) {
      return "child.float";
    }
    protected String over(Number s) {
      return "child.number";
    }
    protected String over(Float f, Number n) {
      return "child.float.number";
    }
    protected String over(Number n, Float f) {
      return "child.number.float";
    }
    private static String stat() {
      return "child.static";
    }
  }

}

/**
 * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
 *             used by plugins); use
 *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
 */
@Deprecated
class ClassWithPrivateConstructor {
  private int n;

  private ClassWithPrivateConstructor() {
    n = 0;
  }

  private ClassWithPrivateConstructor(Integer i) {
    n = 1;
  }

  private ClassWithPrivateConstructor(Integer i, Super x) {
    n = 2;
  }

  private ClassWithPrivateConstructor(Integer i, Sub x) {
    n = 3;
  }

  public int getN() {
    return n;
  }

  public static class Super {
  }

  public static class Sub extends Super {
  }
}
