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
import java.util.*;
import java.lang.reflect.*;
import org.lockss.util.*;

/**
 * <code>PrivilegedAccessor</code> allows access to private or protected
 * constructors, methods and fields of other classes, for unit testing. It does
 * this using reflection. These accessors do not work with primitive types,
 * either as arguments or return values.<br>
 * There are several situations in which the compile-time type of an expression,
 * not the run-time class of an object, determines behavior:
 * <ol>
 * <li>References to static methods and fields are resolved based on the type of
 * the referring expression.</li>
 * <li>Overloaded methods are resolved based on the types of the argument
 * expressions.</li>
 * </ol>
 * In order for this facility to behave correctly in these cases, the type of
 * the expression must be explicitly supplied. Wherever these methods accept an
 * object, one may instead supply an instance of
 * <code>PrivilegedAccessor.Instance</code> that contains both the expression
 * type and value.
 * 
 * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
 *             used by plugins); use
 *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
 */
@Deprecated
public class PrivilegedAccessor {
//   static Logger log =
//     Logger.getLoggerWithInitialLevel("PrivAcc", Logger.LEVEL_DEBUG);

  /**
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  // no instances
  private PrivilegedAccessor() {
  }

  /**
   * Gets the value of the named field and returns it as an object.
   * @param instance the object instance
   * @param fieldName the name of the field
   * @return an object representing the value of the field
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object getValue(Object instance, String fieldName)
      throws IllegalAccessException, NoSuchFieldException {
    return org.lockss.util.test.PrivilegedAccessor.getValue(instance, fieldName);
  }

  /**
   * Calls a method on the given object instance, with no arguments.
   * @param instance the object instance
   * @param methodName the name of the method to invoke
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeMethod(Object instance, String methodName)
      throws NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     AmbiguousMethodException {
    try {
      return org.lockss.util.test.PrivilegedAccessor.invokeMethod(instance, methodName);
    }
    catch (org.lockss.util.test.PrivilegedAccessor.AmbiguousMethodException ame) {
      throw new AmbiguousMethodException(ame);
    }
  }

  /**
   * Calls a method on the given object instance with the given argument.
   * @param instance the object instance
   * @param methodName the name of the method to invoke
   * @param arg the argument to pass to the method
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeMethod(Object instance, String methodName,
				    Object arg)
      throws NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     AmbiguousMethodException {
    try {
      return org.lockss.util.test.PrivilegedAccessor.invokeMethod(instance, methodName, arg);
    }
    catch (org.lockss.util.test.PrivilegedAccessor.AmbiguousMethodException ame) {
      throw new AmbiguousMethodException(ame);
    }
  }

  /**
   * Calls a method on the given object instance with the given arguments.
   * @param instance the object instance
   * @param methodName the name of the method to invoke
   * @param args an array of objects to pass as arguments
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeMethod(Object instance, String methodName,
				    Object[] args)
      throws NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     AmbiguousMethodException {
    try {
      return org.lockss.util.test.PrivilegedAccessor.invokeMethod(instance, methodName, args);
    }
    catch (org.lockss.util.test.PrivilegedAccessor.AmbiguousMethodException ame) {
      throw new AmbiguousMethodException(ame);
    }
  }

  /**
   * Invokes the no-argument constructor for the named class
   * @param className the class name
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeConstructor(String className)
      throws ClassNotFoundException,
	     NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     InstantiationException {
    return org.lockss.util.test.PrivilegedAccessor.invokeConstructor(className);
  }

  /**
   * Invokes a one-argument constructor for the named class
   * @param className the class name
   * @param arg the argument to pass to the constructor
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeConstructor(String className, Object arg)
      throws ClassNotFoundException,
	     NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     InstantiationException {
    return org.lockss.util.test.PrivilegedAccessor.invokeConstructor(className, arg);
  }

  /**
   * Invokes a constructor for the named class
   * @param className the class name
   * @param args an array of objects to pass as arguments to the constructor
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeConstructor(String className, Object[] args)
      throws ClassNotFoundException,
	     NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     InstantiationException {
    return org.lockss.util.test.PrivilegedAccessor.invokeConstructor(className, args);
  }

  /**
   * Invokes the no-arg constructor for the specified class
   * @param cls the class
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeConstructor(Class cls)
      throws NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     InstantiationException {
    return org.lockss.util.test.PrivilegedAccessor.invokeConstructor(cls);
  }

  /**
   * Invokes a one-argument constructor for the specified class
   * @param cls the class
   * @param arg the argument to pass to the constructor
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeConstructor(Class cls, Object arg)
      throws NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     InstantiationException {
    return org.lockss.util.test.PrivilegedAccessor.invokeConstructor(cls, arg);
  }

  /**
   * Invokes a constructor for the specified class
   * @param cls the class
   * @param args an array of objects to pass as arguments to the constructor
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static Object invokeConstructor(Class cls, Object[] args)
      throws NoSuchMethodException,
	     IllegalAccessException,
	     InvocationTargetException,
	     InstantiationException {
    return org.lockss.util.test.PrivilegedAccessor.invokeConstructor(cls, args);
  }

  /** Exception thrown when an attempt is made to invoke an overloaded
   * constructor or method and there is no most specific applicable method.
   * (See section 15.11.2.2 of the Java Language Spec.)  Situations in
   * which this is thrown correspond to code that would cause a comple-time
   * error if attempted other than through reflection.
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static class AmbiguousMethodException extends org.lockss.util.test.PrivilegedAccessor.AmbiguousMethodException {
    /**
     * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
     *             used by plugins); use
     *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
     */
    @Deprecated
    public AmbiguousMethodException(String msg) {
      super(msg);
    }
    public AmbiguousMethodException(org.lockss.util.test.PrivilegedAccessor.AmbiguousMethodException ame) {
      this(ame.getMessage());
    }
  }

  /** <code>PrivilegedAccessor.Instance</code> is used when the difference
   * between an object's class and the type of an expression is important,
   * <i>eg</i>, to simulate compile-type decisions about on what class to
   * reference a static field, or which of several overloaded methods to
   * invoke.  (Null argumants are an instances of the latter.  This wrapper
   * is needed for null values only when the method is overloaded.)
   * 
   * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
   *             used by plugins); use
   *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
   */
  @Deprecated
  public static class Instance extends org.lockss.util.test.PrivilegedAccessor.Instance {

    @Deprecated
    private Object value;
    
    @Deprecated
    private Class cls;

    @Deprecated
    private Instance() {

    }

    /**
     * Create an object that, when passed to the accessors in
     * <code>PrivilegedAccessor</code>, acts like an expression of type
     * <i>cls</i> for purposes of class and method lookup, but whose value
     * is actually <i>value</i>.  <i>value</i> must be either null or of a
     * type assignable to <i>class</i>.
     * 
     * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
     *             used by plugins); use
     *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
     */
    @Deprecated
    public Instance(Class cls, Object value) {
      super(cls, value);
    }

    /**
     * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
     *             used by plugins); use
     *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
     */
    @Deprecated
    Class getInstanceClass() {
      return cls;
    }

    /**
     * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
     *             used by plugins); use
     *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
     */
    @Deprecated
    Object getValue() {
      return value;
    }

    /**
     * @deprecated {@code org.lockss.test.PrivilegedAccessor} is deprecated (but is
     *             used by plugins); use
     *             {@code org.lockss.util.test.PrivilegedAccessor} instead.
     */
    @Deprecated
    public String toString() {
      return "[Priv.Inst: " + StringUtil.shortName(cls) + ", " + value + "]";
    }

  }

}
