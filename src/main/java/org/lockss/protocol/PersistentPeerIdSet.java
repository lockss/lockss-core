/*
Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.lockss.app.LockssApp;

/* Note: This interface comes very close to 'implement Set<PeerIdentity>'.
 * However, it isn't an implementation.

 * Each of the set-like functions can throw an IOException.  Since this
 * exception is not part of the java.util.Set interface, it's not really
 * a Set.
 */


public interface PersistentPeerIdSet extends Iterable<PeerIdentity>  {
  public void store() throws IOException;
  public void store(boolean release) throws IOException;

  /* These methods are equivalents to the functions of java.util.Set. */
  public boolean add(PeerIdentity pi);
  public boolean addAll(Collection<? extends PeerIdentity> cpi);
  public void clear();
  public boolean contains(Object o);
  public boolean containsAll(Collection<?> co);
  public boolean equals(Object o);
  public int hashCode();
  public boolean isEmpty();
  public Iterator<PeerIdentity> iterator();
  public boolean remove(Object o);
  public boolean removeAll(Collection<?> c);
  public boolean retainAll(Collection<?> c);
  public int size();
  public Object[] toArray();
  // public <T> T[] toArray(T[] a);  // Reinsert if you use it.

  /**
   * Provides a serialized version of this entire object as a JSON string.
   * 
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  String toJson() throws IOException;

  /**
   * Provides a serialized version of this entire object as a JSON string.
   * 
   * @param peers
   *          A Set<PeerIdentity> with the peers to be included.
   * @return a String with this object serialized as a JSON string.
   * @throws IOException
   *           if any problem occurred during the serialization.
   */
  String toJson(Set<PeerIdentity> peers) throws IOException;

  /**
   * Provides the PeerIdentitys that are present in a serialized JSON string.
   * 
   * @param json
   *          A String with the JSON text.
   * @param app
   *          A LockssApp with the LOCKSS context.
   * @return a Set<PeerIdentity> that was updated from the JSON source.
   * @throws IOException
   *           if any problem occurred during the deserialization.
   */
  Set<PeerIdentity> updateFromJson(String json, LockssApp app)
      throws IOException;
}
