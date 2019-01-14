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

package org.lockss.util;

import java.util.*;
import org.apache.commons.collections4.map.*;

/**
 * An LRU cache that guarantees to hold and return a reference to an object
 * as long as anyone else does.  Intended to cache objects for which
 * multiple instances per key must not exist.  This cache is synchronized.
 */
public class UniqueRefLruCache<K,V> implements Map<K,V> {
  LRUMap<K,V> lruMap;
  ReferenceMap<K,V> refMap;

  // logging variables
  private int cacheHits = 0;
  private int cacheMisses = 0;
  private int refHits = 0;
  private int refMisses = 0;

  /**
   * Standard constructor.  Size must be positive.
   * @param maxSize maximum size for the LRUMap
   */
  public UniqueRefLruCache(int maxSize) {
    if (maxSize<=0) {
      throw new IllegalArgumentException("Negative cache size");
    }
    lruMap = new LRUMap<>(maxSize);
    refMap = new ReferenceMap<>(AbstractReferenceMap.ReferenceStrength.HARD,
				AbstractReferenceMap.ReferenceStrength.WEAK);
  }

  /**
   * Returns the max cache size.
   * @return the max size
   */
  public synchronized int getMaxSize() {
    return lruMap.maxSize();
  }

  /**
   * Sets the NodeState cache size.
   * @param newSize the new size
   */
  public synchronized void setMaxSize(int newSize) {
    if (newSize<=0) {
      throw new IllegalArgumentException("Negative cache size");
    }
    if (lruMap.maxSize() != newSize) {
      LRUMap<K,V> newMap = new LRUMap<>(newSize);
      newMap.putAll(lruMap);
      lruMap = newMap;
    }
  }

  /**
   * Get an object from the cache.
   * @param key the key
   * @return the corresponding object, or null if no such object exists in
   * memory.
   */
  public synchronized V get(Object key) {
    // first check the LRUMap
    V obj = lruMap.get(key);
    if (obj!=null) {
      cacheHits++;
      return obj;
    } else {
      cacheMisses++;
      // then check the refMap to see if one is still in use
      obj = refMap.get(key);
      if (obj!=null) {
        refHits++;
        // if found, put back in LRUMap
        lruMap.put((K)key, obj);
        return obj;
      } else {
        refMisses++;
        return null;
      }
    }
  }

  /**
   * Put an object in the cache.
   * @param key the key
   * @param obj the Object
   */
  public synchronized V put(K key, V obj) {
    V res = refMap.get(key);
    refMap.put(key, obj);
    lruMap.put(key, obj);
    return res;
  }

  /**
   * Put an object in the cache, only if the key wasn't already associated
   * with a value, returns the existing value or the new value if none
   * existing.
   * @param key the key
   * @param obj the Object
   * @return the value now in the map
   */
  public synchronized Object putIfNew(K key, V obj) {
    Object val = get(key);
    if (val != null) {
      return val;
    }
    put(key, obj);
    return obj;
  }

  public synchronized V remove(Object key) {
    V res = refMap.get(key);
    refMap.remove(key);
    lruMap.remove(key);
    return res;
  }

  public boolean isEmpty() {
    return !refMap.isEmpty();
  }

  public int size() {
    return refMap.size();
  }

  public Set<Entry<K,V>> entrySet() {
    throw new UnsupportedOperationException();
  }

  public Set<K> keySet() {
    throw new UnsupportedOperationException();
  }

  public Set<V> values() {
    throw new UnsupportedOperationException();
  }

  public void putAll(Map<? extends K,? extends V> m) {
    throw new UnsupportedOperationException();
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  public boolean containsKey(Object key) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a snapshot of the values in the cache
   * @return a Set of Objects
   */
  public synchronized Set snapshot() {
    return new HashSet(lruMap.values());
  }

  /**
   * Clears the cache
   */
  public synchronized void clear() {
    lruMap.clear();
    refMap.clear();
  }

  // logging accessors

  public int getCacheHits() { return cacheHits; }
  public int getCacheMisses() { return cacheMisses; }
  public int getRefHits() { return refHits; }
  public int getRefMisses() { return refMisses; }
}
