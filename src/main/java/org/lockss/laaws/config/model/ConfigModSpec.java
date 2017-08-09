/*

 Copyright (c) 2017 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.laaws.config.model;

import java.util.Objects;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The encapsulation of a set of configuration modifications.
 **/
public class ConfigModSpec   {
  private String header = null;

  private Map<String, String> updates = new HashMap<String, String>();

  private List<String> deletes = new ArrayList<String>();

  /**
   * A file header string
   * 
   * @return header
   **/
  public String getHeader() {
    return header;
  }

  public void setHeader(String header) {
    this.header = header;
  }

  /**
   * The map of configuration items that are modified.
   * 
   * @return updates
   **/
  public Map<String, String> getUpdates() {
    return updates;
  }

  public void setUpdates(Map<String, String> updates) {
    this.updates = updates;
  }

  /**
   * The set of configuration keys to be deleted.
   * 
   * @return deletes
   **/
  public List<String> getDeletes() {
    return deletes;
  }

  public void setDeletes(List<String> deletes) {
    this.deletes = deletes;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigModSpec configModSpec = (ConfigModSpec) o;
    return Objects.equals(this.header, configModSpec.header) &&
        Objects.equals(this.updates, configModSpec.updates) &&
        Objects.equals(this.deletes, configModSpec.deletes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(header, updates, deletes);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ConfigModSpec {\n");
    
    sb.append("    header: ").append(toIndentedString(header)).append("\n");
    sb.append("    updates: ").append(toIndentedString(updates)).append("\n");
    sb.append("    deletes: ").append(toIndentedString(deletes)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
