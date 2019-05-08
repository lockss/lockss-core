/*

Copyright (c) 2017-2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

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
package org.lockss.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The metadata generated for a single item.
 */
public class ItemMetadata {
  private Long id = null;
  private Map<String, String> scalarMap = new HashMap<String, String>();
  private Map<String, Set<String>> setMap = new HashMap<String, Set<String>>();

  private Map<String, List<String>> listMap =
      new HashMap<String, List<String>>();

  private Map<String, Map<String, String>> mapMap =
      new HashMap<String, Map<String, String>>();

  /**
   * Default constructor.
   */
  public ItemMetadata() {

  }

  /**
   * Constructor with identifier.
   */
  public ItemMetadata(Long id) {
    super();
    this.id = id;
  }

  /**
   * The identifier for this item.
   * 
   * @return id
   **/
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  /**
   * The map of scalar metadata elements for this item.
   * 
   * @return scalarMap
   **/
  public Map<String, String> getScalarMap() {
    return scalarMap;
  }

  public void setScalarMap(Map<String, String> scalarMap) {
    this.scalarMap = scalarMap;
  }

  /**
   * The map of set-bound metadata elements for this item.
   * 
   * @return setMap
   **/
  public Map<String, Set<String>> getSetMap() {
    return setMap;
  }

  public void setSetMap(Map<String, Set<String>> setMap) {
    this.setMap = setMap;
  }

  /**
   * The map of listed metadata elements for this item.
   * 
   * @return listMap
   **/
  public Map<String, List<String>> getListMap() {
    return listMap;
  }

  public void setListMap(Map<String, List<String>> listMap) {
    this.listMap = listMap;
  }

  /**
   * The map of mapped metadata elements for this item.
   * 
   * @return mapMap
   **/
  public Map<String, Map<String, String>> getMapMap() {
    return mapMap;
  }

  public void setMapMap(Map<String, Map<String, String>> mapMap) {
    this.mapMap = mapMap;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ItemMetadata itemMetadata = (ItemMetadata) o;
    return Objects.equals(this.id, itemMetadata.id) &&
	Objects.equals(this.scalarMap, itemMetadata.scalarMap) &&
        Objects.equals(this.setMap, itemMetadata.setMap) &&
        Objects.equals(this.listMap, itemMetadata.listMap) &&
        Objects.equals(this.mapMap, itemMetadata.mapMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, scalarMap, setMap, listMap, mapMap);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ItemMetadata {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    scalarMap: ").append(toIndentedString(scalarMap))
    .append("\n");
    sb.append("    setMap: ").append(toIndentedString(setMap)).append("\n");
    sb.append("    listMap: ").append(toIndentedString(listMap)).append("\n");
    sb.append("    mapMap: ").append(toIndentedString(mapMap)).append("\n");
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
