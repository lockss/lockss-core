/*

 Copyright (c) 2017-2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.laaws.rs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

public class OldArtifactPage {
  @JsonProperty("next")
  private String next = null;

  @JsonProperty("prev")
  private String prev = null;

  @JsonProperty("page")
  private Integer page = null;

  @JsonProperty("results")
  private Integer results = null;

  @JsonProperty("items")
  private List<OldArtifact> items = null;

  public OldArtifactPage next(String next) {
    this.next = next;
    return this;
  }

  /**
   * Get next
   * 
   * @return next
   */
  public String getNext() {
    return next;
  }

  public void setNext(String next) {
    this.next = next;
  }

  public OldArtifactPage prev(String prev) {
    this.prev = prev;
    return this;
  }

  /**
   * Get prev
   * 
   * @return prev
   */
  public String getPrev() {
    return prev;
  }

  public void setPrev(String prev) {
    this.prev = prev;
  }

  public OldArtifactPage page(Integer page) {
    this.page = page;
    return this;
  }

  /**
   * Get page
   * 
   * @return page
   */
  public Integer getPage() {
    return page;
  }

  public void setPage(Integer page) {
    this.page = page;
  }

  public OldArtifactPage results(Integer results) {
    this.results = results;
    return this;
  }

  /**
   * Get results
   * 
   * @return results
   */
  public Integer getResults() {
    return results;
  }

  public void setResults(Integer results) {
    this.results = results;
  }

  public OldArtifactPage items(List<OldArtifact> items) {
    this.items = items;
    return this;
  }

  /**
   * Get items
   * 
   * @return items
   */
  public List<OldArtifact> getItems() {
    return items;
  }

  public void setItems(List<OldArtifact> items) {
    this.items = items;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OldArtifactPage artifactPage = (OldArtifactPage) o;
    return Objects.equals(next, artifactPage.next)
	&& Objects.equals(prev, artifactPage.prev)
	&& Objects.equals(page, artifactPage.page)
	&& Objects.equals(results, artifactPage.results)
	&& Objects.equals(items, artifactPage.items);
  }

  @Override
  public int hashCode() {
    return Objects.hash(next, prev, page, results, items);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class OldArtifactPage {\n");

    sb.append("    next: ").append(toIndentedString(next)).append("\n");
    sb.append("    prev: ").append(toIndentedString(prev)).append("\n");
    sb.append("    page: ").append(toIndentedString(page)).append("\n");
    sb.append("    results: ").append(toIndentedString(results)).append("\n");
    sb.append("    items: ").append(toIndentedString(items)).append("\n");
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
