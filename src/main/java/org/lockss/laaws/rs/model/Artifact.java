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
package org.lockss.laaws.rs.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Artifact {
  @JsonProperty("id")
  private String id = null;

  private String repository = null;

  @JsonProperty("auid")
  private String auid = null;

  @JsonProperty("uri")
  private String uri = null;

  @JsonProperty("aspect")
  private String aspect = null;

  @JsonProperty("acquired")
  private Integer acquired = null;

  @JsonProperty("committed")
  private Boolean committed = null;

  @JsonProperty("content_hash")
  private String contentHash = null;

  @JsonProperty("metadata_hash")
  private String metadataHash = null;

  @JsonProperty("content_length")
  private Integer contentLength = null;

  @JsonProperty("content_datetime")
  private Integer contentDatetime = null;

  private String path;

  private long offset;

  /**
   * Get id
   *
   * @return id
   */
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Artifact auid(String auid) {
    this.auid = auid;
    return this;
  }

  /**
   * Get auid
   *
   * @return auid
   */
  public String getAuid() {
    return auid;
  }

  public void setAuid(String auid) {
    this.auid = auid;
  }

  public Artifact uri(String uri) {
    this.uri = uri;
    return this;
  }

  /**
   * Get uri
   *
   * @return uri
   */
  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public Artifact aspect(String aspect) {
    this.aspect = aspect;
    return this;
  }

  /**
   * Get aspect
   *
   * @return aspect
   */
  public String getAspect() {
    return aspect;
  }

  public void setAspect(String aspect) {
    this.aspect = aspect;
  }

  public Artifact acquired(Integer acquired) {
    this.acquired = acquired;
    return this;
  }

  /**
   * Get acquired
   *
   * @return acquired
   */
  public Integer getAcquired() {
    return acquired;
  }

  public void setAcquired(Integer acquired) {
    this.acquired = acquired;
  }

  public Artifact committed(Boolean committed) {
    this.committed = committed;
    return this;
  }

  /**
   * Get committed
   *
   * @return committed
   */
  public Boolean getCommitted() {
    return committed;
  }

  public void setCommitted(Boolean committed) {
    this.committed = committed;
  }

  public Artifact contentHash(String contentHash) {
    this.contentHash = contentHash;
    return this;
  }

  /**
   * Get contentHash
   *
   * @return contentHash
   */
  public String getContentHash() {
    return contentHash;
  }

  public void setContentHash(String contentHash) {
    this.contentHash = contentHash;
  }

  public Artifact metadataHash(String metadataHash) {
    this.metadataHash = metadataHash;
    return this;
  }

  /**
   * Get metadataHash
   *
   * @return metadataHash
   */
  public String getMetadataHash() {
    return metadataHash;
  }

  public void setMetadataHash(String metadataHash) {
    this.metadataHash = metadataHash;
  }

  public Artifact contentLength(Integer contentLength) {
    this.contentLength = contentLength;
    return this;
  }

  /**
   * Get contentLength
   *
   * @return contentLength
   */
  public Integer getContentLength() {
    return contentLength;
  }

  public void setContentLength(Integer contentLength) {
    this.contentLength = contentLength;
  }

  public Artifact contentDatetime(Integer contentDatetime) {
    this.contentDatetime = contentDatetime;
    return this;
  }

  /**
   * Get contentDatetime
   *
   * @return contentDatetime
   */
  public Integer getContentDatetime() {
    return contentDatetime;
  }

  public void setContentDatetime(Integer contentDatetime) {
    this.contentDatetime = contentDatetime;
  }

  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Artifact artifact = (Artifact) o;
    return Objects.equals(this.id, artifact.id)
	&& Objects.equals(this.auid, artifact.auid)
	&& Objects.equals(this.uri, artifact.uri)
	&& Objects.equals(this.aspect, artifact.aspect)
	&& Objects.equals(this.acquired, artifact.acquired)
	&& Objects.equals(this.committed, artifact.committed)
	&& Objects.equals(this.contentHash, artifact.contentHash)
	&& Objects.equals(this.metadataHash, artifact.metadataHash)
	&& Objects.equals(this.contentLength, artifact.contentLength)
	&& Objects.equals(this.contentDatetime, artifact.contentDatetime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, auid, uri, aspect, acquired, committed, contentHash,
	metadataHash, contentLength, contentDatetime);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class Artifact {\n");

    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    auid: ").append(toIndentedString(auid)).append("\n");
    sb.append("    uri: ").append(toIndentedString(uri)).append("\n");
    sb.append("    aspect: ").append(toIndentedString(aspect)).append("\n");
    sb.append("    acquired: ").append(toIndentedString(acquired)).append("\n");
    sb.append("    committed: ").append(toIndentedString(committed))
	.append("\n");
    sb.append("    contentHash: ").append(toIndentedString(contentHash))
	.append("\n");
    sb.append("    metadataHash: ").append(toIndentedString(metadataHash))
	.append("\n");
    sb.append("    contentLength: ").append(toIndentedString(contentLength))
	.append("\n");
    sb.append("    contentDatetime: ").append(toIndentedString(contentDatetime))
	.append("\n");
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

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getRepository() {
    return repository;
  }

  public void setRepository(String repository) {
    this.repository = repository;
  }
}
