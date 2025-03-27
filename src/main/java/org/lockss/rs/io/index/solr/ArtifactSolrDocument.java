package org.lockss.rs.io.index.solr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.solr.client.solrj.beans.Field;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;

import static org.lockss.util.rest.repo.model.Artifact.*;

public class ArtifactSolrDocument {
  // We have chosen to map the artifact UUID to the Solr document's "id" field
  // for the sake of convention, even though Solr appears to support assigning
  // another field as the unique identifier.
  @Field("id")
  private String uuid;

  @Field(ARTIFACT_NAMESPACE_KEY)
  private String namespace = "lockss";

  @Field(ARTIFACT_AUID_KEY)
  private String auid;

  @Field(ARTIFACT_URI_KEY)
  private String uri;

  @Field("sortUri")
  private String sortUri;

  @Field(ARTIFACT_VERSION_KEY)
  private Integer version;

  @Field(ARTIFACT_COMMITTED_STATUS_KEY)
  private Boolean committed;

  @Field("storageUrl")
  private String storageUrl;

  @Field(ARTIFACT_LENGTH_KEY)
  private long contentLength;

  @Field(ARTIFACT_DIGEST_KEY)
  private String contentDigest;

  @Field(ARTIFACT_COLLECTION_DATE_KEY)
  private long collectionDate;

  public static ArtifactSolrDocument fromArtifact(Artifact artifact) {
    ArtifactSolrDocument doc = new ArtifactSolrDocument();
    doc.setUuid(artifact.getUuid());
    doc.setNamespace(artifact.getNamespace());
    doc.setAuid(artifact.getAuid());
    doc.setUri(artifact.getUri());
    doc.setSortUri(makeSortUri(artifact.getUri()));
    doc.setVersion(artifact.getVersion());
    doc.setCommitted(artifact.getCommitted());
    doc.setStorageUrl(artifact.getStorageUrl());
    doc.setContentLength(artifact.getContentLength());
    doc.setContentDigest(artifact.getContentDigest());
    doc.setCollectionDate(artifact.getCollectionDate());
    return doc;
  }

  public Artifact toArtifact() {
    Artifact artifact = new Artifact();
    artifact.setUuid(getUuid());
    artifact.setNamespace(getNamespace());
    artifact.setAuid(getAuid());
    artifact.setUri(getUri());
    artifact.setVersion(getVersion());
    artifact.setCommitted(getCommitted());
    artifact.setStorageUrl(getStorageUrl());
    artifact.setContentLength(getContentLength());
    artifact.setContentDigest(getContentDigest());
    artifact.setCollectionDate(getCollectionDate());
    return artifact;
  }

  public static String makeSortUri(String uri) {
    return uri.replaceAll("/", "\u0000");
  }

  /**
   * Constructor. Needed by SolrJ for getBeans() support. *
   *
   * TODO: Reconcile difference with constructor below, which checks parameters for illegal arguments.
   */
  public ArtifactSolrDocument() {
    // Intentionally left blank
  }

  public ArtifactSolrDocument(ArtifactIdentifier aid, Boolean committed, String storageUrl, long contentLength, String contentDigest) {
    this(
        aid.getUuid(), aid.getNamespace(), aid.getAuid(), aid.getUri(), aid.getVersion(),
        committed,
        storageUrl,
        contentLength,
        contentDigest
    );
  }

  public ArtifactSolrDocument(String uuid, String namespace, String auid, String uri, Integer version,
                              Boolean committed,
                  String storageUrl, long contentLength, String contentDigest) {
    if (StringUtil.isNullString(uuid)) {
      throw new IllegalArgumentException(
          "Cannot create Artifact with null or empty UUID");
    }
    this.uuid = uuid;

    if (StringUtil.isNullString(namespace)) {
      throw new IllegalArgumentException(
          "Cannot create Artifact with null or empty namespace");
    }
    this.namespace = namespace;

    if (StringUtil.isNullString(auid)) {
      throw new IllegalArgumentException(
          "Cannot create Artifact with null or empty auid");
    }
    this.auid = auid;

    if (StringUtil.isNullString(uri)) {
      throw new IllegalArgumentException(
          "Cannot create Artifact with null or empty URI");
    }
    this.setUri(uri);

    if (version == null) {
      throw new IllegalArgumentException(
          "Cannot create Artifact with null version");
    }
    this.version = version;

    if (committed == null) {
      throw new IllegalArgumentException(
          "Cannot create Artifact with null commit status");
    }
    this.committed = committed;

    if (StringUtil.isNullString(storageUrl)) {
      throw new IllegalArgumentException("Cannot create "
          + "Artifact with null or empty storageUrl");
    }
    this.storageUrl = storageUrl;

    this.contentLength = contentLength;
    this.contentDigest = contentDigest;
  }

  @JsonIgnore
  public ArtifactIdentifier getIdentifier() {
    return new ArtifactIdentifier(uuid, namespace, auid, uri, version);
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    if (StringUtil.isNullString(namespace)) {
      throw new IllegalArgumentException(
          "Cannot set null or empty namespace");
    }
    this.namespace = namespace;
  }

  public String getAuid() {
    return auid;
  }

  public void setAuid(String auid) {
    if (StringUtil.isNullString(auid)) {
      throw new IllegalArgumentException("Cannot set null or empty auid");
    }
    this.auid = auid;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    if (StringUtil.isNullString(uri)) {
      throw new IllegalArgumentException("Cannot set null or empty URI");
    }
    this.uri = uri;
    this.setSortUri(makeSortUri(uri));
  }

  public String getSortUri() {
    if ((sortUri == null) && (uri != null)) {
      this.setSortUri(makeSortUri(uri));
    }
    return sortUri;
  }

  public void setSortUri(String sortUri) {
    if (StringUtil.isNullString(sortUri)) {
      throw new IllegalArgumentException("Cannot set null or empty SortURI");
    }
    this.sortUri = sortUri;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
//        if (StringUtils.isEmpty(version)) {
//          throw new IllegalArgumentException(
//              "Cannot set null or empty version");
//        }
    if (version == null) {
      throw new IllegalArgumentException("Cannot set null version");
    }

    this.version = version;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getUuid() {
    return uuid;
  }

  public Boolean getCommitted() {
    return committed;
  }

  public boolean isCommitted() {
    return getCommitted();
  }

  public void setCommitted(Boolean committed) {
    if (committed == null) {
      throw new IllegalArgumentException("Cannot set null commit status");
    }
    this.committed = committed;
  }

  public String getStorageUrl() {
    return storageUrl;
  }

  public void setStorageUrl(String storageUrl) {
    if (StringUtil.isNullString(storageUrl)) {
      throw new IllegalArgumentException(
          "Cannot set null or empty storageUrl");
    }
    this.storageUrl = storageUrl;
  }

  public long getContentLength() {
    return contentLength;
  }

  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  public String getContentDigest() {
    return contentDigest;
  }

  public void setContentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
  }

  /**
   * Provides the artifact collection date.
   *
   * @return a long with the artifact collection date in milliseconds since the
   *         epoch.
   */
  public long getCollectionDate() {
    return collectionDate;
  }

  /**
   * Saves the artifact collection date.
   *
   * @param collectionDate
   *          A long with the artifact collection date in milliseconds since the
   *          epoch.
   */
  public void setCollectionDate(long collectionDate) {
    this.collectionDate = collectionDate;
  }

  @Override
  public String toString() {
    return "Artifact{" +
        "uuid='" + uuid + '\'' +
        ", namespace='" + namespace + '\'' +
        ", auid='" + auid + '\'' +
        ", uri='" + uri + '\'' +
//                 ", sortUri='" + sortUri + '\'' +
        ", version='" + version + '\'' +
        ", committed=" + committed +
        ", storageUrl='" + storageUrl + '\'' +
        ", contentLength='" + contentLength + '\'' +
        ", contentDigest='" + contentDigest + '\'' +
        ", collectionDate='" + collectionDate + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    // Cast to Artifact is safe because equalsExceptStorageUrl has
    // already checked instanceof
    return equalsExceptStorageUrl(o)
        && storageUrl.equalsIgnoreCase(((Artifact)o).getStorageUrl());
  }

  public boolean equalsExceptStorageUrl(Object o) {
    if (!(o instanceof Artifact other)) {
      return false;
    }

    return other != null
        && ((this.getIdentifier() == null && other.getIdentifier() == null)
        || (this.getIdentifier() != null && this.getIdentifier().equals(other.getIdentifier())))
        && committed.equals(other.getCommitted())
        && getContentLength() == other.getContentLength()
        && ((contentDigest == null && other.getContentDigest() == null)
        || (contentDigest != null && contentDigest.equals(other.getContentDigest())))
        && getCollectionDate() == other.getCollectionDate();
  }

}
