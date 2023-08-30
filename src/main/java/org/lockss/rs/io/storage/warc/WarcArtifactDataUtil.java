package org.lockss.rs.io.storage.warc;

import org.apache.commons.lang3.StringUtils;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcRecord;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.util.ArtifactConstants;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WarcArtifactDataUtil {
  /**
   * Constructs an {@link Artifact} from a {@link ArtifactData}.
   */
  public static Artifact getArtifact(ArtifactData ad) {

    Artifact artifact = new Artifact(
        ad.getIdentifier(),
        // TODO ad.isCommitted(),
        false,
        ad.getStorageUrl() != null ? ad.getStorageUrl().toString() : null,
        ad.getContentLength(),
        ad.getContentDigest());

    artifact.setCollectionDate(ad.getCollectionDate());

    return artifact;
  }

  /**
   * Instantiates an {@code ArtifactIdentifier} from headers in a ARC / WARC {@code ArchiveRecordHeader} object.
   *
   * @param headers An {@code ArchiveRecordHeader} ARC / WARC header containing an artifact identity.
   * @return An {@code ArtifactIdentifier}.
   */
  public static ArtifactIdentifier buildArtifactIdentifier(ArchiveRecordHeader headers) {
    int version = 0;

    String versionHeader = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_VERSION_KEY);
    if (!StringUtils.isEmpty(versionHeader)) {
      version = Integer.parseInt(versionHeader);
    }

    String namespace = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_NAMESPACE_KEY);
    if (StringUtils.isEmpty(namespace)) {
      namespace = (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_COLLECTION_KEY);
    }

    return new ArtifactIdentifier(
        parseWarcRecordIdForUUID((String)headers.getHeaderValue(WARCConstants.HEADER_KEY_ID)),
        namespace,
        (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_AUID_KEY),
        // Q: Use (String)headers.getHeaderValue(WARCConstants.HEADER_KEY_URI)?
        (String) headers.getHeaderValue(ArtifactConstants.ARTIFACT_URI_KEY),
        version);
  }

  public static ArtifactIdentifier buildArtifactIdentifier(WarcRecord record) {
    int version = 0;
    String versionVal = getHeadValueOrNull(record.getHeader(ArtifactConstants.ARTIFACT_VERSION_KEY));
    if (!StringUtils.isEmpty(versionVal)) {
      version = Integer.parseInt(versionVal);
    }

    String namespace = getHeadValueOrNull(record.getHeader(ArtifactConstants.ARTIFACT_NAMESPACE_KEY));
    if (StringUtils.isEmpty(namespace)) {
      namespace = getHeadValueOrNull(record.getHeader(ArtifactConstants.ARTIFACT_COLLECTION_KEY));
    }

    return new ArtifactIdentifier(
        parseWarcRecordIdForUUID(getHeadValueOrNull(record.getHeader(WARCConstants.HEADER_KEY_ID))),
        namespace,
        getHeadValueOrNull(record.getHeader(ArtifactConstants.ARTIFACT_AUID_KEY)),
        // Q: Use (String)headers.getHeaderValue(WARCConstants.HEADER_KEY_URI)?
        getHeadValueOrNull(record.getHeader(ArtifactConstants.ARTIFACT_URI_KEY)),
        version);
  }

  private static String getHeadValueOrNull(HeaderLine headerLine) {
    return headerLine == null ? null : headerLine.value;
  }

  private final static Pattern uuidPattern = Pattern.compile("<urn:uuid:(.+)>");

  public static String parseWarcRecordIdForUUID(String recordId) {
    Matcher result = uuidPattern.matcher(recordId);

    if (result.matches()) {
      // Validate UUID class
      return UUID.fromString(result.group(1)).toString();
    }

    throw new IllegalArgumentException("Unexpected WARC-Record-ID: " + recordId);
  }
}
