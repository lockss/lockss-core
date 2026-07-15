package org.lockss.rs.io.storage.warc;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.util.ArtifactConstants;
import org.lockss.util.rest.repo.util.ArtifactDataUtil;
import org.springframework.http.HttpHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WarcArtifactDataUtil {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Constructs an {@link Artifact} from a {@link ArtifactData}.
   */
  public static Artifact getArtifact(ArtifactData ad) {

    Artifact artifact = new Artifact(
        ad.getIdentifier(),
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

  public static ArtifactData fromWarcRecord(WarcRecord record) throws IOException {
    // Get WARC record header
    List<HeaderLine> headersList = record.getHeaderList();

    Map<String, String> recordHeaders = new HashMap<>();
    for (HeaderLine hl: record.getHeaderList()) {
      recordHeaders.put(hl.name, hl.value);
    }

    ArtifactData ad;

    // Read ArtifactIdentifier from the WARC record headers
    ArtifactIdentifier artifactId = buildArtifactIdentifier(record);

    // Read WARC record type from record headers
    WARCConstants.WARCRecordType recordType =
        WARCConstants.WARCRecordType.valueOf(recordHeaders.get(WARCConstants.HEADER_KEY_TYPE));

    String mimetype = recordHeaders.get(WARCConstants.CONTENT_TYPE);

    // Artifacts can only be read out of WARC response and resource type records
    switch (recordType) {
      case response:
        // Sanity check
        if (mimetype == null || !mimetype.startsWith("application/http")) {
          log.warn("Unexpected content MIME type WARC response record: {}", mimetype);
          throw new IllegalStateException("Invalid MIME type: " + mimetype);
        }

        // Parse the ArchiveRecord into an ArtifactData
//        ad = ArtifactDataFactory.fromHttpResponseStream(record.getPayload().getInputStreamComplete()); // FIXME
        ad = fromHttpResponseWarcRecord(record);
        ad.setIdentifier(artifactId);
        break;

      case resource:
        // Parse the ArchiveRecord into an ArtifactData
        ad = fromResource(record.getPayloadContent());
        ad.setIdentifier(artifactId);
        HttpHeaders artifactHeaders = ad.getHttpHeaders();

        // Set the ArtifactData content-type to that of the WARC record block if present
        if (!StringUtil.isNullString(mimetype)) {
          artifactHeaders.set(HttpHeaders.CONTENT_TYPE, mimetype);
        }

        // Set Content-Length from WARC record (mandatory field)
        artifactHeaders.setContentLength(Long.parseLong(recordHeaders.get("Content-Length")));

        break;

      default:
        log.warn("Unexpected WARC record type [WARC-Record-ID: {}, WARC-Type: {}]",
            recordHeaders.get(WARCConstants.HEADER_KEY_ID), recordType);

        // Could not return an artifact elsewhere
        return null;
    }

    String artifactContentLength = recordHeaders.get(ArtifactConstants.ARTIFACT_LENGTH_KEY);
    log.trace("artifactContentLength = {}", artifactContentLength);
    if (!StringUtil.isNullString(artifactContentLength)) {
      ad.setContentLength(Long.parseLong(artifactContentLength));
    }

    String artifactDigest = recordHeaders.get(ArtifactConstants.ARTIFACT_DIGEST_KEY);
    log.trace("artifactDigest = {}", artifactDigest);
    if (!StringUtil.isNullString(artifactDigest)) {
      ad.setContentDigest(artifactDigest);
    }

    String artifactStoreDate = recordHeaders.get(ArtifactConstants.ARTIFACT_STORE_DATE_KEY);
    log.trace("artifactStoreDate = {}", artifactStoreDate);
    if (!StringUtil.isNullString(artifactStoreDate)) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactStoreDate);
      ad.setStoreDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    String artifactCollectionDate = recordHeaders.get(WARCConstants.HEADER_KEY_DATE);
    log.trace("artifactCollectionDate = {}", artifactCollectionDate);
    if (!StringUtil.isNullString(artifactCollectionDate)) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactCollectionDate);
      ad.setCollectionDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    return ad;
  }

  /**
   * Instantiates an {@code ArtifactData} from an arbitrary byte stream in an {@code InputStream}.
   * <p>
   * Uses a default HTTP response status of HTTP/1.1 200 OK.
   *
   * @param resourceStream An {@code InputStream} containing the byte stream to instantiate an {@code ArtifactData} from.
   * @return An {@code ArtifactData} wrapping the byte stream.
   */
  public static ArtifactData fromResource(InputStream resourceStream) {
    return fromResourceStream(null, resourceStream);
  }

  /**
   * Constructs an {@link ArtifactData} from an arbitrary byte stream in an {@link InputStream}.
   *
   * @param headers       A Spring {@link HttpHeaders} object containing optional artifact headers.
   * @param resourceStream An {@link InputStream} containing an arbitrary byte stream.
   * @return An {@link ArtifactData} wrapping the byte stream.
   */
  public static ArtifactData fromResourceStream(HttpHeaders headers, InputStream resourceStream) {
    return fromResourceStream(headers, resourceStream, null);
  }

  /**
   * Instantiates an {@code ArtifactData} from an arbitrary byte stream in an {@code InputStream}.
   * <p>
   * Takes a {@code StatusLine} containing the HTTP response status associated with this byte stream.
   *
   * @param headers       A Spring {@code HttpHeaders} object containing optional artifact headers.
   * @param resourceStream An {@code InputStream} containing an arbitrary byte stream.
   * @param responseStatus
   * @return An {@code ArtifactData} wrapping the byte stream.
   */
  public static ArtifactData fromResourceStream(HttpHeaders headers, InputStream resourceStream,
                                                 StatusLine responseStatus) {
    return new ArtifactData(headers, resourceStream, responseStatus);
  }

  /**
   * Instantiates an {@code ArtifactData} from an ARC / WARC {@code ArchiveRecord} object containing an artifact.
   *
   * @param record An {@code ArchiveRecord} object containing an artifact.
   * @return An {@code ArtifactData} representing the artifact contained in the {@code ArchiveRecord}.
   * @throws IOException
   */
  public static ArtifactData fromArchiveRecord(ArchiveRecord record) throws IOException {
    // Get WARC record header
    ArchiveRecordHeader recordHeaders = record.getHeader();

    ArtifactData ad;

    // Read ArtifactIdentifier from the WARC record headers
    ArtifactIdentifier artifactId = buildArtifactIdentifier(recordHeaders);

    // Read WARC record type from record headers
    WARCConstants.WARCRecordType recordType =
        WARCConstants.WARCRecordType.valueOf((String) recordHeaders.getHeaderValue(WARCConstants.HEADER_KEY_TYPE));

    // Artifacts can only be read out of WARC response and resource type records
    switch (recordType) {
      case response:
        // Sanity check
        String mimeType = recordHeaders.getMimetype();
        if (!mimeType.startsWith("application/http")) {
          log.warn("Unexpected content MIME type WARC response record: {}", mimeType);
          throw new IllegalStateException("Invalid MIME type: " + mimeType);
        }

        // Attach WARC record block (HTTP response) stream without parsing it
        ad = new ArtifactData()
            .setResponseInputStream(record)
            .setIdentifier(artifactId);

        break;

      case resource:
        // Parse the ArchiveRecord into an ArtifactData
        ad = fromResource(record);
        ad.setIdentifier(artifactId);
        HttpHeaders artifactHeaders = ad.getHttpHeaders();

        // Set the ArtifactData content-type to that of the WARC record block if present
        String mimetype = recordHeaders.getMimetype();
        if (!StringUtil.isNullString(mimetype)) {
          artifactHeaders.set(HttpHeaders.CONTENT_TYPE, mimetype);
        }

        // Set Content-Length from WARC record (mandatory field)
        artifactHeaders.setContentLength(recordHeaders.getContentLength());
        break;

      default:
        log.warn("Unexpected WARC record type [WARC-Record-ID: {}, WARC-Type: {}]",
            recordHeaders.getHeaderValue(WARCConstants.HEADER_KEY_ID), recordType);

        // Could not return an artifact elsewhere
        return null;
    }

    String artifactContentLength = (String) recordHeaders.getHeaderValue(ArtifactConstants.ARTIFACT_LENGTH_KEY);
    log.trace("artifactContentLength = {}", artifactContentLength);
    if (!StringUtil.isNullString(artifactContentLength)) {
      ad.setContentLength(Long.parseLong(artifactContentLength));
    }

    String artifactDigest = (String) recordHeaders.getHeaderValue(ArtifactConstants.ARTIFACT_DIGEST_KEY);
    log.trace("artifactDigest = {}", artifactDigest);
    if (!StringUtil.isNullString(artifactDigest)) {
      ad.setContentDigest(artifactDigest);
    }

    String artifactStoreDate = (String) recordHeaders.getHeaderValue(ArtifactConstants.ARTIFACT_STORE_DATE_KEY);
    log.trace("artifactStoreDate = {}", artifactStoreDate);
    if (!StringUtil.isNullString(artifactStoreDate)) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactStoreDate);
      ad.setStoreDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    String artifactCollectionDate = (String) recordHeaders.getHeaderValue(WARCConstants.HEADER_KEY_DATE);
    log.trace("artifactCollectionDate = {}", artifactCollectionDate);
    if (!StringUtil.isNullString(artifactCollectionDate)) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactCollectionDate);
      ad.setCollectionDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    return ad;
  }

  /**
   * Constructs an {@link ArtifactData} from a jwat-warc {@link WarcRecord} object.
   * @param record A {@link WarcRecord} containing the backing WARC record.
   * @return An {@link ArtifactData} object constructed from its underlying WARC record.
   * @throws IOException Thrown if there were any IO errors.
   */
  private static ArtifactData fromHttpResponseWarcRecord(WarcRecord record) throws IOException {
    HttpHeader httpHeader = record.getHttpHeader();

    ProtocolVersion protoVer = new ProtocolVersion(
        httpHeader.httpVersion,
        httpHeader.httpVersionMajor,
        httpHeader.httpVersionMinor);

    StatusLine statusLine = new BasicStatusLine(protoVer, httpHeader.statusCode, httpHeader.reasonPhrase);
    HttpResponse response = new BasicHttpResponse(statusLine);

    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(record.getPayloadContent());

    for (HeaderLine hline : httpHeader.getHeaderList()) {
      response.setHeader(hline.name, hline.value);
    }

    response.setEntity(entity);

    return ArtifactDataUtil.fromHttpResponse(response);
  }
}
