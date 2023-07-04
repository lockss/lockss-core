package org.lockss.rs.io.storage.warc;

import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.archive.format.warc.WARCConstants;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
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

import static org.lockss.rs.io.storage.warc.WarcArtifactDataUtil.buildArtifactIdentifier;

public class WarcArtifactData extends ArtifactData {
  private final static L4JLogger log = L4JLogger.getLogger();

  public static ArtifactData fromWarcRecord(WarcRecord record) throws IOException {
    // Get WARC record header
    List<HeaderLine> headersList = record.getHeaderList();

    Map<String, String> headers = new HashMap<>();
    for (HeaderLine hl: record.getHeaderList()) {
      headers.put(hl.name, hl.value);
    }

    ArtifactData ad;

    // Read ArtifactIdentifier from the WARC record headers
    ArtifactIdentifier artifactId = buildArtifactIdentifier(record);

    // Read WARC record type from record headers
    WARCRecordType recordType =
        WARCRecordType.valueOf(headers.get(WARCConstants.HEADER_KEY_TYPE));

    String mimeType = headers.get(WARCConstants.CONTENT_TYPE);

    // Artifacts can only be read out of WARC response and resource type records
    switch (recordType) {
      case response:
        // Sanity check
        if (mimeType == null || !mimeType.startsWith("application/http")) {
          log.warn("Unexpected content MIME type WARC response record: {}", mimeType);
          throw new IllegalStateException("Invalid MIME type: " + mimeType);
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

        // Set the ArtifactData content-type to that of the WARC record block if present
        if (!StringUtil.isNullString(mimeType)) {
          ad.getHttpHeaders().set(HttpHeaders.CONTENT_TYPE, mimeType);
        }
        break;

      default:
        log.warn("Unexpected WARC record type [WARC-Record-ID: {}, WARC-Type: {}]",
            headers.get(WARCConstants.HEADER_KEY_ID), recordType);

        // Could not return an artifact elsewhere
        return null;
    }

    String artifactContentLength = headers.get(ArtifactConstants.ARTIFACT_LENGTH_KEY);
    log.trace("artifactContentLength = {}", artifactContentLength);
    if (artifactContentLength != null && !artifactContentLength.trim().isEmpty()) {
      ad.setContentLength(Long.parseLong(artifactContentLength));
    }

    String artifactDigest = headers.get(ArtifactConstants.ARTIFACT_DIGEST_KEY);
    log.trace("artifactDigest = {}", artifactDigest);
    if (artifactDigest != null && !artifactDigest.trim().isEmpty()) {
      ad.setContentDigest(artifactDigest);
    }

    String artifactStoredDate = headers.get(ArtifactConstants.ARTIFACT_STORED_DATE);
    log.trace("artifactStoredDate = {}", artifactStoredDate);
    if (artifactStoredDate != null && !artifactStoredDate.trim().isEmpty()) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactStoredDate);
      ad.setStoredDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    String artifactCollectionDate = headers.get(WARCConstants.HEADER_KEY_DATE);
    log.trace("artifactCollectionDate = {}", artifactCollectionDate);
    if (artifactCollectionDate != null && !artifactCollectionDate.trim().isEmpty()) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactCollectionDate);
      ad.setCollectionDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    log.trace("ad = {}", ad);

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
    ArchiveRecordHeader recordHeader = record.getHeader();

    ArtifactData ad;

    // Read ArtifactIdentifier from the WARC record headers
    ArtifactIdentifier artifactId = buildArtifactIdentifier(recordHeader);

    // Read WARC record type from record headers
    WARCRecordType recordType =
        WARCRecordType.valueOf((String) recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_TYPE));

    // Artifacts can only be read out of WARC response and resource type records
    switch (recordType) {
      case response:
        // Sanity check
        String mimeType = recordHeader.getMimetype();
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
        ad = WarcArtifactData.fromResource(record);
        ad.setIdentifier(artifactId);

        // Set the ArtifactData content-type to that of the WARC record block if present
        String typeVal = recordHeader.getMimetype();
        if (!StringUtil.isNullString(typeVal)) {
          ad.getHttpHeaders().set(HttpHeaders.CONTENT_TYPE, typeVal);
        }
        break;

      default:
        log.warn("Unexpected WARC record type [WARC-Record-ID: {}, WARC-Type: {}]",
            recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_ID), recordType);

        // Could not return an artifact elsewhere
        return null;
    }

    String artifactContentLength = (String) recordHeader.getHeaderValue(ArtifactConstants.ARTIFACT_LENGTH_KEY);
    log.trace("artifactContentLength = {}", artifactContentLength);
    if (artifactContentLength != null && !artifactContentLength.trim().isEmpty()) {
      ad.setContentLength(Long.parseLong(artifactContentLength));
    }

    String artifactDigest = (String) recordHeader.getHeaderValue(ArtifactConstants.ARTIFACT_DIGEST_KEY);
    log.trace("artifactDigest = {}", artifactDigest);
    if (artifactDigest != null && !artifactDigest.trim().isEmpty()) {
      ad.setContentDigest(artifactDigest);
    }

    String artifactStoredDate = (String) recordHeader.getHeaderValue(ArtifactConstants.ARTIFACT_STORED_DATE);
    log.trace("artifactStoredDate = {}", artifactStoredDate);
    if (artifactStoredDate != null && !artifactStoredDate.trim().isEmpty()) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactStoredDate);
      ad.setStoredDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    String artifactCollectionDate = (String) recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_DATE);
    log.trace("artifactCollectionDate = {}", artifactCollectionDate);
    if (artifactCollectionDate != null && !artifactCollectionDate.trim().isEmpty()) {
      TemporalAccessor t = DateTimeFormatter.ISO_INSTANT.parse(artifactCollectionDate);
      ad.setCollectionDate(ZonedDateTime.ofInstant(Instant.from(t), ZoneOffset.UTC).toInstant().toEpochMilli());
    }

    log.trace("ad = {}", ad);

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
