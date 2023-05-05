package org.lockss.crawler;

import static org.lockss.state.BaseStateManager.JMS_MAP_NAME;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.lockss.util.rest.crawler.CrawlDesc;

public class CrawlEvent {

  public enum Type {
    /** A Crawl attempt has started. */
    CrawlStarted,
    /** The Crawl Attempt has completed. */
    CrawlAttemptComplete,
    /** A Repair crawl has started. */
    RepairCrawlStarted,
    /** A Repoir Crawl Attempt was completed. */
    RepairCrawlComplete
  }

  /** Key - boolean succeed true, failed false */
  public static final String KEY_SUCCEEDED = "succeeded";

  public static final String KEY_CRAWLER_ID = "crawlerId";
  /** Key integer of crawl Status */
  public static final String KEY_CRAWL_STATUS = "status";
  /** Key String of message for crawl status result */
  public static final String KEY_CRAWL_MESSAGE = "statusMsg";
  /** Num of Urls Fetched */
  public static final String KEY_CRAWL_FETCH_COUNT = "numFetched";
  /** A list of URL strings fetched during the crawl. */
  public static final String KEY_CRAWL_FETCH_URLS = "urlsFetched";
  /** Any extra data related to the crawl request. */
  public static final String KEY_CRAWL_EXTRA_DATA = "extraData";

  private Type evtType;
  private String crawlerId;
  private boolean successful;
  private int numFetched;
  private List<String> urlsFetched;
  private String statusMsg;
  private int status;
  private Map<String, Object> extraData;

  public CrawlEvent(Type evtType, Object extraData, boolean successful, CrawlerStatus cs) {
    this.evtType = evtType;
    if (extraData != null && extraData instanceof Map) {
      this.extraData = (Map<String, Object>) extraData;
    }
    this.successful = successful;
    if (cs != null) {
      this.crawlerId = cs.getCrawlerId() == null ? CrawlDesc.CLASSIC_CRAWLER_ID : cs.getCrawlerId();
      this.status = cs.getCrawlStatus();
      this.statusMsg = cs.getCrawlStatusMsg();
      if (successful && evtType != Type.CrawlStarted) {
        this.urlsFetched = cs.getUrlsFetched();
        this.numFetched = cs.getNumFetched();
      }
    }
  }

  public CrawlEvent(Type evtType) {
    this.evtType = evtType;
  }

  public Type getEvtType() {
    return evtType;
  }

  public CrawlEvent setEvtType(Type evtType) {
    this.evtType = evtType;
    return this;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public CrawlEvent setSuccessful(boolean successful) {
    this.successful = successful;
    return this;
  }

  public int getNumFetched() {
    return numFetched;
  }

  public CrawlEvent setNumFetched(int numFetched) {
    this.numFetched = numFetched;
    return this;
  }

  public List<String> getUrlsFetched() {
    return urlsFetched;
  }

  public CrawlEvent setUrlsFetched(List<String> urlsFetched) {
    this.urlsFetched = urlsFetched;
    return this;
  }

  public String getStatusMsg() {
    return statusMsg;
  }

  public CrawlEvent setStatusMsg(String statusMsg) {
    this.statusMsg = statusMsg;
    return this;
  }

  public int getStatus() {
    return status;
  }

  public CrawlEvent setStatus(int status) {
    this.status = status;
    return this;
  }

  public Map<String, Object> getExtraData() {
    return extraData;
  }

  public CrawlEvent setExtraData(Map<String, Object> extraData) {
    this.extraData = extraData;
    return this;
  }

  public String getCrawlerId() {
    return crawlerId;
  }

  public CrawlEvent setCrawlerId(String crawlerId) {
    this.crawlerId = crawlerId;
    return this;
  }

  Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(JMS_MAP_NAME, evtType.name());
    map.put(KEY_SUCCEEDED, successful);
    map.put(KEY_CRAWLER_ID, crawlerId);
    putNotNull(map, KEY_CRAWL_STATUS, status);
    putNotNull(map, KEY_CRAWL_MESSAGE, statusMsg);
    putNotNull(map, KEY_CRAWL_FETCH_COUNT, numFetched);
    putNotNull(map, KEY_CRAWL_FETCH_URLS, urlsFetched);
    putNotNull(map, KEY_CRAWL_EXTRA_DATA, extraData);
    return map;
  }

  public static CrawlEvent fromMap(Map<String, Object> map) {
    CrawlEvent event = new CrawlEvent(Type.valueOf((String) map.get(JMS_MAP_NAME)));
    event.successful = (boolean) map.get(KEY_SUCCEEDED);
    event.crawlerId = (String) map.get(KEY_CRAWLER_ID);
    if (map.containsKey(KEY_CRAWL_STATUS)) event.status = (int) map.get(KEY_CRAWL_STATUS);
    if (map.containsKey(KEY_CRAWL_MESSAGE)) event.statusMsg = (String) map.get(KEY_CRAWL_MESSAGE);
    if (map.containsKey(KEY_CRAWL_FETCH_URLS)) {
      event.numFetched = (int) map.get(KEY_CRAWL_FETCH_COUNT);
      event.urlsFetched = (List<String>) map.get(KEY_CRAWL_FETCH_URLS);
    }
    if (map.containsKey(KEY_CRAWL_EXTRA_DATA)) {
      event.extraData = (Map) map.get(KEY_CRAWL_EXTRA_DATA);
    }
    return event;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CrawlEvent that = (CrawlEvent) o;
    return successful == that.successful
        && numFetched == that.numFetched
        && status == that.status
        && evtType == that.evtType
        && Objects.equals(crawlerId, that.crawlerId)
        && Objects.equals(urlsFetched, that.urlsFetched)
        && Objects.equals(statusMsg, that.statusMsg)
        && Objects.equals(extraData, that.extraData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        evtType, crawlerId, successful, numFetched, urlsFetched, statusMsg, status, extraData);
  }

  @Override
  public String toString() {
    String sb =
        "CrawlEvent["
            + "evtType="
            + evtType
            + ", crawlerId="
            + crawlerId
            + ", successful="
            + successful
            + ", numFetched="
            + numFetched
            + ", urlsFetched="
            + urlsFetched
            + ", statusMsg='"
            + statusMsg
            + '\''
            + ", status="
            + status
            + ", extraData="
            + extraData
            + ']';
    return sb;
  }

  private void putNotNull(Map<String, Object> map, String key, Object value) {
    if (map != null && value != null) {
      map.put(key, value);
    }
  }
}
