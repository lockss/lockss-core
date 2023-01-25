package org.lockss.crawler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.lockss.state.BaseStateManager.JMS_MAP_COOKIE;
import static org.lockss.state.BaseStateManager.JMS_MAP_NAME;

public class CrawlEvent {

  public enum Type {CrawlStarted, CrawlAttemptComplete};

  public static final String KEY_SUCCEEDED = "succeeded";

  public static final String KEY_CRAWL_STATUS = "status";
  public static final String KEY_CRAWL_MESSAGE = "statusMsg";
  public static final String KEY_CRAWL_FETCH_COUNT = "numFetched";
  public static final String KEY_CRAWL_FETCH_URLS = "urlsFetched";
  public static final String KEY_CRAWL_EXTRA_DATA = "extraData";

  private Type evtType;
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
      this.status = cs.getCrawlStatus();
      this.statusMsg = cs.getCrawlStatusMsg();
      if (successful && evtType == Type.CrawlAttemptComplete) {
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

  public CrawlEvent setCookie(Map<String, Object> extraData) {
    this.extraData = extraData;
    return this;
  }



  Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put(JMS_MAP_NAME, evtType);
    map.put(KEY_SUCCEEDED, successful);
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
    if(map.containsKey(KEY_CRAWL_STATUS))
      event.status =  (int) map.get(KEY_CRAWL_STATUS);
    if(map.containsKey(KEY_CRAWL_MESSAGE))
      event.statusMsg = (String) map.get(KEY_CRAWL_MESSAGE);
    if(map.containsKey(KEY_CRAWL_FETCH_URLS)) {
      event.numFetched = (int) map.get(KEY_CRAWL_FETCH_COUNT);
      event.urlsFetched = (List<String>) map.get(KEY_CRAWL_FETCH_URLS);
    }
    if(map.containsKey(KEY_CRAWL_EXTRA_DATA)) {
      event.extraData = (Map) map.get(KEY_CRAWL_EXTRA_DATA);
    }
    return event;
  }

  private void putNotNull(Map<String, Object> map, String key, Object value) {
    if (map != null && value != null) {map.put(key, value);}
  }
}
