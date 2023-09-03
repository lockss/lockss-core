/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.daemon;

import java.io.*;
import java.util.*;
import org.apache.commons.collections4.set.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.daemon.status.*;
import org.lockss.log.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.util.rest.*;
import org.lockss.util.rest.status.*;
import org.lockss.util.rest.exception.LockssRestException;

/** Queries readiness of known REST services on startup, keeps querying
 * until service is ready, provides readiness status.
 * TODO - ongoing service monitoring
 * TODO - multiple service instances (multiple bindings)
 * TODO - could improve responsiveness w/ JMS hint
 */
public class RestServicesManager
  extends BaseLockssManager implements ConfigurableManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  public static final String PREFIX = Configuration.PREFIX + "services.";
  
  /** Probe interval as a function of how long we've been probing.
   * Idea is to probe frequently at start so it doesn't take long to
   * detect services coming up, but not cause excessive traffic
   * probing services that don't start.
   */
  public static final String PARAM_PROBE_INTERVAL_CURVE =
      PREFIX + "probeDownIntervalCurve";
  public static final String DEFAULT_PROBE_INTERVAL_CURVE =
      "[0,10s],[15m,10s],[15m,2m],[30m,2m],[30m,10m]";

  /** Minimum inter-probe sleep. */
  public static final String PARAM_MIN_PROBE_SLEEP =
    PREFIX + "minProbeSleep";
  public static final long DEFAULT_MIN_PROBE_SLEEP = 10 * Constants.SECOND;

  /** Connect timeout for REST call to service's status endpoint. */
  public static final String PARAM_CONNECT_TIMEOUT = PREFIX + "connectTimeout";
  public static final long DEFAULT_CONNECT_TIMEOUT = 30 + Constants.SECOND;

  /** Read timeout for REST call to service's status endpoint. */
  public static final String PARAM_READ_TIMEOUT = PREFIX + "readTimeout";
  public static final long DEFAULT_READ_TIMEOUT = 30 + Constants.SECOND;


//   public static final String PARAM_PREREQUISITES = Configuration.PREFIX +
//     "prerequisites";
//   public static final List DEFAULT_PREREQUISITES = null;

  private long minProbeSleep = DEFAULT_MIN_PROBE_SLEEP;;
  private long probeConnectTimeout = DEFAULT_CONNECT_TIMEOUT;
  private long probeReadTimeout = DEFAULT_READ_TIMEOUT;
  private CompoundLinearSlope probeIntervals =
    new CompoundLinearSlope(DEFAULT_PROBE_INTERVAL_CURVE);


  /** Maps service binding to status.  When the status changes, the
   * ServiceStatus object in this map is replaced, not updated, so that no
   * synchronization is needed within ServiceStatus. */
  protected Map<ServiceBinding,ServiceStatus> serviceStatusMap =
    new HashMap<>();

  protected Map<ServiceBinding,Thread> probeThreads = new HashMap<>();

  // Will have to change to resettable latch when ongoing monitoring is added
  protected Map<ServiceBinding,OneShotSemaphore> waitSems = new HashMap<>();

  public void startService() {
    super.startService();
    RestServiceStatus.registerAccessors(getApp());
    startProbes();
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      minProbeSleep = config.getTimeInterval(PARAM_MIN_PROBE_SLEEP,
					     DEFAULT_MIN_PROBE_SLEEP);
      probeConnectTimeout = config.getTimeInterval(PARAM_CONNECT_TIMEOUT,
						   DEFAULT_CONNECT_TIMEOUT);
      probeReadTimeout = config.getTimeInterval(PARAM_READ_TIMEOUT,
						   DEFAULT_READ_TIMEOUT);
      String probeCurve = config.get(PARAM_PROBE_INTERVAL_CURVE,
                                     DEFAULT_PROBE_INTERVAL_CURVE);
      try {
        probeIntervals = new CompoundLinearSlope(probeCurve);
      } catch (Exception e) {
        log.warn("Malformed {}: {}, using default: {}",
                 PARAM_PROBE_INTERVAL_CURVE, probeCurve,
                 DEFAULT_PROBE_INTERVAL_CURVE);
        probeIntervals = new CompoundLinearSlope(DEFAULT_PROBE_INTERVAL_CURVE);
      }
    }
  }

  public void stopService() {
    RestServiceStatus.unregisterAccessors(getApp());
    super.stopService();
  }

  /** Start probes for all known services. */
  void startProbes() {
    log.debug2("descrs: {}", getAllServiceDescrs());
    for (ServiceDescr descr : getAllServiceDescrs()) {
      ServiceBinding binding = getServiceBinding(descr);
      if (binding != null) {
	startProbe(descr, binding);
      }
    }
  }

  /** Start a probe thread for a single service. */
  void startProbe(ServiceDescr descr, ServiceBinding binding) {
    synchronized (probeThreads) {
      if (!probeThreads.containsKey(binding)) {
	Thread th = new Thread(() -> {probeService(descr, binding);});
	th.start();
	probeThreads.put(binding, th);
      }
    }
  }

  /** Probe a service until it becumes ready. */
  void probeService(ServiceDescr descr, ServiceBinding binding) {
    log.debug2("Probing {}: {}", descr.getAbbrev(), binding);
    long start = TimeBase.nowMs();
    try {
      while (true) {
	long probeStart = TimeBase.nowMs();
	probeOnce(descr, binding);
	ServiceStatus stat = getServiceStatus(binding);
	if (stat != null) {
	  if (stat.isReady()) {
	    log.debug2("probe thread exiting because service is ready: {}: {}",
		       descr.getAbbrev(), binding);
	    break;
	  }
	} else {
	  log.warn("probeOnce() didn't store a ServiceStatus: " + binding);
	}
      
	try {
	  long interval = (long)probeIntervals.getY(TimeBase.nowMs() - start);
	  long sleep = Long.max(interval - (TimeBase.nowMs() - probeStart),
                                minProbeSleep);
          log.debug2("svc: {}, interval: {}, sleep: {}",
                     descr.getAbbrev(),interval, sleep);
	  Thread.sleep(sleep);
	} catch (InterruptedException ignore) {}
      }
    } finally {
      synchronized (probeThreads) {
	probeThreads.remove(binding);
	log.debug2("Removed thread: {}", binding);
      }
    }
  }

  /** Execute a single probe of a service and store the results. */
  void probeOnce(ServiceDescr descr, ServiceBinding binding) {
    if (binding == null) {
      ServiceStatus stat = new ServiceStatus(descr, binding, "Unknown");
      putServiceStatus(binding, stat);
      return;
    }
    try {
      synchronized (serviceStatusMap) {
	if (!serviceStatusMap.containsKey(binding)) {
	  // set the status to updating only if it doesn't already have a
	  // status (i.e., first time)
	  ServiceStatus stat = new ServiceStatus(descr, binding, "Updating");
	  putServiceStatus(binding, stat);
	}
      }
      ApiStatus apiStat = callClientStatus(binding);
      ServiceStatus stat = new ServiceStatus(descr, binding, apiStat);
      stat.setLastTransition(TimeBase.nowMs());
      putServiceStatus(binding, stat);
    } catch (LockssRestException e) {
      ServiceStatus stat = new ServiceStatus(descr, binding, e);
      synchronized (serviceStatusMap) {
	stat.setLastTransition(TimeBase.nowMs());
      }
      putServiceStatus(binding, stat);
    }
  }

  /** Return last recorded status of service.
   * @param binding ServiceBinding of service
   * @return last recorded ServiceStatus of service
   */
  public ServiceStatus getServiceStatus(ServiceBinding binding) {
    synchronized (serviceStatusMap) {
      return serviceStatusMap.get(binding);
    }
  }

  /** Wait until service is ready.  If a non-infinite Deadline is supplied
   * the service will not necessarily be ready when this returns.
   * @param descr ServiceDescr of service
   * @return last recorded ServiceStatus of service, when it becomes ready
   * or when the Deadline expires
   */
  public ServiceStatus waitServiceReady(ServiceDescr descr,
					Deadline until) {
    ServiceBinding binding = getApp().getServiceBinding(descr);
    if (binding != null) {
      return waitServiceReady(descr, binding, until);
    }
    return null;
  }

  /** Wait until service is ready.  If a non-infinite Deadline is supplied
   * the service will not necessarily be ready when this returns.
   * @param descr ServiceDescr of service
   * @param binding ServiceBinding of service
   * @return last recorded ServiceStatus of service, when it becomes ready
   * or when the Deadline expires
   */
  public ServiceStatus waitServiceReady(ServiceDescr descr,
					ServiceBinding binding,
					Deadline until) {
    boolean logged = false;
    do {
      ServiceStatus stat = getServiceStatus(binding);
      if (stat != null) {
	if (stat.isReady()) {
	  if (logged) {
	    log.debug("Done waiting for service ready: {}: {}",
		      descr.getAbbrev(), binding);
	  }
	  return stat;
	}
      }
      if (!logged) {
	log.debug("Waiting for service ready: {}: {}",
		  descr.getAbbrev(), binding);
	logged = true;
      }
      OneShotSemaphore sem = getWaitSem(binding);
      try {
	sem.waitFull(until);
      } catch (InterruptedException e) {
      }
    } while (!until.expired());
    if (logged) {
      log.debug("Gave up waiting for service ready: {}: {}",
		descr.getAbbrev(), binding);
    }
    return getServiceStatus(binding);
  }

  /** Return true if the service is ready
   * @param binding ServiceBinding of service
   * @return true if service is ready
   */
  public boolean isServiceReady(ServiceBinding binding) {
    ServiceStatus stat = getServiceStatus(binding);
    return stat != null && stat.isReady();
  }

  void putServiceStatus(ServiceBinding binding, ServiceStatus stat) {
    synchronized (serviceStatusMap) {
      log.debug2("putStatus {}: {}", binding, stat);
      serviceStatusMap.put(binding, stat);
    }
    if (stat.isReady()) {
      getWaitSem(binding).fill();
    }
  }

  // Overridable for testing
  protected List<ServiceDescr> getAllServiceDescrs() {
    return getApp().getAllServiceDescrs();
  }

  // Overridable for testing
  protected ServiceBinding getServiceBinding(ServiceDescr sd) {
    return getApp().getServiceBinding(sd);
  }

  // Overridable for testing
  protected ApiStatus callClientStatus(ServiceBinding binding)
      throws LockssRestException {
    return new RestStatusClient(binding.getRestStem())
      .setTimeouts(probeConnectTimeout, probeReadTimeout)
      .getStatus();
  }

  OneShotSemaphore getWaitSem(ServiceBinding binding) {
    synchronized (waitSems) {
      OneShotSemaphore sem = waitSems.get(binding);
      if (sem == null) {
	sem = new OneShotSemaphore();
	waitSems.put(binding, sem);
      }
      return sem;
    }
  }

  /** Service status.  Records and provides access to results of most
   * recent probe: returned ApiStatus, network error, fixed message.
   * Immutable.
   */
  public static class ServiceStatus {
    private ServiceDescr descr;
    private ServiceBinding binding;
    private ApiStatus apiStat;
    private LockssRestException restEx;
    private String msg;
    private long lastTransition;

    public ServiceStatus(ServiceDescr descr, ServiceBinding binding,
			 ApiStatus apiStat) {
      this.descr = descr;
      this.binding = binding;
      this.apiStat = apiStat;
    }

    public ServiceStatus(ServiceDescr descr, ServiceBinding binding,
			 LockssRestException ex) {
      this.descr = descr;
      this.binding = binding;
      this.restEx = ex;
    }

    public ServiceStatus(ServiceDescr descr, ServiceBinding binding,
			 String msg) {
      this.descr = descr;
      this.binding = binding;
      this.msg = msg;
    }

    public ServiceDescr getServiceDescr() {
      return descr;
    }

    public ApiStatus getApiStatus() {
      return apiStat;
    }

    public boolean isReady() {
      return apiStat != null && apiStat.isReady();
    }

    public String getReason() {
      if (apiStat != null) {
	return apiStat.getReason();
      } else if (restEx != null) {
	return restEx.getShortMessage();
      } else if (msg != null) {
	return msg;
      } else {
	return "Not determined";
      }
    }

    /** Return the time at which the service claims to have become ready. */
    public long getReadyTime() {
      if (apiStat != null) {
	return apiStat.getReadyTime();
      } else {
	return 0;
      }
    }

    public long getLastTransition() {
      return lastTransition;
    }

    public ServiceStatus setLastTransition(long time) {
      lastTransition = time;
      return this;
    }

    public String toString() {
      return "[ServiceStatus: " + (isReady() ? "Ready" : "Not Ready")
	+ ", " + getReason() + "]";
    }
  }

}
