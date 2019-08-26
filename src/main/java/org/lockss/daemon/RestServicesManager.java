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
// import java.util.concurrent.*;
import org.apache.commons.collections4.set.*;

import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.daemon.status.*;
import org.lockss.log.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.rs.*;
import org.lockss.laaws.status.model.*;
import org.lockss.rs.exception.LockssRestException;

// XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX JMS XXXXXXXXXXXXXXXXXXXXXXXXXXX


/** Polls service dependencies on startup */
public class RestServicesManager
  extends BaseLockssManager implements ConfigurableManager {

  protected static L4JLogger log = L4JLogger.getLogger();

  public static final String PREFIX = Configuration.PREFIX + "services.";
  
  public static final String PARAM_RATE = PREFIX + "rate";
  public static final long DEFAULT_RATE = 10 * Constants.SECOND;

  public static final String PARAM_CONNECT_TIMEOUT = PREFIX + "connectTimeout";
  public static final long DEFAULT_CONNECT_TIMEOUT = 30 + Constants.SECOND;

  public static final String PARAM_READ_TIMEOUT = PREFIX + "readTimeout";
  public static final long DEFAULT_READ_TIMEOUT = 30 + Constants.SECOND;

  public static final String PARAM_PREREQUISITES = Configuration.PREFIX +
    "prerequisites";
  public static final List DEFAULT_PREREQUISITES = null;

  private long probeInterval = DEFAULT_RATE;;
  private long probeConnectTimeout = DEFAULT_CONNECT_TIMEOUT;
  private long probeReadTimeout = DEFAULT_READ_TIMEOUT;


  private Map<String,ServiceStatus> serviceStatusMap = new HashMap<>();

  private ListOrderedSet<ServiceDescr> unsatisfied = new ListOrderedSet<>();
  private ListOrderedSet<ServiceDescr> satisfied = new ListOrderedSet<>();

//   private static ThreadPoolExecutor executor;

  public void startService() {
    super.startService();
    RestServiceStatus.registerAccessors(getApp());
    startProbes();
//     if (!unsatisfied.isEmpty()) {
// //       startWatcher();
//     }
  }

  public void setConfig(Configuration config, Configuration oldConfig,
			Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      probeInterval = config.getTimeInterval(PARAM_RATE, DEFAULT_RATE);
      probeConnectTimeout = config.getTimeInterval(PARAM_CONNECT_TIMEOUT,
						   DEFAULT_CONNECT_TIMEOUT);
      probeReadTimeout = config.getTimeInterval(PARAM_READ_TIMEOUT,
						   DEFAULT_READ_TIMEOUT);
    }

//     if (!getApp().isAppRunning()) {
//       processPrerequisites(config.getList(PARAM_PREREQUISITES,
// 					  DEFAULT_PREREQUISITES));
//     }

  }

  public void stopService() {
    RestServiceStatus.unregisterAccessors(getApp());
    super.stopService();
  }

//   void processPrerequisites(List<String> svcNames) {
//     if (svcNames == null) {
//       return;
//     }
//     for (String name : svcNames) {
//     }
//     if (!unsatisfied.isEmpty()) {
//       new Thread(() -> {checkPrerequisites();}).start();
//     }
//   }

//   void checkPrerequisites() {
//     while (true) {
//       List<ServiceDescr> nowReady = new ArrayList<>();
//       for (ServiceDescr descr : unsatisfied) {
// // 	if (probeService(descr)) {
// // 	  nowReady.add(descr);
// // 	}
//       }
//       unsatisfied.removeAll(nowReady);
//       satisfied.addAll(nowReady);
//     }
    
//   }

  Map<String,Thread> probeThreads = new HashMap<>();

  void startProbes() {
    for (ServiceDescr descr : getApp().getAllServiceDescrs()) {
      ServiceBinding binding = getApp().getServiceBinding(descr);
      if (binding != null) {
	startProbe(descr, binding);
      }
    }
  }

  void startProbe(ServiceDescr descr, ServiceBinding binding) {
    String key = binding.getRestStem();
    synchronized (probeThreads) {
      if (!probeThreads.containsKey(key)) {
	Thread th = new Thread(() -> {probeService(descr, binding);});
	th.start();
	probeThreads.put(key, th);
      }
    }
  }

  void probeService(ServiceDescr descr, ServiceBinding binding) {
    try {
      while (true) {
	long start = TimeBase.nowMs();
	probeOnce(descr, binding);
	ServiceStatus stat = getServiceStatus(binding);
	if (stat != null) {
	  if (stat.isReady()) {
	    break;
	  }
	} else {
	  log.warn("probeOnce() didn't store a ServiceStatus: " + binding);
	}
      
	try {
	  long sleep = probeInterval - (TimeBase.nowMs() - start);
	  Thread.sleep(sleep);
	} catch (InterruptedException ignore) {}
      }
    } finally {
      synchronized (probeThreads) {
	probeThreads.remove(Thread.currentThread());
      }
    }
  }

  void putServiceStatus(ServiceBinding binding, ServiceStatus stat) {
    synchronized (serviceStatusMap) {
      serviceStatusMap.put(binding.getRestStem(), stat);
    }
  }

//   void probeOnce(ServiceDescr descr) {
//     // Eventually this will have to deal with multiple bindings per
//     // service.
//     probeOnce(descr, getApp().getServiceBinding(descr));
//   }

  void probeOnce(ServiceDescr descr, ServiceBinding binding) {
    if (binding == null) {
      ServiceStatus stat = new ServiceStatus(descr, binding, "Unknown");
      putServiceStatus(binding, stat);
      return;
    }
    String uri = binding.getRestStem();
    RestStatusClient client =
      new RestStatusClient(uri, probeConnectTimeout, probeReadTimeout);
    try {
      synchronized (serviceStatusMap) {
	if (!serviceStatusMap.containsKey(descr)) {
	  ServiceStatus stat = new ServiceStatus(descr, binding, "Updating");
	  putServiceStatus(binding, stat);
	}
      }
      ApiStatus apiStat = client.getStatus();
      ServiceStatus stat = new ServiceStatus(descr, binding, apiStat);
      stat.setLastTransition(TimeBase.nowMs());
      putServiceStatus(binding, stat);
    } catch (LockssRestException e) {
      ServiceStatus stat = new ServiceStatus(descr, binding, e);
      synchronized (serviceStatusMap) {
	// update lastTransition on network error only if we already had a
	// last transition time
	ServiceStatus curStat = getServiceStatus(binding);
	if (curStat != null && curStat.getLastTransition() > 0) {
	  stat.setLastTransition(TimeBase.nowMs());
	}
	putServiceStatus(binding, stat);
      }
    }
  }

  public ServiceStatus getServiceStatus(ServiceBinding binding) {
    synchronized (serviceStatusMap) {
      return serviceStatusMap.get(binding.getRestStem());
    }
  }

  public ServiceStatus waitServiceReady(ServiceDescr descr,
					Deadline until) {
    ServiceBinding binding = getApp().getServiceBinding(descr);
    if (binding != null) {
      return waitServiceReady(descr, binding, until);
    }
    return null;
  }

  public ServiceStatus waitServiceReady(ServiceDescr descr,
					ServiceBinding binding,
					Deadline until) {
    boolean logged = false;
    do {
      ServiceStatus stat = getServiceStatus(binding);
      if (stat != null) {
	if (stat.isReady()) {
	  if (logged) {
	    log.debug("Done waiting for service ready: {}", descr);
	  }
	  return stat;
	}
      }
      if (!logged) {
	log.debug("Waiting for service ready: {}", descr);
	logged = true;
      }
      try {
	Deadline.in(10 * Constants.SECOND).sleep();
      } catch (InterruptedException e) {
      }
    } while (!until.expired());
    if (logged) {
      log.debug("Gave up waiting for service ready: {}", descr);
    }
    return getServiceStatus(binding);
  }

  /** Service status.  Isolates clients from the REST-specific ApiStatus,
   * and holds some state info */
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

    public boolean isReady() {
      return apiStat != null && apiStat.isReady();
    }

    public String getReason() {
      if (apiStat != null) {
	return apiStat.getReason();
      } else if (restEx != null) {
	return restEx.getMessage();
      } else if (msg != null) {
	return msg;
      } else {
	return "Not determined";
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
	+ ", " + getReason();
    }
  }

}
