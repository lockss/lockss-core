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

package org.lockss.remote;

import org.lockss.config.Configuration;
import org.lockss.daemon.*;
import org.lockss.db.DbException;
import org.lockss.plugin.*;
import org.lockss.log.*;
import org.lockss.rs.exception.LockssRestException;

/**
 * Proxy object for remote access to ArchivalUnit.  Subset of ArchivalUnit
 * interface in which all {@link Plugin}s and {@link ArchivalUnit}s are
 * replaced with {@link PluginProxy}s and {@link AuProxy}s.
 */
public class AuProxy {
  private static L4JLogger log = L4JLogger.getLogger();

  private RemoteApi remoteApi;
  private ArchivalUnit au;
  private String auid;
  private Configuration config;

  AuProxy(ArchivalUnit au, RemoteApi remoteApi) {
    this.remoteApi = remoteApi;
    this.au = au;
    this.auid = au.getAuId();
    this.config = au.getConfiguration();
  }

  /** Create an AuProxy for the AU with the given ID.
   * @param auid the AU ID string.
   * @param remoteApi the RemoteApi service
   * @throws NoSuchAU if no AU with the given ID exists
   */
  public AuProxy(String auid, RemoteApi remoteApi)
      throws NoSuchAU, DbException, LockssRestException {
    this.auid = auid;
    this.remoteApi = remoteApi;
    au = remoteApi.getAuFromIdIfExists(auid);
    if (au != null) {
      config = au.getConfiguration();
    } else {
      config = getStoredConfiguration();
      if (config == null) {
	throw new NoSuchAU(auid);
      }
    }
  }

//   AuProxy(RemoteApi remoteApi) {
//     this.remoteApi = remoteApi;
//   }

  public ArchivalUnit getAu() {
    if (au != null) {
      return au;
    } else {
      throw new IllegalStateException("Can't get AU from absent AU");
    }
  }

  /**
   * Return the AU's current configuration.
   * @return a Configuration
   */
  public Configuration getConfiguration() {
    return config;
  }

  /**
   * Return the AU's current configuration or null if not configured
   * @return a Configuration
   * @throws DbException
   * @throws LockssRestException
   */
  public Configuration getStoredConfiguration()
      throws DbException, LockssRestException {
    return getRemoteApi().getStoredAuConfiguration(auid);
  }

  /**
   * Return the AU's TitleConfig, if any.
   * @return a TitleConfig, or null
   */
  public TitleConfig getTitleConfig() {
    return au.getTitleConfig();
  }

  /**
   * Returns the {@link PluginProxy} for the {@link Plugin} to which this
   * AU belongs
   * @return the plugin
   */
  public PluginProxy getPlugin() {
    return getRemoteApi().findPluginProxy(getPluginId());
  }

  /**
   * Returns a unique string identifier for the Plugin.
   * @return a unique id
   */
  public String getPluginId() {
    return getRemoteApi().pluginIdFromAuId(getAuId());
  }

  /**
   * Returns a globally unique string identifier for the ArchivalUnit.
   * @return a unique id
   */
  public String getAuId() {
    return auid;
  }

  /**
   * Returns a human-readable name for the ArchivalUnit.
   * @return the AU name
   */
  public String getName() {
    if (au != null) {
      return au.getName();
    } else {
      return "Noname AU: " + auid;
    }
  }

  public static class NoSuchAU extends Exception {
    public NoSuchAU(String msg) {
      super(msg);
    }
  }

  protected RemoteApi getRemoteApi() {
    return remoteApi;
  }

  public boolean isActiveAu() {
    return !config.getBoolean(PluginManager.AU_PARAM_DISABLED, false);
  }
}
