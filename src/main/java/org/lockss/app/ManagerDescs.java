/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.app;

import org.lockss.app.LockssApp.ManagerDesc;

/**
 * A collection of standard {@link LockssApp#ManagerDesc}s, intended for
 * application startup code to import static.  Using these is not required
 * - apps can construct their own ManagerDesc.
 */
public class ManagerDescs {
  public static ManagerDesc RANDOM_MANAGER_DESC =
    new ManagerDesc(LockssApp.RANDOM_MANAGER,
		    "org.lockss.daemon.RandomManager");
  public static ManagerDesc RESOURCE_MANAGER_DESC =
    new ManagerDesc(LockssApp.RESOURCE_MANAGER,
		    "org.lockss.daemon.ResourceManager");
  public static ManagerDesc MAIL_SERVICE_DESC =
    new ManagerDesc(LockssApp.MAIL_SERVICE,
		    "org.lockss.mail.SmtpMailService");
  public static ManagerDesc ALERT_MANAGER_DESC =
    new ManagerDesc(LockssApp.ALERT_MANAGER,
		    "org.lockss.alert.AlertManagerImpl");
  public static ManagerDesc STATUS_SERVICE_DESC =
    new ManagerDesc(LockssApp.STATUS_SERVICE,
		    "org.lockss.daemon.status.StatusServiceImpl");
  public static ManagerDesc TRUEZIP_MANAGER_DESC =
    new ManagerDesc(LockssApp.TRUEZIP_MANAGER,
		    "org.lockss.truezip.TrueZipManager");
  public static ManagerDesc URL_MANAGER_DESC =
    new ManagerDesc(LockssApp.URL_MANAGER,
		    "org.lockss.daemon.UrlManager");
  public static ManagerDesc TIMER_SERVICE_DESC =
    new ManagerDesc(LockssApp.TIMER_SERVICE,
		    "org.lockss.util.TimerQueue$Manager");
  public static ManagerDesc KEYSTORE_MANAGER_DESC =
    new ManagerDesc(LockssApp.KEYSTORE_MANAGER,
                    "org.lockss.daemon.LockssKeyStoreManager");
  public static ManagerDesc PLUGIN_MANAGER_DESC =
    new ManagerDesc(LockssApp.PLUGIN_MANAGER,
		    "org.lockss.plugin.PluginManager");
  public static ManagerDesc SCHED_SERVICE_DESC =
    new ManagerDesc(LockssDaemon.SCHED_SERVICE,
		    "org.lockss.scheduler.SchedService");
  public static ManagerDesc HASH_SERVICE_DESC =
    new ManagerDesc(LockssDaemon.HASH_SERVICE,
		    "org.lockss.hasher.HashSvcQueueImpl");
  public static ManagerDesc SYSTEM_METRICS_DESC =
    new ManagerDesc(LockssDaemon.SYSTEM_METRICS,
		    "org.lockss.daemon.SystemMetrics");
  public static ManagerDesc ACCOUNT_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.ACCOUNT_MANAGER,
		    "org.lockss.account.AccountManager");
  public static ManagerDesc IDENTITY_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.IDENTITY_MANAGER,
                    "org.lockss.protocol.IdentityManagerImpl");
  public static ManagerDesc PSM_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.PSM_MANAGER,
		    "org.lockss.protocol.psm.PsmManager");
  public static ManagerDesc POLL_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.POLL_MANAGER,
		    "org.lockss.poller.PollManager");
  public static ManagerDesc CRAWL_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.CRAWL_MANAGER,
		    "org.lockss.crawler.CrawlManagerImpl");
  public static ManagerDesc REPOSITORY_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.REPOSITORY_MANAGER,
                    "org.lockss.repository.RepositoryManager");
  public static ManagerDesc METADATA_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.METADATA_MANAGER,
		    "org.lockss.metadata.MetadataManager");
  public static ManagerDesc METADATA_DB_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.METADATA_DB_MANAGER,
		    "org.lockss.metadata.MetadataDbManager");
  public static ManagerDesc REMOTE_API_DESC =
    new ManagerDesc(LockssDaemon.REMOTE_API,
		    "org.lockss.remote.RemoteApi");
  public static ManagerDesc COUNTER_REPORTS_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.COUNTER_REPORTS_MANAGER,
		    "org.lockss.exporter.counter.CounterReportsManager");
  public static ManagerDesc SUBSCRIPTION_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.SUBSCRIPTION_MANAGER,
		    "org.lockss.subscription.SubscriptionManager");
  public static ManagerDesc FETCH_TIME_EXPORT_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.FETCH_TIME_EXPORT_MANAGER,
		    "org.lockss.exporter.FetchTimeExportManager");
  public static ManagerDesc SERVLET_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.SERVLET_MANAGER,
		    "org.lockss.servlet.AdminServletManager");
  public static ManagerDesc CONTENT_SERVLET_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.CONTENT_SERVLET_MANAGER,
                    "org.lockss.servlet.ContentServletManager");
  public static ManagerDesc PROXY_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.PROXY_MANAGER,
		    "org.lockss.proxy.ProxyManager");
  public static ManagerDesc AUDIT_PROXY_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.AUDIT_PROXY_MANAGER,
		    "org.lockss.proxy.AuditProxyManager");
  public static ManagerDesc FAIL_OVER_PROXY_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.FAIL_OVER_PROXY_MANAGER ,
                    "org.lockss.proxy.FailOverProxyManager");
  public static ManagerDesc DATAGRAM_COMM_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.DATAGRAM_COMM_MANAGER,
                    "org.lockss.protocol.LcapDatagramComm");
  public static ManagerDesc STREAM_COMM_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.STREAM_COMM_MANAGER,
                    "org.lockss.protocol.BlockingStreamComm");
  public static ManagerDesc ROUTER_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.ROUTER_MANAGER,
                    "org.lockss.protocol.LcapRouter");
  public static ManagerDesc ICP_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.ICP_MANAGER,
                    "org.lockss.proxy.icp.IcpManager");
  public static ManagerDesc PLATFORM_CONFIG_STATUS_DESC =
    new ManagerDesc(LockssDaemon.PLATFORM_CONFIG_STATUS,
                    "org.lockss.config.PlatformConfigStatus");
  public static ManagerDesc BUILD_INFO_STATUS_DESC =
    new ManagerDesc(LockssDaemon.BUILD_INFO_STATUS,
                    "org.lockss.config.BuildInfoStatus");
  public static ManagerDesc CONFIG_STATUS_DESC =
    new ManagerDesc(LockssDaemon.CONFIG_STATUS,
                    "org.lockss.config.ConfigStatus");
  public static ManagerDesc ARCHIVAL_UNIT_STATUS_DESC =
    new ManagerDesc(LockssDaemon.ARCHIVAL_UNIT_STATUS,
                    "org.lockss.state.ArchivalUnitStatus");
  public static ManagerDesc REPOSITORY_STATUS_DESC =
    new ManagerDesc(LockssDaemon.REPOSITORY_STATUS,
                    "org.lockss.repository.LockssRepositoryStatus");
  public static ManagerDesc OVERVIEW_STATUS_DESC =
    new ManagerDesc(LockssDaemon.OVERVIEW_STATUS,
                    "org.lockss.daemon.status.OverviewStatus");
  public static ManagerDesc JMS_MANAGER_DESC =
    new ManagerDesc("org.lockss.jms.JMSManager");

  public static ManagerDesc STATE_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.STATE_MANAGER,
		    "org.lockss.state.ClientStateManager") {
      public String getDefaultClass(LockssApp app) {
	return app.chooseStateManager();
      }};
  
  public static ManagerDesc CONFIG_DB_MANAGER_DESC =
    new ManagerDesc(LockssDaemon.CONFIG_DB_MANAGER,
		    "org.lockss.config.db.ConfigDbManager") {
      // Start ConfigDbManager iff we're not using a remote config service
      public boolean shouldStart(LockssApp app) {
	// Temporarily unconditional until ConfigManager fixed
// 	return true;
        return !app.isConfigClient();
      }};
}
