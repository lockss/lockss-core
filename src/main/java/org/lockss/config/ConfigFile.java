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

package org.lockss.config;

import java.io.*;
import org.lockss.util.urlconn.*;

/**
 * Common functionality for a config file loadable from a URL or filename,
 * and parseable as either XML or props.
 */
public interface ConfigFile {
  public static final int XML_FILE = 0;
  public static final int PROPERTIES_FILE = 1;

  public String getFileUrl();

  public String getLoadedUrl();

  public String resolveConfigUrl(String relativeUrl);

  /** Return true if this file might contain platform values that are
   * needed in order to properly parse other config files.
   */
  public boolean isPlatformFile();

  /** if true, forces isPlatformFile() to return true */
  public void setPlatformFile(boolean val);

  public int getFileType();

  public String getLastModified();

  public long getLastAttemptTime();

  public default String getProxyUsed() {
    return null;
  }

  default public boolean isLoadedFromFailover() {
    return false;
  }

  public void setKeyPredicate(ConfigManager.KeyPredicate pred);

  public Generation getGeneration() throws IOException;

  public String getLoadErrorMessage();

  public boolean isLoaded();

  /**
   * Instruct the ConfigFile to check for modifications the next time it's
   * accessed
   */
  public void setNeedsReload();

  public void setConnectionPool(LockssUrlConnectionPool connPool);

  public void setProperty(String key, Object val);

  /** Return the Configuration object built from this file
   */
  public Configuration getConfiguration() throws IOException;

  /**
   * Provides an input stream to the content of this file.
   * <br>
   * Use this to stream the file contents.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  public InputStream getInputStream() throws IOException;

  /**
   * Do the actual writing of the file to the disk by renaming a temporary file.
   * 
   * @param tempfile
   *          A File with the source temporary file.
   * @param config
   *          A Configuration with the configuration to be written.
   * @throws IOException
   *           if there are problems.
   */
  public void writeFromTempFile(File tempfile, Configuration config)
      throws IOException;

  /**
   * Provides an indication of whether this is a RestConfigFile.
   * 
   * @return a boolean with <code>true</code> if this is a RestConfigFile,
   *         <code>false</code> otherwise.
   */
  default boolean isRestConfigFile() {
    return false;
  }

  /**
   * Provides the input stream to the content of this configuration file if the
   * passed preconditions are met.
   * 
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   * @throws UnsupportedOperationException
   *           if the operation is not overriden in a subclass.
   */
  default ConfigFileReadWriteResult conditionallyRead(HttpRequestPreconditions
      preconditions) throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Writes the passed content to this configuration file if the passed
   * preconditions are met.
   * 
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @param inputStream
   *          An InputStream to the content to be written to this configuration
   *          file.
   * @return a ConfigFileReadWriteResult with the result of the operation.
   * @throws IOException
   *           if there are problems.
   * @throws UnsupportedOperationException
   *           if the operation is not overriden in a subclass.
   */
  default ConfigFileReadWriteResult conditionallyWrite(HttpRequestPreconditions
      preconditions, InputStream inputStream)
	  throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /** Represents a single generation (version) of the contents of a
   * ConfigFile, to make it easy to determine when the contents has
   * changed */
  public static class Generation {
    private ConfigFile cf;
    private Configuration config;
    private int generation;
    public Generation(ConfigFile cf, Configuration config, int generation) {
      this.cf = cf;
      this.config = config;
      this.generation = generation;
    }
    public Configuration getConfig() {
      return config;
    }
    public int getGeneration() {
      return generation;
    }
    public String getUrl() {
      return cf.getFileUrl();
    }
    public ConfigFile getConfigFile() {
      return cf;
    }

    @Override
    public String toString() {
      return "[Generation cf=" + cf + ", "
	  + Configuration.loggableConfiguration(config, "config")
	  + ", generation=" + generation + "]";
    }
  }
}
