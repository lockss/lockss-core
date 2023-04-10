/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.rs.io.storage.warc;

import org.apache.commons.io.FileUtils;
import org.archive.format.warc.WARCConstants;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcFilePool;
import org.lockss.util.rest.repo.model.NamespacedAuid;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.storage.StorageInfo;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Local filesystem implementation of WarcArtifactDataStore.
 */
public class LocalWarcArtifactDataStore extends WarcArtifactDataStore {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** Label to describe type of LocalWarcArtifactDataStore */
  public static String ARTIFACT_DATASTORE_TYPE = "Posix";

  public final static long DEFAULT_BLOCKSIZE = FileUtils.ONE_KB * 4;

  // *******************************************************************************************************************
  // * CONSTRUCTORS
  // *******************************************************************************************************************

  public LocalWarcArtifactDataStore(File basePath) throws IOException {
    this(new File[]{basePath});
  }

  public LocalWarcArtifactDataStore(File[] basePath) throws IOException {
    this(Arrays.stream(basePath).map(File::toPath).toArray(Path[]::new));
  }

  public LocalWarcArtifactDataStore(Path basePaths) throws IOException {
    this(new Path[]{basePaths});
  }

  /**
   * Constructor. Rebuilds the index on start-up from a given repository base path, if using a volatile index.
   */
  public LocalWarcArtifactDataStore(Path[] basePaths) throws IOException {
    log.debug2("Starting local WARC artifact data store [basePaths: {}]", basePaths);

    // Set local base paths
    this.basePaths = basePaths;

    // Start temporary WARC file pool
    this.tmpWarcPool = new WarcFilePool(this);

    // Initialize LOCKSS repository structure under base paths
    for (Path basePath : basePaths) {
      mkdirs(basePath);
      mkdirs(getTmpWarcBasePaths());
    }
  }

  // *******************************************************************************************************************
  // * UTILITY METHODS
  // *******************************************************************************************************************

  public void mkdirs(Path dirPath) throws IOException {
    log.trace("dirPath = {}", dirPath);

    if (dirPath == null) {
      log.debug2("dirPath is null!");
      return;
    }

    if (!FileUtil.ensureDirExists(dirPath.toFile())) {
      throw new IOException(String.format("Could not create directory [dirPath: %s]", dirPath));
    }
  }

  public void mkdirs(Path[] dirs) throws IOException {
    for (Path dirPath : dirs) {
      mkdirs(dirPath);
    }
  }


  /**
   * Only used to enable testing!
   *
   */
  // FIXME
  protected void clearAuMaps() {
    log.debug("Cleared internal AU maps");

    // Reset maps
    auPathsMap = new HashMap<>();
    auActiveWarcsMap = new HashMap<>();
  }

  // *******************************************************************************************************************
  // * ABSTRACT METHOD IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  protected long getBlockSize() {
    return DEFAULT_BLOCKSIZE;
  }

  @Override
  protected long getFreeSpace(Path fsPath) {
    return fsPath.toFile().getFreeSpace();
  }

  @Override
  public void initNamespace(String namespace) throws IOException {
    mkdirs(getNamespacePaths(namespace));
  }

  /**
   * Local filesystems implementation of {@link ArtifactDataStore#initAu(String, String)}.
   * <p>
   * Initializes an AU by reloading any existing directories of this AU or creates a new one if initializing this AU
   * for the first time.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid
   * @return
   * @throws IOException
   */
  @Override
  public List<Path> initAu(String namespace, String auid) throws IOException {
    //// Initialize namespace on each filesystem
    initNamespace(namespace);

    //// Reload any existing AU base paths

    // Get base paths of the repository
    Path[] baseDirs = getBasePaths();

    if (baseDirs == null || baseDirs.length < 1) {
      log.error("No data store base directories configured");
      throw new IllegalStateException("Data store is misconfigured");
    }

    // Find existing base directories of this AU
    List<Path> auPathsFound = Arrays.stream(baseDirs)
        .map(basePath -> getAuPath(basePath, namespace, auid))
        .filter(auPath -> auPath.toFile().isDirectory())
        .collect(Collectors.toList());

    if (auPathsFound.isEmpty()) {
      // No existing directories for this AU: Initialize a new AU directory
      auPathsFound.add(initAuDir(namespace, auid));
    }

    // Track AU directories in internal AU paths map
    NamespacedAuid key = new NamespacedAuid(namespace, auid);
    auPathsMap.put(key, auPathsFound);

    return auPathsFound;
  }

  /**
   * Creates a new AU base directory on the repository base directory having the most free space. No-op if the directory
   * already exists on disk.
   *
   * @param namespace A {@link String} containing the namespace.
   * @param auid         A {@link String} containing the AUID of the AU.
   * @return A {@link Path} containing the path to the AU base directory.
   * @throws IOException
   */
  @Override
  protected Path initAuDir(String namespace, String auid) throws IOException {
    Path[] basePaths = getBasePaths();

    if (basePaths == null || basePaths.length < 1) {
      log.error("No data store base directories configured");
      throw new IllegalStateException("Data store is misconfigured");
    }

    // Determine which base path to use based on current available space
    Path basePath = Arrays.stream(basePaths)
        .sorted((a, b) -> (int) (getFreeSpace(b.getParent()) - getFreeSpace(a.getParent())))
        .findFirst()
        .get();

    // Generate an AU path under this base path and create it on disk
    Path auPath = getAuPath(basePath, namespace, auid);

    // Create the AU directory if necessary
    if (!auPath.toFile().isDirectory()) {
      mkdirs(auPath);
    }

    return auPath;
  }

  /**
   * Returns a boolean indicating whether this artifact store is ready.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return dataStoreState != DataStoreState.STOPPED;
  }

  /**
   * Recursively finds artifact WARC files under a given base path.
   *
   * @param basePath The base path to scan recursively for WARC files.
   * @return Paths to WARC files under the given base path.
   */
  @Override
  public Collection<Path> findWarcs(Path basePath) throws IOException {
    log.trace("basePath = {}", basePath);

    File basePathFile = basePath.toFile();

    if (basePathFile.exists() && basePathFile.isDirectory()) {

      File[] dirObjs = basePathFile.listFiles();

      if (dirObjs == null) {
        // File#listFiles() can return null if the path doesn't exist or if there was an I/O error; we checked that
        // the path exists and is a directory earlier so it must be the former
        log.error("Unable to list directory contents [basePath = {}]", basePath);
        throw new IOException(String.format("Unable to list directory contents [basePath = %s]", basePath));
      }

      Collection<Path> warcFiles = new ArrayList<>();

      // Recursively look for WARCs
      // FIXME: Potential stack overflow here with sufficiently deep tree
      // Arrays.stream(dirObjs).map(x -> findWarcs(x.toPath())).forEach(warcFiles::addAll);
      for (File dir : Arrays.stream(dirObjs).filter(File::isDirectory).toArray(File[]::new)) {
        warcFiles.addAll(findWarcs(dir.toPath()));
      }

      // Add WARC files from this directory
      warcFiles.addAll(
          Arrays.stream(dirObjs)
              .filter(x -> x.isFile() &&
                  (x.getName().toLowerCase().endsWith(WARCConstants.DOT_WARC_FILE_EXTENSION) ||
                      x.getName().toLowerCase().endsWith(WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION)))
              .map(x -> x.toPath())
              .collect(Collectors.toSet())
      );

      // Return WARC files at this level
      return warcFiles;
    } else if (basePathFile.exists() && !basePathFile.isDirectory()) {
      log.error("Base path is not a directory! [basePath: {}]", basePath);
      throw new IllegalStateException("Base path is not a directory!");
    }

    log.warn("Path doesn't exist or was not a directory [basePath = {}]", basePath);

    return Collections.EMPTY_SET;
  }

  @Override
  public long getWarcLength(Path warcPath) {
    return warcPath.toFile().length();
  }

  @Override
  public URI makeStorageUrl(Path filePath, MultiValueMap<String, String> params) {
    UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString("file://" + filePath.toAbsolutePath().normalize());
    uriBuilder.queryParams(params);
    return uriBuilder.build().toUri();
  }

  @Override
  public OutputStream getAppendableOutputStream(Path filePath) throws IOException {
    return new BufferedOutputStream(new FileOutputStream(filePath.toFile(),
                                                         true),
                                    (256 * 1024));
  }

  @Override
  public InputStream getInputStreamAndSeek(Path filePath, long seek) throws IOException {
    log.trace("filePath = {}", filePath);
    log.trace("seek = {}", seek);

    InputStream inputStream = new FileInputStream(filePath.toFile());
    inputStream.skip(seek);

    return inputStream;
  }

  @Override
  public void initWarc(Path warcPath) throws IOException {
    File warcFile = warcPath.toFile();

    if (!warcFile.exists()) {
      mkdirs(warcPath.getParent());

      initFile(warcFile);

      try (OutputStream output = getAppendableOutputStream(warcPath)) {
        writeWarcInfoRecord(output);
      }
    }
  }

  protected void initFile(File file) throws IOException {
    FileUtils.touch(file);
  }

  @Override
  public boolean removeWarc(Path filePath) {
    return filePath.toFile().delete();
  }

  /**
   * Returns information about the storage size and free space
   *
   * @return A {@code StorageInfo}
   */
  @Override
  public StorageInfo getStorageInfo() {
    // Build a StorageInfo
    StorageInfo sum = new StorageInfo(ARTIFACT_DATASTORE_TYPE);
    Map<String,PlatformUtil.DF> mnts = new LinkedHashMap<>();
    List<StorageInfo> basePathSis = new ArrayList<>();
    PlatformUtil putil = PlatformUtil.getInstance();

    // Report the sum of the DF for each distinct mount point, include as
    // components all the base paths (even if they're have the same mount
    // point, as it's handy for testing)
    for (Path basePath : getBasePaths()) {
      PlatformUtil.DF df = putil.getDF(basePath.toString());
      if (df != null) {
        mnts.put(df.getMnt(), df);
        StorageInfo si = StorageInfo.fromDF(df);
        si.setPath(basePath.toString());
        basePathSis.add(si);
      }
    }
    PlatformUtil.DF oneDF = null;
    // Compute sum of DFs
    for (PlatformUtil.DF df : mnts.values()) {
      oneDF = df;
      // Sizes in DF are KB, StorageInfo is bytes
      sum.setSizeKB(sum.getSizeKB() + df.getSize());
      sum.setUsedKB(sum.getUsedKB() + df.getUsed());
      sum.setAvailKB(sum.getAvailKB() + df.getAvail());
    }

    // Set one-time StorageInfo fields
    sum.setName(String.join(",", mnts.keySet()));
    if (mnts.size() == 1) {
      // If only one, use percentages returns by DF
      sum.setPercentUsed(oneDF.getPercent());
      sum.setPercentUsedString(oneDF.getPercentString());
    } else {
      // Compute percent used as 1.0 - avail / size, as some FSs have a
      // "full" threshold that's lower than the total size
      sum.setPercentUsed(1.0d - (double)sum.getAvailKB() / (double)sum.getSizeKB());
      sum.setPercentUsedString(String.valueOf(Math.round(100.0 *
                                                         sum.getPercentUsed())) + "%");
    }
    if (basePathSis.size() > 1) {
      sum.setComponents(basePathSis);
    } else {
      sum.setPath(basePathSis.get(0).getPath());
    }
    // Return the sum
    return sum;

  }
}
