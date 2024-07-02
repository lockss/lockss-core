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

package org.lockss.rs;

import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Factory for common LOCKSS Repository configurations.
 */
public class LockssRepositoryFactory {
    /**
     * Instantiates a LOCKSS repository in memory. Not intended for production use.
     *
     * @return A {@code VolatileLockssRepository} instance.
     */
    public static LockssRepository createVolatileRepository() throws IOException {
        return new VolatileLockssRepository();
    }

    /**
     * Creates a repository state directory under th given base path, if it doesn't exist yet.
     *
     * @param basePath Repository base directory
     * @return A {@link File} containing the path to the repository state directory.
     */
    private static File getRepositoryStateDir(File basePath) {
        File stateDir = basePath.toPath().resolve("state").toFile();
        if (!stateDir.exists()) {
            stateDir.mkdir();
        }

        return stateDir;
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository, with a locally persisted artifact index
     * and a repository state directory under the provided content directory.
     *
     * Deprecated. Creates the repository state directory under the first base path.
     *
     * @param basePath
     *          A {@link File} containing the base path of this LOCKSS Repository.
     * @param persistedIndexName
     *          A String with the name of the file where to persist the index.
     * @return A {@link LocalLockssRepository} instance.
     */
    @Deprecated
    public static LockssRepository createLocalRepository(File basePath, String persistedIndexName) throws IOException {
        return new LocalLockssRepository(getRepositoryStateDir(basePath), basePath, persistedIndexName);
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository. Uses a volatile index that must be rebuilt upon each
     * instantiation. Use of a volatile index is not recommended for large installations.
     *
     * @param stateDir
     *          A {@link File} containing the path to the state directory of this LOCKSS Repository.
     * @param basePath
     *          A {@link File} containing the base path of this LOCKSS Repository.
     * @param persistedIndexName
     *          A String with the name of the file where to persist the index.
     * @return A {@link LocalLockssRepository} instance.
     */
    public static LockssRepository createLocalRepository(File stateDir, File basePath, String persistedIndexName) throws IOException {
        return new LocalLockssRepository(stateDir, basePath, persistedIndexName);
    }

    /**
     * Instantiates a LOCKSS repository backed by a local data store with one or more base paths, and a locally
     * persisting artifact index.
     *
     * Deprecated. Creates the repository state directory under the first base path.
     *
     * @param basePaths          A {@link File[]} containing the base paths of the local data store.
     * @param persistedIndexName A {@link String} containing the locally persisted artifact index name.
     * @return A {@link LocalLockssRepository} backed by a local data store and locally persisted artifact index.
     * @throws IOException
     */
    @Deprecated
    public static LockssRepository createLocalRepository(File[] basePaths, String persistedIndexName) throws IOException {
        return new LocalLockssRepository(getRepositoryStateDir(basePaths[0]), basePaths, persistedIndexName);
    }

    /**
     * Instantiates a local filesystem based LOCKSS repository with a provided ArtifactIndex implementation for artifact
     * indexing. It does not invoke rebuilding the index, so it is only appropriate for implementations that persist.
     *
     * Deprecated. Creates the repository state directory under the first base path.
     *
     * @param basePath A {@code File} containing the base path of this LOCKSS Repository.
     * @param index    An {@code ArtifactIndex} to use as this repository's artifact index.
     * @return A {@code LocalLockssRepository} instance.
     */
    @Deprecated
    public static LockssRepository createLocalRepository(File basePath, ArtifactIndex index) throws IOException {
        return new LocalLockssRepository(getRepositoryStateDir(basePath), index, basePath);
    }

    /**
     * Instantiates a LOCKSS repository backed by a local data store with one or more base paths, and the provided
     * artifact index.
     *
     * Deprecated. Creates the repository state directory under the first base path.
     *
     * @param basePaths A {@link File[]} containing the base paths of the local data store.
     * @param index     An {@link ArtifactIndex} to use as this repository's artifact index.
     * @return A {@link LocalLockssRepository} backed by a local data store and locally persisted artifact index.
     * @throws IOException
     */
    @Deprecated
    public static LockssRepository createLocalRepository(File[] basePaths, ArtifactIndex index) throws IOException {
        return new LocalLockssRepository(getRepositoryStateDir(basePaths[0]), index, basePaths);
    }
}
