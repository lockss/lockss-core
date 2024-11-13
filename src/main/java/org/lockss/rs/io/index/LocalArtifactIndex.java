/*
 * Copyright (c) 2018-2020, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.rs.io.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.storage.StorageInfo;
import org.lockss.log.L4JLogger;

/**
 * ArtifactData index implemented in memory and persisted in the local
 * filesystem.
 */
public class LocalArtifactIndex extends VolatileArtifactIndex {
    private final static L4JLogger log = L4JLogger.getLogger();

    /** Label to describe type of LocalArtifactIndex */
    public static String ARTIFACT_INDEX_TYPE = "Java-persist";

    // The location of persisted index.
    private File persistedIndex = null;

    /**
     * Constructor.
     *
     * @param basePath
     *          A File with the directory where to persist the index.
     * @param persistedIndexName
     *          A String with the name of the file where to persist the index.
     */
    public LocalArtifactIndex(File basePath, String persistedIndexName) {
        super();

        // Check whether there is a valid specification of the persisted index.
        if (basePath != null && (!basePath.exists() || basePath.isDirectory())
            && persistedIndexName != null
            && !persistedIndexName.trim().isEmpty()) {

            // Yes: Get the location of the persisted index.
            persistedIndex = new File(basePath, persistedIndexName);
            log.info("Setup persistence of index to file " + persistedIndex);

            // Populate the in-memory index with the persisted copy. 
            populateFromPersistence();
        } else {
            log.info("Persistence of index is disabled");
        }
    }

    /**
     * Returns information about the device the index is stored on: size,
     * free space, etc.
     * @return A {@code StorageInfo}
     */
    @Override
    public StorageInfo getStorageInfo() {
      if (persistedIndex == null) {
	return super.getStorageInfo();
      }
        // Mustn't use persistedIndex filename as it mightn't have been
        // created yet.  The parent directory is required to already exist.
        String parentDir = persistedIndex.getParent();
        return StorageInfo.fromDF(PlatformUtil.getInstance().getDF(parentDir))
          .setType(ARTIFACT_INDEX_TYPE)
          .setPath(persistedIndex.toString());
    }

    /**
     * Adds an artifact to the index.
     *
     * @param id
     *          A String with the identifier of the article to be added.
     * @param artifact
     *          An Artifact with the artifact to be added.
     */
    @Override
    protected void addToIndex(String id, Artifact artifact) {
      super.addToIndex(id, artifact);

      // Persist the just modified index.
      persist();
    }

    /**
     * Removes an artifact from the index.
     *
     * @param id
     *          A String with the identifier of the article to be removed.
     * @return an Artifact with the artifact that has been removed from the
     *         index.
     */
    @Override
    protected Artifact removeFromIndex(String id) {
      Artifact artifact = super.removeFromIndex(id);

      // Persist the just modified index.
      persist();
      return artifact;
    }

    /**
     * Commits to the index an artifact with a given text index identifier.
     *
     * @param artifactUuid
     *          A String with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    @Override
    public Artifact commitArtifact(String artifactUuid) {
      Artifact artifact = super.commitArtifact(artifactUuid);
      if (artifact != null) {
        persist();
      }
      return artifact;
    }

    @Override
    public Artifact updateStorageUrl(String artifactUuid, String storageUrl) throws IOException {
      Artifact artifact = super.updateStorageUrl(artifactUuid, storageUrl);
      if (artifact != null) {
        persist();
      }
      return artifact;
    }

    /**
     * Populates the in-memory index with the contents previously persisted.
     */
    protected void populateFromPersistence() {
        // Do nothing if there is nothing previously persisted to load. 
        if (persistedIndex == null || !persistedIndex.exists()
            || !persistedIndex.isFile()) {
	    return;
        }

        FileInputStream fis = null;
        ObjectInputStream ois = null;

        try {
            fis = new FileInputStream(persistedIndex);
            ois = new ObjectInputStream(fis);
            indexedByUuid = (ConcurrentHashMap<String, Artifact>)ois.readObject();
            for (Artifact artifact : indexedByUuid.values()) {
              indexedByUrlMap.put(artifact.getUri(), artifact);
            }
            log.info("Index successfully deserialized from file " + persistedIndex);
        } catch(IOException ioe) {
            log.error("Exception caught deserializing index from " + persistedIndex, ioe);
        } catch(ClassNotFoundException cnfe) {
            log.error("Exception caught deserializing index from " + persistedIndex, cnfe);
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ioe) {
                    log.warn("Ignored exception caught closing ObjectInputStream from "
                        + persistedIndex);
                }
            }

            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                    log.warn("Ignored exception caught closing FileInputStream from "
                        + persistedIndex);
                }
            }
        }
    }

    /**
     * Persists the contents of the in-memory index.
     */
    protected void persist() {
        // Do nothing if persistence is not setup. 
        if (persistedIndex == null) {
          return;
        }

        FileOutputStream fos = null;
        ObjectOutputStream oos = null;

        try {
            fos = new FileOutputStream(persistedIndex);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(indexedByUuid);
        } catch (IOException ioe) {
            log.error("Exception caught serializing index to " + persistedIndex, ioe);
        } finally {
            if (oos != null) {
                try {
                    oos.close();
                } catch (IOException ioe) {
                    log.warn("Ignored exception caught closing ObjectOutputStream to "
                        + persistedIndex);
                }
            }

            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ioe) {
                    log.warn("Ignored exception caught closing FileOutputStream to "
                        + persistedIndex);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "[LocalArtifactIndex persistedIndex=" + persistedIndex
            + ",index.size() = " + indexedByUuid.size() + "]";
    }
}
