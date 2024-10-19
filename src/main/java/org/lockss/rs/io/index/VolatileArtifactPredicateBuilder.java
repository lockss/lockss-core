/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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

import org.lockss.util.rest.repo.model.Artifact;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Builder of artifact filtering predicates.
 */
public class VolatileArtifactPredicateBuilder {
    // The individual filtering predicates.
    private final Set<Predicate<Artifact>> predicates = new HashSet<>();

    /**
     * Adds a filtering predicate by Archival Unit identifier.
     * 
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public VolatileArtifactPredicateBuilder filterByAuid(String auid) {
        if (auid != null)
            predicates.add(x -> x.getAuid().equals(auid));
        return this;
    }

    /**
     * Adds a filtering predicate by indexing commit status.
     * 
     * @param committedStatus
     *          A Boolean with the commit status.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public VolatileArtifactPredicateBuilder filterByCommitStatus(Boolean committedStatus) {
        if (committedStatus != null)
            predicates.add(artifact -> artifact.getCommitted().equals(committedStatus));
        return this;
    }

    /**
     * Adds a filtering predicate by namespace.
     * 
     * @param namespace
     *          A String with the namespace.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public VolatileArtifactPredicateBuilder filterByNamespace(String namespace) {
        predicates.add(artifact -> artifact.getNamespace().equals(namespace));
        return this;
    }

    /**
     * Adds a filtering predicate by URI prefix.
     *
     * If the URI prefix is {@code null}, then a filter is not applied.
     * 
     * @param prefix
     *          A String with the URI prefix.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public VolatileArtifactPredicateBuilder filterByURIPrefix(String prefix) {
      if (prefix != null) {
          // Q: Perhaps it would be better to throw an IllegalArgumentException?
          predicates.add(artifact -> artifact.getUri().startsWith(prefix));
      }

      return this;
    }

    /**
     * Adds a filtering predicate by full URI.
     * 
     * @param uri
     *          A String with the URI.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public VolatileArtifactPredicateBuilder filterByURIMatch(String uri) {
      predicates.add(artifact -> artifact.getUri().equals(uri));
      return this;
    }

    /**
     * Adds a filtering predicate by version.
     * 
     * @param version
     *          A String with the version.
     * @return an ArtifactPredicateBuilder with this object.
     */
    public VolatileArtifactPredicateBuilder filterByVersion(Integer version) {
        if (version != null)
            predicates.add(artifact -> artifact.getVersion().equals(version));
        return this;
    }

    /**
     * Builds the full artifact filtering predicate.
     * 
     * @return a {@code Predicate<Artifact>} with the full artifact
     *         filtering predicate.
     */
    public Predicate<Artifact> build() {
        return predicates.stream().reduce(Predicate::and).orElse(include -> false);
    }
}
