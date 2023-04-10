/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.rs.util;

import java.io.ByteArrayInputStream;

import org.junit.jupiter.api.*;
import org.lockss.util.rest.repo.util.NamedInputStreamResource;
import org.lockss.util.test.LockssTestCase5;

/**
 * Test class for {@code org.lockss.laaws.rs.util.NamedInputStreamResource}.
 */
public class TestNamedInputStreamResource extends LockssTestCase5 {
    private static final String RESOURCE_BYTES = "foobar";
    private static final String RESOURCE_NAME = "xyzzy";
    private static final String RESOURCE_DESC = "Named InputStream resource [resource loaded through InputStream]";

    private NamedInputStreamResource namedResource;

    @BeforeEach
    public void setUp() throws Exception {
       namedResource = new NamedInputStreamResource(RESOURCE_NAME, new ByteArrayInputStream(RESOURCE_BYTES.getBytes()));
    }

    @Test
    public void getFilename() {
        assertEquals(RESOURCE_NAME, namedResource.getFilename());
    }

    @Test
    public void getDescription() {
        assertEquals(RESOURCE_DESC, namedResource.getDescription());
    }
}