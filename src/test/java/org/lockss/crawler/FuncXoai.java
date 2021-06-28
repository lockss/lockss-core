/*

Copyright (c) 2000-2021, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.crawler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.io.InputStream;

import org.lockss.test.LockssTestCase;

import org.dspace.xoai.model.xoai.XOAIMetadata;
import org.dspace.xoai.serviceprovider.parsers.MetadataParser;
import org.dspace.xoai.services.api.MetadataSearch;

public class FuncXoai extends LockssTestCase {
    public void testParseMetadata() throws Exception {
        InputStream input = getResourceAsStream("xoai.xml");

        XOAIMetadata metadata = new MetadataParser().parse(input);
        MetadataSearch<String> searcher = metadata.searcher();
        assertThat(metadata.getElements().size(), equalTo(1));
        assertThat(searcher.findOne("dc.creator"), equalTo("Sousa, Jesus Maria Angelica Fernandes"));
        assertThat(searcher.findOne("dc.date.submitted"), equalTo("1995"));
        assertThat(searcher.findAll("dc.subject").size(), equalTo(5));
    }

}