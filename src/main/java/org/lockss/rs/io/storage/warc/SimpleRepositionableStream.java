/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University
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

package org.lockss.rs.io.storage.warc;

import java.io.*;

import org.apache.commons.io.input.ProxyInputStream;
import org.archive.io.ArchiveRecordHeader;

import it.unimi.dsi.fastutil.io.RepositionableStream;
import org.archive.io.RepositionableInputStream;

/**
 * <p>
 * A naive, non-buffered implementation of {@link RepositionableStream}, that
 * correctly keeps track of the position and returns it with
 * {@link #position()}, but resets it to whatever value is passed to
 * {@link #position(long)} without any error checking or action.
 * </p>
 * <p>
 * The purpose of this class is to circumvent a bug of sorts in
 * {@code org.archive.io.warc.WARCRecord.parseHeaders(InputStream, String, long, boolean)}.
 * That method receives a generic {@link InputStream}, and if it is of type
 * {@link RepositionableStream}, casts it and uses it to compute the length of
 * headers. Only instances of {@code com.google.common.io.CountingInputStream}
 * and
 * {@link org.archive.util.zip.GZIPMembersInputStream.GZIPMembersInputStream(InputStream)}
 * seem to ever be passed down, neither of which implements
 * {@link RepositionableStream}. Thus the conditional cast is never made and the
 * returned {@link ArchiveRecordHeader}'s
 * {@link ArchiveRecordHeader#getLength()} method does not return the correct
 * value for the WARC record length. Wrapping the {@link InputStream} in a
 * {@link RepositionableInputStream} does not work because it extends
 * {@link BufferedInputStream} and consumes bytes from the underlying stream
 * past the current WARC record. But the {@link RepositionableStream} only seems
 * to be used to count bytes being read, not for any actual repositioning, so
 * this naive minimum suffices to get the correct WARC header length.
 * </p>
 * 
 * @since 1.12.0
 */
public class SimpleRepositionableStream extends ProxyInputStream implements RepositionableStream {

  protected long position;
  
  public SimpleRepositionableStream(InputStream inputStream) {
    super(inputStream);
    position = 0L;
  }
  
  @Override
  public long position() throws IOException {
    return position;
  }

  @Override
  public void position(long newPosition) throws IOException {
    position = newPosition;
  }

  @Override
  public int read() throws IOException {
    int ret = super.read();
    if (ret != -1) {
      ++position;
    }
    return ret;
  }

  @Override
  public int read(byte[] bts) throws IOException {
    int ret = super.read(bts);
    if (ret != -1) {
      position += ret;
    }
    return ret;
  }
  
  @Override
  public int read(byte[] bts, int off, int len) throws IOException {
    int ret = super.read(bts, off, len);
    if (ret != -1) {
      position += ret;
    }
    return ret;
  }
  
}
