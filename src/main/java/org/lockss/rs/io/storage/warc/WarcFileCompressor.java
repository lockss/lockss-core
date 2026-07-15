/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University

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

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.lockss.log.L4JLogger;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Recompresses an <em>uncompressed</em> WARC file into a compressed WARC file
 * ({@code .warc.gz}) in which each WARC record is compressed as its own GZIP
 * member, matching the on-disk structure produced by {@link WarcArtifactDataStore}
 * and the IIPC/JWAT libraries.
 * <p>
 * This exists to repair compressed WARC files that were accidentally decompressed
 * with a plain {@code gunzip}. Naively re-running {@code gzip} on such a file yields
 * a single GZIP member spanning the whole file; both JWAT
 * ({@link WarcReaderFactory#getReaderCompressed}) and the wayback/webarchive-commons
 * readers iterate over GZIP members to find WARC records, so a single-member file is
 * read as a single (mis-parsed) record. This utility restores the one-member-per-record
 * layout those readers expect.
 * <p>
 * The transformation is byte-exact: JWAT is used only to locate WARC record boundaries
 * in the uncompressed input; each record's raw bytes (including the trailing CRLFs that
 * separate records) are then written to their own GZIP member. Gunzipping the output
 * therefore reproduces the input exactly.
 *
 * <h2>Command-line usage</h2>
 *
 * <pre>{@code
 * WarcFileCompressor [-h|--help] <input.warc> [output.warc.gz]}</pre>
 *
 * If {@code output.warc.gz} is omitted, it defaults to the input path with a
 * {@code .gz} suffix appended. On success the utility prints the number of GZIP
 * members (WARC records) written. It exits {@code 0} on success or for
 * {@code -h}/{@code --help}, and {@code 2} for invalid arguments.
 *
 * <h3>Running with Maven ({@code exec:java})</h3>
 *
 * The simplest way from a checkout, with no classpath wrangling. To recompress
 * {@code /foo/bar.warc} into {@code /foo/bar.warc.gz}:
 *
 * <pre>{@code
 * cd .../lockss-core
 * jenv local 17   # ensure JDK 17
 *
 * mvn -q compile exec:java \
 *   -Dexec.mainClass=org.lockss.rs.io.storage.warc.WarcFileCompressor \
 *   -Dexec.args="/foo/bar.warc"}</pre>
 *
 * To choose the output path explicitly, pass a second argument:
 *
 * <pre>{@code
 * mvn -q compile exec:java \
 *   -Dexec.mainClass=org.lockss.rs.io.storage.warc.WarcFileCompressor \
 *   -Dexec.args="/foo/bar.warc /foo/bar.warc.gz"}</pre>
 *
 * Note on {@code exec.args}: the space-separated form above is the common case. If a
 * path contains a space, use the comma-separated indexed form instead so arguments
 * are not split on the space:
 *
 * <pre>{@code
 * -Dexec.arguments="/path with space/bar.warc,/out.warc.gz"}</pre>
 *
 * <h3>Running with plain {@code java}</h3>
 *
 * Useful in scripts, avoiding Maven startup overhead. Assemble the classpath once, then
 * invoke the class directly:
 *
 * <pre>{@code
 * cd .../lockss-core
 * mvn -q compile dependency:build-classpath -Dmdep.outputFile=/tmp/cp.txt
 * java -cp "target/classes:$(cat /tmp/cp.txt)" \
 *   org.lockss.rs.io.storage.warc.WarcFileCompressor /foo/bar.warc}</pre>
 */
public class WarcFileCompressor {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** Size of the buffer used when copying raw record bytes. */
  private static final int COPY_BUFFER_SIZE = 8 * 1024;

  /** Size of the buffer used when reading the WARC to find record boundaries. */
  private static final int READ_BUFFER_SIZE = 32 * 1024;

  private WarcFileCompressor() {
    // Static utility; not instantiable.
  }

  /**
   * Recompresses an uncompressed WARC file, writing one GZIP member per WARC record.
   *
   * @param input  {@link Path} to the uncompressed source WARC file.
   * @param output {@link Path} to the compressed WARC file to write (typically ending in
   *               {@code .warc.gz}). Overwritten if it already exists.
   * @return The number of GZIP members (WARC records) written.
   * @throws IOException if reading the input or writing the output fails.
   */
  public static long compress(Path input, Path output) throws IOException {
    long inputLength = Files.size(input);

    // Pass 1: use JWAT to find the start offset of every WARC record in the
    // uncompressed input. getStartOffset() returns the start of the most recently
    // parsed record; iterator.hasNext() is what parses/caches the next one, so we
    // record the offset after hasNext() and before consuming the cached record.
    List<Long> recordStarts = findRecordStartOffsets(input);

    if (recordStarts.isEmpty()) {
      log.warn("No WARC records found in {}; writing empty output {}", input, output);
      // Still produce an output file so callers don't have to special-case emptiness.
      try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(output))) {
        // Nothing to write.
      }
      return 0;
    }

    // Build the list of member boundaries: [start_0, start_1, ..., start_{n-1}, inputLength].
    // Pass 2 reads the input sequentially from offset 0, so the first boundary must be 0.
    List<Long> boundaries = new ArrayList<>(recordStarts.size() + 2);
    if (recordStarts.get(0) != 0L) {
      // A well-formed WARC begins with a record at offset 0. If not, prepend 0 so no
      // leading bytes are dropped, but warn since this indicates an unexpected file.
      log.warn("First WARC record in {} starts at offset {}, not 0; leading bytes will be " +
               "written as a separate GZIP member", input, recordStarts.get(0));
      boundaries.add(0L);
    }
    boundaries.addAll(recordStarts);
    boundaries.add(inputLength);

    // Pass 2: read each byte range [boundaries[i], boundaries[i+1]) and write it as its
    // own GZIP member to the output. Ranges are contiguous and cover [0, inputLength), so
    // every byte of the input lands in exactly one member.
    long members = 0;
    byte[] buf = new byte[COPY_BUFFER_SIZE];

    try (InputStream in =
             new BufferedInputStream(Files.newInputStream(input), READ_BUFFER_SIZE);
         OutputStream out =
             new BufferedOutputStream(Files.newOutputStream(output))) {

      for (int i = 0; i < boundaries.size() - 1; i++) {
        long memberLength = boundaries.get(i + 1) - boundaries.get(i);

        // Shield 'out' so closing the per-member GZIPOutputStream finishes the member
        // (writing its trailer) and releases the native Deflater, without closing the
        // shared underlying stream. A new GZIPOutputStream then starts a new member.
        try (GZIPOutputStream gzip =
                 new GZIPOutputStream(CloseShieldOutputStream.wrap(out))) {
          copyExactly(in, gzip, memberLength, buf, input);
        }
        members++;
      }
    }

    log.debug2("Recompressed {} into {} with {} GZIP members", input, output, members);
    return members;
  }

  /**
   * Iterates over the uncompressed WARC file with JWAT and returns the start offset of
   * every WARC record.
   */
  private static List<Long> findRecordStartOffsets(Path input) throws IOException {
    List<Long> starts = new ArrayList<>();

    try (InputStream in =
             new BufferedInputStream(Files.newInputStream(input), READ_BUFFER_SIZE);
         WarcReader reader = WarcReaderFactory.getReaderUncompressed(in)) {

      Iterator<WarcRecord> iter = reader.iterator();
      while (iter.hasNext()) {
        // hasNext() has parsed and cached the record; getStartOffset() is now its start.
        starts.add(reader.getStartOffset());
        iter.next();
      }

      if (reader.getIteratorExceptionThrown() != null) {
        throw new IOException("Error iterating WARC records in " + input,
                              reader.getIteratorExceptionThrown());
      }
    }

    return starts;
  }

  /**
   * Copies exactly {@code length} bytes from {@code in} to {@code out}, throwing if the
   * input ends early.
   */
  private static void copyExactly(InputStream in, OutputStream out, long length,
                                  byte[] buf, Path input) throws IOException {
    long remaining = length;
    while (remaining > 0) {
      int toRead = (int) Math.min(buf.length, remaining);
      int n = in.read(buf, 0, toRead);
      if (n < 0) {
        throw new IOException("Unexpected end of " + input + " with " + remaining +
                              " bytes remaining to copy");
      }
      out.write(buf, 0, n);
      remaining -= n;
    }
  }

  private static final String USAGE =
      "Usage: " + WarcFileCompressor.class.getName() +
      " [-h|--help] <input.warc> [output.warc.gz]\n" +
      "\n" +
      "Recompresses an uncompressed WARC file into a compressed WARC (.warc.gz) in\n" +
      "which each WARC record is compressed as its own GZIP member. Use this to\n" +
      "repair a compressed WARC that was flattened by a plain `gunzip`.\n" +
      "\n" +
      "Arguments:\n" +
      "  <input.warc>       Path to the uncompressed source WARC file.\n" +
      "  [output.warc.gz]   Path to write the compressed WARC. Defaults to the input\n" +
      "                     path with a \".gz\" suffix appended. Overwritten if it exists.\n" +
      "\n" +
      "Options:\n" +
      "  -h, --help         Show this help and exit.";

  /**
   * Command-line entry point.
   * <p>
   * Usage: {@code WarcFileCompressor [-h|--help] <input.warc> [output.warc.gz]}
   * <p>
   * If the output path is omitted, it defaults to the input path with a {@code .gz}
   * suffix appended. Exits {@code 0} on success or for {@code -h}/{@code --help}, and
   * {@code 2} for invalid arguments.
   *
   * @param args Command-line arguments.
   * @throws IOException if reading the input or writing the output fails.
   */
  public static void main(String[] args) throws IOException {
    // Help: print usage to stdout and exit 0.
    for (String arg : args) {
      if ("-h".equals(arg) || "--help".equals(arg)) {
        System.out.println(USAGE);
        System.exit(0);
        return;
      }
    }

    // Reject unrecognized options (any leading '-...' that isn't a known flag).
    for (String arg : args) {
      if (arg.startsWith("-")) {
        System.err.println("Unknown option: " + arg);
        System.err.println(USAGE);
        System.exit(2);
        return;
      }
    }

    if (args.length < 1 || args.length > 2) {
      System.err.println(args.length < 1
                         ? "Missing required argument: <input.warc>"
                         : "Too many arguments (expected at most 2, got " + args.length + ")");
      System.err.println(USAGE);
      System.exit(2);
      return;
    }

    Path input = Paths.get(args[0]);
    Path output = args.length == 2 ? Paths.get(args[1]) : Paths.get(args[0] + ".gz");

    if (!Files.isRegularFile(input)) {
      System.err.println("Input is not a readable file: " + input);
      System.exit(2);
      return;
    }

    long members = compress(input, output);
    System.out.println("Wrote " + members + " GZIP member(s) to " + output);
  }
}
