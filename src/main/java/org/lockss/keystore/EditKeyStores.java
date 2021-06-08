/*

Copyright (c) 2000-2021 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.keystore;

import java.util.*;
import java.io.*;
import java.net.*;
import java.security.*;
import java.security.cert.*;
import javax.net.ssl.*;

import org.lockss.util.*;

/**
 * A tool to build key stores for LCAP over SSL support in LOCKSS
 */
public class EditKeyStores {
  protected static Logger log = Logger.getLogger();

  private static SecureRandom testOnlySecureRandom = null;

  /** Unit tests can stall waiting for the random number generator to
   * collect enough entropy.  Test should use this to install a RNG that
   * doesn't rely on system entropy. This is also used as a signal that
   * main() shouldn't call System.exit() */
  static void setTestOnlySecureRandom(SecureRandom rng) {
    testOnlySecureRandom = rng;
  }

  static boolean isTesting() {
    return testOnlySecureRandom != null;
  }

  private String inDir;
  private String outDir;
  private String ksType;
  private File pubFile;
  private String pubPass;
  private List<String> hosts;

  private String result;
  private boolean success = false;

  private EditKeyStores(Builder b) {
    this.inDir = b.inDir;
    this.outDir = b.outDir;
    this.ksType = b.ksType;
    this.pubFile = b.pubFile != null ? new File(b.pubFile) : null;
    this.pubPass = b.pubPass;
    this.hosts = b.hosts;
  }

  public String getResult() {
    return result;
  }

  public boolean isSuccess() {
    return success;
  }

  public boolean generate() throws Exception {
    File outDirFile = new File(outDir);
    if (!outDirFile.isDirectory()) {
      outDirFile.mkdirs();
    }
    SecureRandom rng = isTesting() ? testOnlySecureRandom : getSecureRandom();
    if (pubFile != null) {
      if (StringUtil.isNullString(pubPass)) {
        log.info("No public keystore password supplied, using \"password\"");
        pubPass = "password";
      }
      KeyStoreUtil.createSharedPLNKeyStores(ksType, outDirFile, hosts,
                                            pubFile, pubPass, rng);
      log.info("Keystores generated in " + outDirFile
               + ", public keystore in " + pubFile);
    } else {
      KeyStoreUtil.createPLNKeyStores(ksType, new File(inDir), outDirFile,
                                      hosts, rng);
      log.info("Keystores generated in " + outDirFile);
    }
    result = "Keystores generated in " + outDirFile;
    success = true;
    return true;
  }

  private static void usage() {
    System.out.println("Usage:");
    System.out.println("   EditKeyStores [-t keyStoreType] [-i inDir] [-o outDir] host1 host2 ...");
    System.out.println("      or");
    System.out.println("   EditKeyStores [-t keyStoreType] -s pub-keystore [-p pub-password] [-o outDir] host1 host2 ...");
    System.out.println("");
    System.out.println("Creates, in outDir, a private key for each host.  In the first variant");
    System.out.println("each keystore contains the host's private key and public certificates");
    System.out.println("for all the other hosts.  In the second variant the public certificates");
    System.out.println("are written into a shared, public keystore and each hosts keystore contains");
    System.out.println("only its own private key and public cert.");
  }

  public static void main(String[] cmdline) throws Exception {
    try {
      Args args = new Args(cmdline);
      if (!args.parse()) {
        usage();
        if (!isTesting()) {
          System.exit(1);
        }
      } else {
        EditKeyStores inst = args.build();
        if (!inst.generate()) {
          if (!isTesting()) {
            System.exit(1);
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed, keystores not generated", e);
      if (!isTesting()) {
        System.exit(1);
      }
      throw e;
    }
  }

  public static EditKeyStores generateFromCmdline(String cmdline)
      throws Exception {
    Args args = new Args(cmdline);
    if (!args.parse()) {
      throw new IllegalArgumentException("Can't parse command line");
    }
    EditKeyStores inst = args.build();
    inst.clientGenerate();
    return inst;
  }

  boolean clientGenerate() {
    try {
      if (generate()) {
        success = true;
        return true;
      } else {
        result = "Failed, keystores not generated";
      }
    } catch (Exception e) {
      log.error("Failed, keystores not generated", e);
      result = "Failed, keystores not generated: " + e.toString();
    }
    return false;
  }

  public static class Builder {
    private String inDir;
    private String outDir;
    private String ksType;
    private String pubFile;
    private String pubPass;
    private List<String> hosts;

    public EditKeyStores build() {
      return new EditKeyStores(this);
    }

    public Builder setHosts(List<String> hosts) {
      this.hosts = hosts;
      return this;
    }

    public Builder setInDir(String inDir) {
      this.inDir = inDir;
      return this;
    }

    public Builder setOutDir(String outDir) {
      this.outDir = outDir;
      return this;
    }

    public Builder setKsType(String ksType) {
      this.ksType = ksType;
      return this;
    }

    public Builder setPubFile(String pubFile) {
      this.pubFile = pubFile;
      return this;
    }

    public Builder setPubPass(String pubPass) {
      this.pubPass = pubPass;
      return this;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }


  static class Args {
    Builder b = newBuilder();
    String[] args;

    List hostlist = new ArrayList();

    Args(String[] args) {
      this.args = args;
    }

    Args(String args) {
      this.args = StringUtil.breakAt(args, " ").toArray(new String[0]);
    }

    EditKeyStores build() {
      return b.build();
    }

    /**
     * Parse args
     */
    boolean parse() {
      if (args == null || args.length == 0) {
        return false;
      }

      try {
        for (int ix = 0; ix < args.length; ix++) {
          if (args[ix].startsWith("-")) {
            if ("-t".equals(args[ix])) {
              b.setKsType(args[++ix]);
              log.debug("KeyStore type " + b.ksType);
              continue;
            }
            if ("-i".equals(args[ix])) {
              b.setInDir(args[++ix]);
              log.debug("Input directory " + b.inDir);
              continue;
            }
            if ("-o".equals(args[ix])) {
              b.setOutDir(args[++ix]);
              log.debug("Output directory " + b.outDir);
              continue;
            }
            if ("-s".equals(args[ix])) {
              b.setPubFile(args[++ix]);
              log.debug("Public keystore " + b.pubFile);
              continue;
            }
            if ("-p".equals(args[ix])) {
              b.setPubPass(args[++ix]);
              continue;
            }
            return false;
          } else {
            hostlist.add(args[ix]);
          }
        }
      } catch (Exception e) {
        return false;
      }
      if (hostlist.isEmpty()) {
        return false;
      }
      b.setHosts(hostlist);
      return true;
    }
  }

  static SecureRandom getSecureRandom() {
    try {
      return SecureRandom.getInstance("SHA1PRNG", "SUN");
    } catch (Exception ex) {
      log.error("Couldn't get SecureRandom: " + ex);
      return null;
    }
  }
}
