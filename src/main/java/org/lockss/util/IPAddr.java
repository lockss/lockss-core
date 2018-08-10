/*

Copyright (c) 2000-2018, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

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

package org.lockss.util;

import java.net.*;

/**
 * This class is used to wrap InetAddress instances, in order to intercept
 * and prevent unnecessary calls to the resolver.  Most of the methods
 * simply forward to the wrapped InetAddress
 */
public class IPAddr implements java.io.Serializable {

  private static boolean exerciseDNS = false;
  
  private InetAddress ina;

  /** Create a new IPAddr from an existing InetAddress.  Should be used
   * only to wrap InetAddresses extracted from datagrams, sockets, etc.  To
   * create an IPAddr from a hostname or address, use {@link
   * #getByName(String)}, {@link #getAllByName(String)} or {@link
   * #getLocalHost()}
   * @param ina an existing InetAddresses
   * @throws NullPointerException if ina is null
   */
  public IPAddr(InetAddress ina) {
    if (ina == null) {
      throw new NullPointerException();
    }
    this.ina = ina;
  }

  /** Set the wrapper class to maximize the use of DNS, by doing reverse
   * lookups in toString().
   * @param val if true, will minimize the use of DNS */
  public static void setExerciseDNS(boolean val) {
    exerciseDNS = val;
  }

  /** Extract the wrapped InetAddress to pass to a socket, datagram,
   * etc. */
  public InetAddress getInetAddr() {
    return ina;
  }

  public boolean isMulticastAddress() {
    return ina.isMulticastAddress();
  }

  public boolean isLoopbackAddress() {
    byte[] bytes = ina.getAddress();
    return bytes[0] == 127 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] != 0;
  }

  public String getHostName() {
    return ina.getHostName();
  }

  public byte[] getAddress() {
    return ina.getAddress();
  }

  public String getHostAddress() {
    return ina.getHostAddress();
  }

  public int hashCode() {
    return ina.hashCode();
  }

  public boolean equals(Object obj) {
    return (obj != null) && (obj instanceof IPAddr) &&
      ina.equals(((IPAddr)obj).getInetAddr());
  }

  public String toString() {
    if (exerciseDNS) {
      return ina.toString();
    } else {
      return ina.getHostAddress();
    }
  }

  public static IPAddr getByAddress(int[] ipBytes)
      throws UnknownHostException {
    return getByAddress( new byte[] {
        (byte)ipBytes[0], (byte)ipBytes[1], (byte)ipBytes[2], (byte)ipBytes[3]
    });
  }

  public static IPAddr getByAddress(byte[] ipBytes)
      throws UnknownHostException {
    return new IPAddr(InetAddress.getByAddress(ipBytes));
  }

  public static IPAddr getByName(String host) throws UnknownHostException {
    return new IPAddr(InetAddress.getByName(host));
  }

  public static IPAddr[] getAllByName(String host)
      throws UnknownHostException {
    InetAddress[] all = InetAddress.getAllByName(host);
    int len = all.length;
    IPAddr[] res = new IPAddr[len];
    for (int ix = 0; ix < len; ix++) {
      res[ix] = new IPAddr(all[ix]);
    }
    return res;
  }

  public static IPAddr getLocalHost() throws UnknownHostException {
    return new IPAddr(InetAddress.getLocalHost());
  }

  public static boolean isLoopbackAddress(String addr) {
    try {
      return getByName(addr).isLoopbackAddress();
    } catch (Exception e) {
      return false;
    }
  }
}
