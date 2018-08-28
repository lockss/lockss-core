/*
* $Id$
 */

/*

Copyright (c) 2000-2005 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.poller;

import org.mortbay.util.B64Code;
import org.lockss.protocol.*;
import java.util.Arrays;
import org.lockss.util.*;
import org.lockss.util.io.LockssSerializable;

/**
 * Vote stores the information need to replay a single vote. These are needed
 * to run a repair poll.
 */
public class Vote implements LockssSerializable {
  private PeerIdentity voterID = null;
  protected boolean agree = false;
  private ActiveVote activeInfo = null;
  private static IdentityManager idMgr = null;
  static Logger theLog = Logger.getLogger();

  protected Vote() {
  }


  Vote(byte[] challenge, byte[] verifier, byte[] hash,
       PeerIdentity id, boolean agree) {
    this.voterID = id;
    this.agree = agree;
  }

  Vote(Vote vote) {
    this(vote.getChallenge(),vote.getVerifier(),vote.getHash(),
         vote.voterID,vote.agree);
  }

  Vote(LcapMessage msg, boolean agree) {
    // XXX: Split into V1Vote and V3Vote.  Stubbed for V3.
  }

  public static void setIdentityManager(IdentityManager im) {
    idMgr = im;
  }

  /**
   * Create a Vote object from strings.  used by VoteBean
   * @param challengeStr B64 encoded string of the challenge
   * @param verifierStr B64 encoded string of the challenge
   * @param hashStr B64 encoded string of the challenge
   * @param idStr the string representation of the voter's PeerIdentity
   * @param agree boolean indiciating agreement or disagreement
   * @return newly created Vote object
   */

  protected Vote makeVote(String challengeStr,
			  String verifierStr,
			  String hashStr,
			  String idStr,
			  boolean agree) {
    Vote vote = null;
    try {
      PeerIdentity pid = idMgr.stringToPeerIdentity(idStr);
      vote = new Vote();
      vote.voterID = pid;
      vote.agree = agree;
      // XXX the ActiveVote info should not be needed
	    activeInfo = null;
    } catch (IdentityManager.MalformedIdentityKeyException e) {
      theLog.error("Malformed peer identity: " + idStr, e);
    }
    return vote;
  }

  /**
   * Discard the information no longer needed after the poll has finished.
   */
  protected void pollClosed() {
    activeInfo = null;
  }


  public String toString() {
    StringBuffer sbuf = new StringBuffer();
    sbuf.append(agree ? "[YesVote: " : "[NoVote: ");
    sbuf.append("from " + voterID);
    sbuf.append("]");
    return sbuf.toString();
  }

  /**
   * Return the Identity key of the voter
   * @return <code>PeerIdentity</code> the id
   */
  public PeerIdentity getVoterIdentity() {
    return voterID;
  }

  /**
   * return whether we agreed or disagreed with this vote
   * @return booleean true if we agree; false otherwise
   */
  public boolean isAgreeVote() {
    return agree;
  }

  /**
   * Return the challenge bytes of the voter
   * @return the array of bytes of the challenge
   */
  public byte[] getChallenge() {
    byte[] ret = null;
    if (activeInfo != null) {
      ret = activeInfo.getChallenge();
    }
    return ret;
  }

  /**
   * Return the challenge bytes as a string
   * @return a String representing the challenge
   */
  public String getChallengeString() {
    String ret = null;
    if (activeInfo != null) {
      ret = String.valueOf(B64Code.encode(activeInfo.getChallenge()));
    }
    return ret;
  }

  /**
   * Return the bytes of the hash computed by this voter
   * @return the array of bytes of the hash
   */
  public byte[] getHash() {
    byte[] ret = null;
    if (activeInfo != null) {
      ret = activeInfo.getHash();
    }
    return ret;
  }

  /**
   * Return the hash bytes as a string
   * @return a String representing the hash
   */
  public String getHashString() {
    String ret = null;
    if (activeInfo != null) {
      byte[] hash = activeInfo.getHash();
      if(hash != null) {
	ret = String.valueOf(B64Code.encode(hash));
      }
    }
    return ret;
  }

  /**
   * Return the verifer bytes of the voter
   * @return the array of bytes of the verifier
   */
  public byte[] getVerifier() {
    byte[] ret = null;
    if (activeInfo != null) {
      ret = activeInfo.getVerifier();
    }
    return ret;
  }

  /**
   * Return the verifier bytes as a string
   * @return a String representing the verifier
   */
  public String getVerifierString() {
    String ret = null;
    if (activeInfo != null) {
      byte[] verifier = activeInfo.getVerifier();
      ret = String.valueOf(B64Code.encode(verifier));
    }
    return ret;
  }

  public String getPollKey() {
    return getChallengeString();
  }

}

class ActiveVote implements LockssSerializable {
  byte[] challenge;
  byte[] verifier;

  byte[] getChallenge() {
    return challenge;
  }

  byte[] getVerifier() {
    return verifier;
  }

  byte[] getHash() {
    return null;
  }
}

