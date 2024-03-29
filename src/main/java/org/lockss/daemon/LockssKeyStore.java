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

package org.lockss.daemon;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.*;
import java.security.cert.*;
import javax.net.ssl.*;

import org.bouncycastle.operator.OperatorCreationException;
import org.lockss.app.*;
import org.lockss.util.*;
import org.lockss.util.io.FileUtil;
import org.lockss.config.*;

/** Wrapper around a keystore to manager passwords and loading and manager
 * KeyManagerFactory */
public class LockssKeyStore {
  protected static Logger log = Logger.getLogger();

  public static  enum LocationType {File, Resource, Url};

  String name;
  String type;
  String location;
  LocationType ltype;
  String password;
  String keyPassword;
  String keyPasswordFile;
  KeyStore keystore;
  KeyManagerFactory kmf;
  TrustManagerFactory tmf;
  boolean mayCreate = false;
  boolean loaded = false;

  LockssKeyStore(String name) {
    if (name == null) {
      throw new NullPointerException();
    }
    this.name = name;
  }

  String getName() {
    return name;
  }

  void setName(String val) {
    name = val;
  }

  String getType() {
    return type;
  }

  void setType(String val) {
    type = val;
  }

  String getLocation() {
    return location;
  }

  void setLocation(String val, LocationType ltype) {
    location = val;
    this.ltype = ltype;
  }

  LocationType getLocationType() {
    return ltype;
  }

  public boolean isSameLocation(LockssKeyStore o) {
    return o != null
      && getLocationType().equals(o.getLocationType())
      && getLocation().equals(o.getLocation());
  }      

  void setMayCreate(boolean val) {
    if (val && ltype != LocationType.File) {
      throw new IllegalStateException("Only KeyStores of type File can be created");
    }
    mayCreate = val;
  }

  boolean getMayCreate() {
    return mayCreate;
  }

  void setPassword(String val) {
    password = val;
  }

  void setKeyPassword(String val) {
    keyPassword = val;
  }

  void setKeyPasswordFile(String val) {
    keyPasswordFile = val;
  }

  KeyManagerFactory getKeyManagerFactory() {
    if (keyPassword == null) {
      throw new IllegalStateException("No key password supplied; can't get KeyManagerFactory");
    }
    return kmf;
  }

  TrustManagerFactory getTrustManagerFactory() {
    return tmf;
  }

  KeyStore getKeyStore() {
    return keystore;
  }

  /** Load the keystore from a file */
  synchronized void load() throws UnavailableKeyStoreException {
    load(null);
  }

  synchronized void load(LockssApp lapp) throws UnavailableKeyStoreException {
    if (loaded) {
      return;
    }
    if (StringUtil.isNullString(location))
      throw new NullPointerException("location must be a non-null string");
    if (lapp == null) {
      lapp = LockssApp.getLockssApp();
    }
    try {
      if (keyPassword == null && keyPasswordFile != null) {
        if (keyPasswordFile.startsWith("secret:")) {
          String sname = keyPasswordFile.substring("secret:".length());
          keyPassword = lapp.getClientCredentialsAsString(sname);
        } else {
          keyPassword = FileUtil.readPasswdFile(keyPasswordFile);
        }
      }
      if (mayCreate) {
	File file = new File(location);
	if (!file.exists()) {
	  createKeyStore();
	}
      }
      loadKeyStore();
      // Create KeyManagerFactory iff a key password was supplied.
      if (keyPassword != null) {
	createKeyManagerFactory();
      }
      createTrustManagerFactory();
      loaded = true;
      log.info("Loaded keystore: " + name);
      // Useful for debugging but can leave private keystore data in the
      // log.
//       if (log.isDebug3()) logKeyStore(keystore, null);
    } catch (Exception e) {
      // logged at higher level
      throw new UnavailableKeyStoreException(e);
    }
  }

  /** Create a keystore with a self-signed certificate */
  void createKeyStore()
      throws CertificateException,
      IOException,
      InvalidKeyException,
      KeyStoreException,
      NoSuchAlgorithmException,
      NoSuchProviderException,
      SignatureException,
      UnrecoverableKeyException,
      InvalidAlgorithmParameterException,
      OperatorCreationException {
    log.info("Creating keystore: " + location);
    if (StringUtil.isNullString(keyPassword)) {
      throw new NullPointerException("keyPassword must be non-null string");
    }					       
    String fqdn = ConfigManager.getPlatformHostname();
    if (StringUtil.isNullString(password)) {
      // keystore password is required when creating keys.  If none
      // supplied, use machine's fqdn, or "unknown" if unknown.
      if (StringUtil.isNullString(fqdn)) {
	password = "password";
      } else {
	password = fqdn;
      }
    }
    // fqdn is X500 common name, and base for keystore entry aliases.
    if (StringUtil.isNullString(fqdn)) {
      fqdn = "unknown";
    }
    Properties p = new Properties();
    p.put(KeyStoreUtil.PROP_KEYSTORE_FILE, getLocation());
    if (getType() != null) {
      p.put(KeyStoreUtil.PROP_KEYSTORE_TYPE, getType());
    }
    p.put(KeyStoreUtil.PROP_KEY_ALIAS, fqdn + ".key");
    p.put(KeyStoreUtil.PROP_CERT_ALIAS, fqdn + ".cert");
    p.put(KeyStoreUtil.PROP_X500_NAME, makeX500Name(fqdn));

    p.put(KeyStoreUtil.PROP_KEYSTORE_PASSWORD, password);
    p.put(KeyStoreUtil.PROP_KEY_PASSWORD, keyPassword);
    if (log.isDebug2()) log.debug2("Creating keystore from props: " + p);
    KeyStore ks = KeyStoreUtil.createKeyStore(p);
  }

  String makeX500Name(String fqdn) {
    return "CN=" + fqdn + ", O=LOCKSS box";
  }

  /** Load the keystore from the file */
  void loadKeyStore()
      throws KeyStoreException,
	     IOException,
	     NoSuchAlgorithmException,
	     NoSuchProviderException,
	     CertificateException {
    char[] passchar = null;
    if (!StringUtil.isNullString(password)) {
      passchar = password.toCharArray();
    }

    try (InputStream ins = getInputStream()) {
      // ignore specified type when loading
      keystore = KeyStoreUtil.loadKeystoreOfUnknownType(getInputStream(),
                                                        password);
//       if (getType() != null) {
//         KeyStore ks = KeyStore.getInstance(getType());
//         ks.load(ins, passchar);
//         keystore = ks;
//       } else {
//         keystore = KeyStoreUtil.loadKeystoreOfUnknownType(getInputStream(),
//                                                           password);
//       }
    }
  }

  InputStream getInputStream() throws IOException {
    InputStream ins;
    switch (ltype) {
    case File:
      ins = new FileInputStream(new File(location));
      break;
    case Resource:
      ins = getClass().getClassLoader().getResourceAsStream(location);
      break;
    case Url:
      if (UrlUtil.isHttpOrHttpsUrl(location)) {
	ins = UrlUtil.openInputStream(location);
      } else {
	URL keystoreUrl = new URL(location);
	ins = keystoreUrl.openStream();
      }
      break;
    default:
      throw new IllegalStateException("Impossible keystore location type: "
				      + ltype);
    }
    return new BufferedInputStream(ins);
  }

  /** Create a KeyManagerFactory from the keystore and key password */
  void createKeyManagerFactory()
      throws NoSuchAlgorithmException,
	     KeyStoreException,
	     UnrecoverableKeyException {
    kmf =
      KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keystore, keyPassword.toCharArray());
  }

  /** Create a TrustManagerFactory from the keystore */
  void createTrustManagerFactory()
      throws KeyStoreException,
	     NoSuchAlgorithmException,
	     CertificateException {
    tmf = 
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(keystore);
  }

  // private debug output of keystore
  private void logKeyStore(KeyStore ks, char[] keyPassword) {
    log.debug3("start of key store");
    try {
      for (Enumeration en = ks.aliases(); en.hasMoreElements(); ) {
        String alias = (String)en.nextElement();
        if (ks.isCertificateEntry(alias)) {
          java.security.cert.Certificate cert = ks.getCertificate(alias);
          if (cert == null) {
	    log.debug3("Null cert chain for: " + alias);
          } else {
            log.debug3("Cert chain for " + alias + " is " + cert);
          }
        } else if (ks.isKeyEntry(alias)) {
  	  Key privateKey = ks.getKey(alias, keyPassword);
  	  log.debug3(alias + " key " + privateKey.getAlgorithm()
		     + "/" + privateKey.getFormat());
        } else {
  	  log.debug3(alias + " neither key nor cert");
        }
      }
    } catch (Exception e) {
      log.error("logKeyStore() threw", e);
    }
  }

  public boolean equals(Object other) {
    if (! (other instanceof LockssKeyStore)) {
      return false;
    }
    LockssKeyStore o = (LockssKeyStore)other;
    return name.equals(o.name)
      && StringUtil.equalStrings(type, o.type)
      && StringUtil.equalStrings(location, o.location)
      && ltype == o.ltype
      && mayCreate == o.mayCreate
      && StringUtil.equalStrings(password, o.password)
      && StringUtil.equalStrings(keyPassword, o.keyPassword)
      && StringUtil.equalStrings(keyPasswordFile, o.keyPasswordFile);
  }

  public int hashCode() {
    int hash = 0x272053;
    hash += name.hashCode();
    return hash;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer(40);
    sb.append("[LKS: ");
    sb.append(name);
    sb.append(", ");
    sb.append(location);
    sb.append("]");
    return sb.toString();
  }

  public class UnavailableKeyStoreException extends Exception {
    UnavailableKeyStoreException(Throwable cause) {
      super(cause);
    }
  }

}
