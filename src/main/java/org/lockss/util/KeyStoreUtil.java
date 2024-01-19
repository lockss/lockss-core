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

package org.lockss.util;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.RSAKeyGenParameterSpec;
import java.util.*;
import java.io.*;
import java.security.*;
import java.security.cert.*;

import org.apache.commons.io.*;
import org.bouncycastle.asn1.ASN1InputStream;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.lockss.app.*;
import org.lockss.daemon.*;
import org.lockss.config.*;
import org.lockss.util.time.TimeBase;

/**
 * Utilities for creating keystores
 */
public class KeyStoreUtil {

  private static final Logger log = Logger.getLogger();

  // The Oracle JDK bug pages linked from the github issue suggest
  // this has been fixed.  I'm leaving the workaround here for now,
  // commented out, as reference in case we see it again.

//   // Workaround for Java bug ID 9070059.  See
//   // https://github.com/bcgit/bc-java/issues/941
//   static {
//     fixBug9070059();
//   }

//   @SuppressWarnings("sunapi")
//   private static void fixBug9070059() {
//     try {
//       sun.security.x509.AlgorithmId.get("PBEWithSHA1AndDESede");
//     } catch (NoSuchAlgorithmException e) {}
//   }

  // Large set of args passed in Properties or Configuration
  /** File to write keystore to */
  public static final String PROP_KEYSTORE_FILE = "File";
  /** Optional, default is PKCS12 */
  public static final String PROP_KEYSTORE_TYPE = "Type";
  /** KeyStore password */
  public static final String PROP_KEYSTORE_PASSWORD = "Password";
  /** Private key password */
  public static final String PROP_KEY_PASSWORD = "KeyPassword";
  /** Default MyKey */
  public static final String PROP_KEY_ALIAS = "KeyAlias";
  /** Default MyCert */
  public static final String PROP_CERT_ALIAS = "CertAlias";
  /** Default RSA */
  public static final String PROP_KEY_ALGORITHM = "KeyAlgorithm";
  /** Default SHA256WithRSA */
  public static final String PROP_SIG_ALGORITHM = "SigAlgorithm";
  /** X500Name.  Default 5 years */
  public static final String PROP_X500_NAME = "X500Name";
  /** Default 2048 */
  public static final String PROP_KEY_BITS = "KeyBits";
  /** Seconds.  Default 5 years */
  public static final String PROP_EXPIRE_IN = "ExpireIn";
  

  public static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

  public static final String DEFAULT_KEY_ALIAS = "MyKey";
  public static final String DEFAULT_CERT_ALIAS = "MyCert";
  public static final String DEFAULT_KEY_ALGORITHM = "RSA";
  public static final String DEFAULT_SIG_ALGORITHM = "SHA256WithRSA";
  public static final String DEFAULT_X500_NAME = "CN=LOCKSS box";
  public static final int DEFAULT_KEY_BITS = 2048;
  public static final long DEFAULT_EXPIRE_IN = 5 * Constants.YEAR / 1000;


  public static String randomString(int len, SecureRandom rng) {
    return org.apache.commons.lang3.RandomStringUtils.random(len, 32, 126,
							     false, false,
							     null, rng);
  }

  public static String randomString(int len, LockssDaemon daemon)
      throws NoSuchAlgorithmException,
	     NoSuchProviderException {
    RandomManager rmgr = daemon.getRandomManager();
    SecureRandom rng = rmgr.getSecureRandom();
    return randomString(len, rng);
  }

  public static KeyStore createKeyStore(Properties p)
      throws CertificateException,
      IOException,
      InvalidKeyException,
      KeyStoreException,
      NoSuchAlgorithmException,
      NoSuchProviderException,
      SignatureException,
      UnrecoverableKeyException, InvalidAlgorithmParameterException, OperatorCreationException {
    KeyStore ks = KeyStore.getInstance(p.getProperty(PROP_KEYSTORE_TYPE,
                                                     DEFAULT_KEYSTORE_TYPE));
    initializeKeyStore(ks, p);
    String keyStoreFileName = p.getProperty(PROP_KEYSTORE_FILE);
    if (!StringUtil.isNullString(keyStoreFileName)) { 
      storeKeyStore(ks, keyStoreFileName,
		    p.getProperty(PROP_KEYSTORE_PASSWORD));
    }
    return ks;
  }

  static void storeKeyStore(KeyStore keyStore,
			    String filename, String keyStorePassword)
      throws FileNotFoundException,
	     KeyStoreException,
	     NoSuchAlgorithmException,
	     CertificateException,
	     IOException {
    storeKeyStore(keyStore, new File(filename), keyStorePassword);
  }

  static void storeKeyStore(KeyStore keyStore,
			    File keyStoreFile, String keyStorePassword)
      throws FileNotFoundException,
	     KeyStoreException,
	     NoSuchAlgorithmException,
	     CertificateException,
	     IOException {
    OutputStream outs = null;
    try {
      log.debug("Storing " + keyStore.getType() +
                " KeyStore in " + keyStoreFile);
      outs = new BufferedOutputStream(new FileOutputStream(keyStoreFile));
      keyStore.store(outs, keyStorePassword.toCharArray());
      outs.close();
    } finally {
      IOUtil.safeClose(outs);
    }
  }


  private static void initializeKeyStore(KeyStore keyStore, Properties p)
      throws CertificateException,
      IOException,
      InvalidKeyException,
      KeyStoreException,
      NoSuchAlgorithmException,
      NoSuchProviderException,
      SignatureException,
      UnrecoverableKeyException, InvalidAlgorithmParameterException, OperatorCreationException {
    initializeKeyStore(keyStore, ConfigManager.fromProperties(p));
  }

  private static void initializeKeyStore(KeyStore keyStore,
					 Configuration config)
      throws CertificateException,
      IOException,
      InvalidKeyException,
      KeyStoreException,
      NoSuchAlgorithmException,
      NoSuchProviderException,
      SignatureException,
      UnrecoverableKeyException, InvalidAlgorithmParameterException, OperatorCreationException {
    String keyAlias = config.get(PROP_KEY_ALIAS, DEFAULT_KEY_ALIAS);
    String certAlias = config.get(PROP_CERT_ALIAS, DEFAULT_CERT_ALIAS);
    String keyAlgName = config.get(PROP_KEY_ALGORITHM, DEFAULT_KEY_ALGORITHM);
    String sigAlgName = config.get(PROP_SIG_ALGORITHM, DEFAULT_SIG_ALGORITHM);
    String keyStorePassword = config.get(PROP_KEYSTORE_PASSWORD);
    String keyPassword = config.get(PROP_KEY_PASSWORD);
    int keyBits = config.getInt(PROP_KEY_BITS, DEFAULT_KEY_BITS);
    long expireIn = config.getTimeInterval(PROP_EXPIRE_IN, DEFAULT_EXPIRE_IN);
    String x500String = config.get(PROP_X500_NAME, DEFAULT_X500_NAME);

    CertAndKeyGen keypair = new CertAndKeyGen(keyAlgName, sigAlgName);
    keypair.generate(keyBits);

    PrivateKey privKey = keypair.getPrivateKey();
    log.debug3("PrivKey: " + privKey.getAlgorithm()
	       + " " + privKey.getFormat());

    X509Certificate[] chain = new X509Certificate[1];

    X500Name x500Name = new X500Name(x500String);
    chain[0] = keypair.getSelfCertificate(x500Name, expireIn);
    log.debug3("Certificate: " + chain[0].toString());

    keyStore.load(null, keyStorePassword.toCharArray());
    keyStore.setCertificateEntry(certAlias, chain[0]);
    keyStore.setKeyEntry(keyAlias, privKey,
			 keyPassword.toCharArray(), chain);
    Key myKey = keyStore.getKey(keyAlias, keyPassword.toCharArray());
    log.debug2("MyKey: " + myKey.getAlgorithm() + " " + myKey.getFormat());
  }


  // Moved here mostly intact from EditKeyStores so it can be used by other
  // code.  Should be integrated with methods above.
  public static void createPLNKeyStores(File inDir,
					File outDir,
					List hostlist,
					SecureRandom rng)
      throws Exception {
    createPLNKeyStores(null, inDir, outDir, hostlist, rng);
  }

  public static void createPLNKeyStores(String ksTypeName,
                                        File inDir,
					File outDir,
					List hostlist,
					SecureRandom rng)
      throws Exception {

    KsType kst = ksTypeOrDefault(ksTypeName);
    String[] hosts = (String[])hostlist.toArray(new String[0]);
    KeyStore[] ks = new KeyStore[hosts.length];
    String[] pwd = new String[hosts.length];

    for (int i = 0; i < hosts.length; i++) {
      ks[i] = null;
      pwd[i] = null;
    }
    /*
     * Read in any existing keystores and their passwords
     */
    for (int i = 0; i < hosts.length; i++) {
      Exception err = null;
      try {
	readKeyStore(hosts, ks, pwd, i, inDir);
      } catch (Exception e) {
	log.error("Couldn't read keystore for " + hosts[i], e);
	err = e;
      }
      if (err != null) {
	throw err;
      }
    }
    /*
     * Create a password for each machine's keystore
     */
    for (int i = 0; i < hosts.length; i++) {
      if (pwd[i] == null) {
	pwd[i] = randomString(20, rng);
      }
    }
    /*
     * Create a keystore for each machine with a certificate
     * and a private key.
     */
    for (int i = 0; i <hosts.length; i++) {
      if (ks[i] == null) {
	ks[i] = createKeystore(kst, hosts[i], pwd[i]);
      }
    }
    /*
     * Build an array of the certificates
     */
    java.security.cert.Certificate[] cert =
      new X509Certificate[hosts.length];
    for (int i = 0; i < hosts.length; i++) {
      cert[i] = getCertificate(hosts, ks, i);
    }
    /*
     * Add all but the local certificate to the keyStore
     */
    for (int i = 0; i < hosts.length; i++) {
      addCertificates(hosts, ks[i], cert, i);
    }
    /*
     * Verify the key stores
     */
    boolean ok = true;
    for (int i = 0; i < hosts.length; i++) {
      if (!verifyKeyStore(hosts, ks, pwd, i)) {
	ok = false;
      }
    }
    if (ok) {
      /*
       * Write out each keyStore and its password
       */
      for (int i = 0; i < hosts.length; i++) {
	writeKeyStore(hosts, ks, pwd, i, outDir);
      }
    }
    for (int i = 0; i < hosts.length; i++) {
      listKeyStore(hosts, ks, pwd, i);
    }
  }

  public static void createSharedPLNKeyStores(File dir,
					      List hostlist,
					      File pubKeyStoreFile,
					      String pubKeyStorePassword,
					      SecureRandom rng)
      throws Exception {
    createSharedPLNKeyStores(null, dir, hostlist,
                             pubKeyStoreFile, pubKeyStorePassword, rng);
  }

  public static void createSharedPLNKeyStores(String ksType,
                                              File dir,
					      List hostlist,
					      File pubKeyStoreFile,
					      String pubKeyStorePassword,
					      SecureRandom rng)
      throws Exception {

    KsType kst = ksTypeOrDefault(ksType);
    String[] hosts = (String[])hostlist.toArray(new String[0]);
    KeyStore[] ks = new KeyStore[hosts.length];
    String[] pwd = new String[hosts.length];

    for (int i = 0; i < hosts.length; i++) {
      ks[i] = null;
      pwd[i] = null;
    }
    /*
     * Create or read the public keystore
     */
    KeyStore pubStore;
    File inPub;
    // If the pub keystore doesn't exist and the name has no extension,
    // append the appropriate extension
    if (!pubKeyStoreFile.exists() &&
        StringUtil.isNullString(FilenameUtils.getExtension(pubKeyStoreFile.toString()))) {
      pubKeyStoreFile = new File (pubKeyStoreFile + "." + kst.getExtension());
    }
    if (pubKeyStoreFile.exists()) {
      log.debug("Loading old pub keystore: " + pubKeyStoreFile);
      pubStore = loadKeystore(pubKeyStoreFile, pubKeyStorePassword);
    } else {
      pubStore = makeNewKeystore(kst);
      pubStore.load(null, pubKeyStorePassword.toCharArray());
    }      
    /*
     * Create a password for each machine's keystore
     */
    for (int i = 0; i < hosts.length; i++) {
      if (pwd[i] == null) {
	pwd[i] = randomString(20, rng);
      }
    }
    /*
     * Create a keystore for each machine with a certificate
     * and a private key.
     */
    for (int i = 0; i <hosts.length; i++) {
      String host = hosts[i];
      String certAlias = host + crtSuffix;
      Properties p = new Properties();
      p.put(PROP_KEYSTORE_TYPE, kst.toString());
      p.put(PROP_KEY_ALIAS, host + keySuffix);
      p.put(PROP_CERT_ALIAS, certAlias);
      p.put(PROP_KEY_PASSWORD, pwd[i]);
      p.put(PROP_KEYSTORE_PASSWORD, host);

      ks[i] = createKeyStore(p);

      /*
       * Add its certificate to the public keystore.
       */
      java.security.cert.Certificate cert = ks[i].getCertificate(certAlias);
      log.debug("About to store " + certAlias + " in keyStore for " +
		hosts[i]);
      try {
	pubStore.setCertificateEntry(certAlias, cert);
      } catch (KeyStoreException e) {
	log.debug("pubStore.setCertificateEntry(" + certAlias + "," +
		  host + ") threw " + e);
	throw e;
      }
    }
    /*
     * Write out each keyStore and its password
     */
    for (int i = 0; i < hosts.length; i++) {
      storeKeyStore(ks[i], new File(dir, (hosts[i] + "." +
                                          getKeystoreExtension(ks[i]))),
                    hosts[i]);
      writePasswordFile(new File(dir, hosts[i] + ".pass"), pwd[i]);
    }
    storeKeyStore(pubStore, pubKeyStoreFile, pubKeyStorePassword);

    if (log.isDebug()) {
      for (int i = 0; i < hosts.length; i++) {
	listKeyStore(hosts, ks, pwd, i);
      }
    }
  }

  private static KeyStore makeNewKeystore(KsType kst)
      throws KeyStoreException, NoSuchProviderException {
    try {
      log.debug("Trying to create " + kst.toString() + " keystore");
      KeyStore res = kst.newKeyStore();
      if (res != null) return res;
    } catch (KeyStoreException e) {
      log.debug("KeyStore.getInstance(" + kst.getType() + ") threw " + e);
      throw e;
    } catch (NoSuchProviderException e) {
      log.debug("KeyStore.getInstance(" + kst.getType() + ") threw " + e);
      throw e;
    }
    return null;
  }

  private static KeyStore loadKeystore(File file, String pass)
      throws KeyStoreException,
             NoSuchAlgorithmException,
             NoSuchProviderException,
             CertificateException,
             IOException {
    KsType kst = getKsTypeFromFilename(file);
    if (kst != null) {
      KeyStore ks = kst.newKeyStore();
      if (ks != null) {
        log.debug("Trying to read KeyStore from " + file);
        try (FileInputStream fis = new FileInputStream(file)) {
          ks.load(fis, pass.toCharArray());
          return ks;
        }
      }
    } else {
      return loadKeystoreOfUnknownType(file, pass);
    }
    return null;
  }

  public static KeyStore loadKeystoreOfUnknownType(File file, String pass)
      throws KeyStoreException,
             NoSuchAlgorithmException,
             NoSuchProviderException,
             CertificateException,
             IOException {
    try (InputStream ins = new FileInputStream(file)) {
      return loadKeystoreOfUnknownType(ins, pass);
    }
  }

  public static KeyStore loadKeystoreOfUnknownType(InputStream ins, String pass)
      throws KeyStoreException,
             NoSuchAlgorithmException,
             NoSuchProviderException,
             CertificateException,
             IOException {
    ins = StreamUtil.getResettableInputStream(ins);
    ins.mark(10 * 1024);
    IOException lastEx = null;
    for (KsType kst : KsType.values()) {
      log.debug2("Checking for " + kst);
      if (!kst.isKsType(ins)) {
        log.debug2("Isn't " + kst);
        continue;
      }
      log.debug("Trying to load as " + kst);
      KeyStore ks = kst.newKeyStore();
      if (ks != null) {
        ks.load(ins, StringUtil.isNullString(pass) ? null : pass.toCharArray());
        return ks;
      }
      ins.reset();
    }
    if (lastEx != null) {
      throw lastEx;
    } else {
      throw new IOException("Couldn't load unknown keystore type");
    }
  }

  private static KeyStore createKeystore(KsType ksType,
                                         String domainName, String password)
      throws CertificateException,
      IOException,
      InvalidKeyException,
      KeyStoreException,
      NoSuchAlgorithmException,
      NoSuchProviderException,
      SignatureException,
      UnrecoverableKeyException, InvalidAlgorithmParameterException, OperatorCreationException {
    KeyStore ks = createKeystore0(ksType);
    if (ks == null) {
      log.error("No key store available");
      return null;  // will fail subsequently
    }
    initializeKeyStore(ks, domainName, password);
    return ks;
  }

  private static KeyStore createKeystore0(KsType ksType)
      throws CertificateException,
	     IOException,
	     InvalidKeyException,
	     KeyStoreException,
	     NoSuchAlgorithmException,
	     NoSuchProviderException,
	     SignatureException,
	     UnrecoverableKeyException {
    //  No KeyStore - make one.
    KeyStore ks = makeNewKeystore(ksType);
    if (ks != null) {
      log.debug("Using key store type " + ks.getType());
    }
    return ks;
  }

  private static String keySuffix = ".key";
  private static String crtSuffix = ".crt";
  private static void initializeKeyStore(KeyStore keyStore,
					 String domainName, String password)
      throws IOException,
      CertificateException,
      InvalidKeyException,
      SignatureException,
      NoSuchAlgorithmException,
      NoSuchProviderException,
      KeyStoreException,
      UnrecoverableKeyException, InvalidAlgorithmParameterException, OperatorCreationException {
    String keyAlias = domainName + keySuffix;
    String certAlias = domainName + crtSuffix;
    String keyStorePassword = domainName;
    String keyStoreFileName = domainName + "." + getKeystoreExtension(keyStore);
    File keyStoreFile = new File(keyStoreFileName);
    if (keyStoreFile.exists()) {
      log.debug("Key store file " + keyStoreFileName + " exists");
      throw new IOException("Key store file " + keyStoreFileName + " exists");
    }
    String keyAlgName = DEFAULT_KEY_ALGORITHM;
    String sigAlgName = DEFAULT_SIG_ALGORITHM;
    log.debug("About to create a CertAndKeyGen: " + keyAlgName + " " + sigAlgName);
    CertAndKeyGen keypair;
    keypair = new CertAndKeyGen(keyAlgName, sigAlgName);
    log.debug("About to generate a key pair");
    try {
      keypair.generate(1024);
    } catch (InvalidAlgorithmParameterException | NoSuchAlgorithmException e) {
      log.debug("new CertAndKeyGen(" + keyAlgName + "," + sigAlgName +
          ") threw " + e);
      throw e;
    } catch (InvalidKeyException e) {
      log.debug("keypair.generate(1024) threw " + e);
      throw e;
    }
    log.debug("About to get a PrivateKey");
    PrivateKey privKey = keypair.getPrivateKey();
    log.debug("MyKey: " + privKey.getAlgorithm() + " " +
	      privKey.getFormat());
    log.debug("About to get a self-signed certificate");
    X509Certificate[] chain = new X509Certificate[1];
    X500Name x500Name = new X500Name("CN=" + domainName + ", " +
				     "OU=LOCKSS Team, O=Stanford, " +
				     "L=Stanford, ST=California, C=US");
    chain[0] = keypair.getSelfCertificate(x500Name, 365*24*60*60);
    log.debug("Certificate: " + chain[0].toString());
    log.debug("About to keyStore.load(null)");
    try {
      keyStore.load(null, keyStorePassword.toCharArray());
    } catch (IOException e) {
      log.debug("keyStore.load() threw " + e);
      throw e;
    } catch (CertificateException e) {
      log.debug("keyStore.load() threw " + e);
      throw e;
    } catch (NoSuchAlgorithmException e) {
      log.debug("keyStore.load() threw " + e);
      throw e;
    }
    log.debug("About to store " + certAlias + " in key store");
    try {
      keyStore.setCertificateEntry(certAlias, chain[0]);
    } catch (KeyStoreException e) {
      log.debug("keyStore.setCertificateEntry() threw " + e);
      throw e;
    }
    log.debug("About to store " + keyAlias + " in key store");
    try {
      keyStore.setKeyEntry(keyAlias, privKey,
			   password.toCharArray(), chain);
    } catch (KeyStoreException e) {
      log.debug("keyStore.setKeyEntry() threw " + e);
      throw e;
    }
    log.debug("About to getKeyEntry()");
    Key myKey = keyStore.getKey(keyAlias,
				password.toCharArray());
    log.debug("MyKey: " + myKey.getAlgorithm() + " " +
	      myKey.getFormat());
    log.debug("Done storing");
  }

  public static java.security.cert.Certificate
    getCertificate(String[] domainNames, KeyStore[] keyStores, int i)
      throws KeyStoreException {
    java.security.cert.Certificate ret;
    String alias = domainNames[i] + crtSuffix;
    try {
      ret = keyStores[i].getCertificate(alias);
      log.debug(alias + ": " + ret.getType());
      return ret;
    } catch (KeyStoreException e) {
      log.error("keyStore.getCertificate(" + alias + ") threw: " + e);
      throw e;
    }
  }

  public static void addCertificates(String[] domainNames,
				     KeyStore keyStore,
				     java.security.cert.Certificate[] certs,
				     int i) throws KeyStoreException {
    KeyStoreException err = null;
    for (int j = 0; j <domainNames.length; j++) {
      if (j != i) {
	String alias = domainNames[j] + crtSuffix;
	log.debug("About to store " + alias + " in keyStore for " +
		 domainNames[i]);
	try {
	  keyStore.setCertificateEntry(alias, certs[j]);
	} catch (KeyStoreException e) {
	  log.debug("keyStore.setCertificateEntry(" + alias + "," +
		   domainNames[i] + ") threw " + e);
	  err = e;
	}
      }
    }
    if (err != null) {
      throw err;
    }
  }

  private static void writePasswordFile(File passwordFile,
					String password) {
    try {
      log.debug3("Writing Password to " + passwordFile);
      PrintWriter pw = new PrintWriter(new FileOutputStream(passwordFile));
      pw.print(password);
      pw.close();
      log.debug3("Done storing Password in " + passwordFile);
    } catch (Exception e) {
      log.debug("ks.store(" + passwordFile + ")", e);
    }
  }

  private static void writeKeyStore(String domainNames[],
				    KeyStore kss[],
				    String passwords[],
				    int i,
				    File outDir)
      throws FileNotFoundException {
    String domainName = domainNames[i];
    KeyStore ks = kss[i];
    String password = passwords[i];
    if (domainName == null || ks == null || password == null) {
      return;
    }
    if (!outDir.exists() || !outDir.isDirectory()) {
      log.error("No directory " + outDir);
      throw new FileNotFoundException("No directory " + outDir);
    }
    File keyStoreFile = new File(outDir, domainName + "." +
                                 getKeystoreExtension(ks));
    File passwordFile = new File(outDir, domainName + ".pass");
    String keyStorePassword = domainName;
    try {
      log.debug("Writing KeyStore to " + keyStoreFile);
      FileOutputStream fos = new FileOutputStream(keyStoreFile);
      ks.store(fos, keyStorePassword.toCharArray());
      fos.close();
      log.debug("Done storing KeyStore in " + keyStoreFile);
    } catch (Exception e) {
      log.debug("ks.store(" + keyStoreFile + ") threw " + e);
    }
    writePasswordFile(passwordFile, password);
  }

  private static void readKeyStore(String domainNames[],
				   KeyStore kss[],
				   String passwords[],
				   int i,
				   File inDir)
      throws KeyStoreException,
             NoSuchAlgorithmException,
             NoSuchProviderException,
             CertificateException,
             IOException {
    String domainName = domainNames[i];
    if (domainName == null) {
      return;
    }
    File passwordFile = new File(inDir, domainName + ".pass");
    String password = null;
    try {
      if (!passwordFile.exists() || !passwordFile.isFile()) {
	log.debug("No password file " + passwordFile);
	return;
      }
      log.debug("Trying to read password from " + passwordFile);
      FileInputStream fis = new FileInputStream(passwordFile);
      byte[] buf = new byte[fis.available()];
      int l = fis.read(buf);
      if (l != buf.length) {
	log.debug("password read short " + l + " != " + buf.length);
	return;
      }
      password = new String(buf);
    } catch (IOException e) {
      log.debug("Read password threw " + e);
      throw e;
    }
    KeyStore ks = null;
    File keyStoreFile;
    for (KsType kst : KsType.values()) {
      keyStoreFile = new File(inDir, domainName + "." + kst.getExtension());
      if (keyStoreFile.exists()) {
        try {
          ks = kst.newKeyStore();
          if (ks != null) {
            log.debug("Trying to read KeyStore from " + keyStoreFile);
            try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
              ks.load(fis, domainName.toCharArray());
            }
            break;
          }
        } catch (KeyStoreException e) {
          log.debug("KeyStore.getInstance(" + kst.getType() + ") threw " + e);
          throw e;
        } catch (NoSuchProviderException e) {
          log.debug("KeyStore.getInstance(" + kst.getType() + ") threw " + e);
          throw e;
        }
      }
    }
    String keyStorePassword = domainName;
    passwords[i] = password;
    kss[i] = ks;
    log.debug("KeyStore and password for " + domainName + " read");
  }

  private static boolean verifyKeyStore(String domainNames[],
					KeyStore kss[],
					String passwords[],
					int i) {
    boolean ret = false;
    boolean[] hasKey = new boolean[domainNames.length];
    boolean[] hasCert = new boolean[domainNames.length];
    for (int j = 0; j < domainNames.length; j++) {
      hasKey[j] = false;
      hasCert[j] = false;
    }
    log.debug("start of key store verification for " + domainNames[i]);
    try {
      for (Enumeration en = kss[i].aliases(); en.hasMoreElements(); ) {
        String alias = (String) en.nextElement();
	log.debug("Next alias " + alias);
	int k = -1;
	for (int j = 0; j < domainNames.length; j++) {
	  if (alias.startsWith(domainNames[j])) {
	    k = j;
	  }
	}
	if (k < 0) {
	  log.error(alias + " not in domain names");
	  return ret;
	}
        if (kss[i].isCertificateEntry(alias)) {
	  log.debug("About to Certificate");
          java.security.cert.Certificate cert = kss[i].getCertificate(alias);
          if (cert == null) {
            log.error(alias + " null cert chain");
	    return ret;
	  }
	  log.debug("Cert for " + alias);
          hasCert[k] = true;
        } else if (kss[i].isKeyEntry(alias)) {
	  log.debug("About to getKey");
  	  Key privateKey = kss[i].getKey(alias, passwords[i].toCharArray());
	  if (privateKey != null) {
	    log.debug("Key for " + alias);
	    hasKey[k] = true;
	  } else {
	    log.error("No private key for " + alias);
	    return ret;
	  }
        } else {
  	  log.error(alias + " neither key nor cert");
	  return ret;
        }
      }
      log.debug("end of key store verification for "+ domainNames[i]);
    } catch (Exception ex) {
      log.error("listKeyStore() threw " + ex);
      return ret;
    }
    if (!hasKey[i]) {
      log.debug("no key for " + domainNames[i]);
      return ret;
    }
    for (int j = 0; j < domainNames.length; j++) {
      if (!hasCert[j]) {
	log.debug("no cert for " + domainNames[j]);
	return ret;
      }
    }
    ret = true;
    return ret;
  }

  private static void listKeyStore(String domainNames[],
				   KeyStore kss[],
				   String passwords[],
				   int i) {
    log.debug("start of key store for " + domainNames[i]);
    try {
      for (Enumeration en = kss[i].aliases(); en.hasMoreElements(); ) {
        String alias = (String) en.nextElement();
	log.debug("Next alias " + alias);
        if (kss[i].isCertificateEntry(alias)) {
	  log.debug("About to getCertificate");
          java.security.cert.Certificate cert = kss[i].getCertificate(alias);
          if (cert == null) {
            log.debug(alias + " null cert chain");
          } else {
            log.debug2("Cert for " + alias + " is " + cert.toString());
          }
        } else if (kss[i].isKeyEntry(alias)) {
	  log.debug("About to getKey");
  	  Key privateKey = kss[i].getKey(alias, passwords[i].toCharArray());
  	  log.debug(alias + " key " + privateKey.getAlgorithm() +
		   "/" + privateKey.getFormat());
        } else {
  	  log.error(alias + " neither key nor cert");
        }
      }
      log.debug("end of key store for "+ domainNames[i]);
    } catch (Exception ex) {
      log.error("listKeyStore() threw " + ex);
    }
  }

  /** Info about each supported KeyStore type - name, extension, keystore
   * recognition predicate. */
  public enum KsType {
    PKCS12() {
      @Override
      public boolean isKsType(InputStream ins) throws IOException {
        try {
          ASN1InputStream asnin = new ASN1InputStream(ins);
          asnin.readObject();
          return true;
        } catch (IOException e) {
          log.debug2("Expected ASN.1 error: " + e.toString());
        } finally {
          ins.reset();
        }
        return false;
      }
    },

    JCEKS(new byte[] {(byte)0xCE, (byte)0xCE, (byte)0xCE, (byte)0xCE}),
    JKS(new byte[] {(byte)0xFE, (byte)0xED, (byte)0xFE, (byte)0xED});

    private byte[] magic;

    KsType() {
    };

    /** Create a KsType with a magic number */
    KsType(byte[] magic) {
      this.magic = magic;
    };

    public String getType() {
      return toString();
    }

    byte[] getMagic() {
      return magic;
    }

    public boolean isKsType(InputStream ins) throws IOException {
      try {
        if (magic == null) {
          throw new UnsupportedOperationException("Can't check magic when there is none.");
        }
        int mlen = magic.length;
        byte[] buf = new byte[mlen];
        try {
          if (StreamUtil.readBytes(ins, buf, mlen) != mlen) {
            return false;
          }
          return Arrays.equals(magic, buf);
        } catch (IOException e) {
          log.debug2("Error checking magic number", e);
          return false;
        }
      } finally {
        ins.reset();
      }
    }

    public String getExtension() {
      return name().toLowerCase();
    }

    public KeyStore newKeyStore()
        throws KeyStoreException, NoSuchProviderException {
      return KeyStore.getInstance(getType());
    }
  }

  static String getKeystoreExtension(KeyStore ks) {
    return Enum.valueOf(KsType.class, ks.getType()).getExtension();
  }

  public static KsType getKsTypeFromFilename(File file) {
    return getKsTypeFromFilename(file.toString());
  }

  public static KsType getKsTypeFromFilename(String name) {
    String ext = FilenameUtils.getExtension(name);
    for (KsType kst : KsType.values()) {
      if (kst.getExtension().equalsIgnoreCase(ext)) {
        return kst;
      }
    }
    return null;
  }

  static KsType ksTypeOrDefault(String ksTypeName) {
    if (ksTypeName != null) {
      try {
        return Enum.valueOf(KsType.class, ksTypeName.toUpperCase());
      } catch (IllegalArgumentException e) {
      }
    }
    for (KsType kst : KsType.values()) {
      return kst;
    }
    throw new IllegalStateException("There are no Keystore types defined");
  }

  public static class CertAndKeyGen {
    String keyType;
    String sigAlg;
    KeyPair keyPair;

    static {
      Security.addProvider(new BouncyCastleProvider());
    }

    public CertAndKeyGen(String keyType, String sigAlg) {
      this.keyType = keyType;
      this.sigAlg = sigAlg;
    }

    private AlgorithmParameterSpec getAlgorithmParamSpec(int keyBits) {
      switch (keyType) {
        case "RSA":
          return new RSAKeyGenParameterSpec(keyBits, RSAKeyGenParameterSpec.F4);
        default:
          log.warning("No default AlgorithmParamSpec for algorithm: " + keyType);
      }

      return null;
    }

    public void generate(int keyBits)
        throws InvalidKeyException, NoSuchAlgorithmException, NoSuchProviderException, InvalidAlgorithmParameterException {
      KeyPairGenerator kpGen =
          KeyPairGenerator.getInstance(keyType, "BC");
      kpGen.initialize(getAlgorithmParamSpec(keyBits));
      keyPair = kpGen.generateKeyPair();
    }

    public PrivateKey getPrivateKey() {
      if (keyPair == null) {
        throw new IllegalStateException("Must generate keypair first");
      }
      return keyPair.getPrivate();
    }

    public X509Certificate getSelfCertificate(X500Name myname, long validity)
        throws CertificateException, InvalidKeyException, SignatureException,
        NoSuchAlgorithmException, NoSuchProviderException, IOException, OperatorCreationException {

      SubjectPublicKeyInfo subPubKeyInfo =
          SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

      Date notBefore = TimeBase.nowDate();
      Date notAfter  = new Date(notBefore.toInstant().plusSeconds(validity).toEpochMilli());

      X509v1CertificateBuilder builder =
          new X509v1CertificateBuilder(myname,
              new BigInteger("0"),
              notBefore, notAfter, myname, subPubKeyInfo);

      AlgorithmIdentifier sigAlgId;
      try {
        sigAlgId =
            new DefaultSignatureAlgorithmIdentifierFinder().find(sigAlg);
      } catch (IllegalArgumentException e) {
        throw new NoSuchAlgorithmException(e);
      }

      AlgorithmIdentifier digAlgId =
          new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);

      ContentSigner contentSigner =
          new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
              .build(PrivateKeyFactory.createKey(keyPair.getPrivate().getEncoded()));

      X509CertificateHolder holder = builder.build(contentSigner);

      return new JcaX509CertificateConverter().getCertificate(holder);
    }
  }
}
