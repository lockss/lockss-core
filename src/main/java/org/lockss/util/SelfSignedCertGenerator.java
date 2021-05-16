package org.lockss.util;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.x500.*;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.*;
import org.bouncycastle.cert.jcajce.*;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.*;
import org.bouncycastle.operator.bc.BcDigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Utility class for generating self-signed certificates.
 *
 * @author Mister PKI
 */
public final class SelfSignedCertGenerator {

  private SelfSignedCertGenerator() {}

  /**
   * Generates a self signed certificate using the BouncyCastle lib.
   *
   * @param keyPair used for signing the certificate with PrivateKey
   * @param hashAlgorithm Hash function
   * @param cn Common Name to be used in the subject dn
   * @param days validity period in days of the certificate
   *
   * @return self-signed X509Certificate
   *
   * @throws OperatorCreationException on creating a key id
   * @throws CertIOException on building JcaContentSignerBuilder
   * @throws CertificateException on getting certificate from provider
   */
  public static X509Certificate generate(final KeyPair keyPair,
                                         final String hashAlgorithm,
                                         final String cn,
                                         final int days)
      throws OperatorCreationException, CertificateException, CertIOException {
    return generate(keyPair, hashAlgorithm, new X500Name("CN=" + cn), days);
  }

  public static X509Certificate generate(final KeyPair keyPair,
                                         final String hashAlgorithm,
                                         final X500Name x500Name,
                                         final int days)
      throws OperatorCreationException, CertificateException, CertIOException
  {
    final Instant now = Instant.now();
    final Date notBefore = Date.from(now);
    final Date notAfter = Date.from(now.plus(Duration.ofDays(days)));

    final ContentSigner contentSigner = new JcaContentSignerBuilder(hashAlgorithm).build(keyPair.getPrivate());
    final X509v3CertificateBuilder certificateBuilder =
      new JcaX509v3CertificateBuilder(x500Name,
        BigInteger.valueOf(now.toEpochMilli()),
        notBefore,
        notAfter,
        x500Name,
        keyPair.getPublic())
        .addExtension(Extension.subjectKeyIdentifier, false, createSubjectKeyId(keyPair.getPublic()))
        .addExtension(Extension.authorityKeyIdentifier, false, createAuthorityKeyId(keyPair.getPublic()))
        .addExtension(Extension.basicConstraints, true, new BasicConstraints(true));

    return new JcaX509CertificateConverter()
      .setProvider(new BouncyCastleProvider()).getCertificate(certificateBuilder.build(contentSigner));
  }

  /**
   * Creates the hash value of the public key.
   *
   * @param publicKey of the certificate
   *
   * @return SubjectKeyIdentifier hash
   *
   * @throws OperatorCreationException
   */
  private static SubjectKeyIdentifier createSubjectKeyId(final PublicKey publicKey) throws OperatorCreationException {
    final SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
    final DigestCalculator digCalc =
      new BcDigestCalculatorProvider().get(new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));

    return new X509ExtensionUtils(digCalc).createSubjectKeyIdentifier(publicKeyInfo);
  }

  /**
   * Creates the hash value of the authority public key.
   *
   * @param publicKey of the authority certificate
   *
   * @return AuthorityKeyIdentifier hash
   *
   * @throws OperatorCreationException
   */
  private static AuthorityKeyIdentifier createAuthorityKeyId(final PublicKey publicKey)
    throws OperatorCreationException
  {
    final SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
    final DigestCalculator digCalc =
      new BcDigestCalculatorProvider().get(new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));

    return new X509ExtensionUtils(digCalc).createAuthorityKeyIdentifier(publicKeyInfo);
  }
}
