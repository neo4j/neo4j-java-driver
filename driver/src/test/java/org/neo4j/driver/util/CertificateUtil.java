/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.util;

import static org.neo4j.driver.internal.util.CertificateTool.saveX509Cert;
import static org.neo4j.driver.util.FileTools.tempFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.neo4j.driver.internal.InternalPair;

public class CertificateUtil {
    private static final String DEFAULT_HOST_NAME = "localhost";
    private static final String DEFAULT_ENCRYPTION = "RSA";
    private static final Provider PROVIDER = new BouncyCastleProvider();

    static {
        // adds the Bouncy castle provider to java security
        Security.addProvider(PROVIDER);
    }

    private static KeyPair generateKeyPair() throws NoSuchProviderException, NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(DEFAULT_ENCRYPTION);
        keyPairGenerator.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        return keyPair;
    }

    private static X509Certificate generateCert(
            X500Name issuer, X500Name subject, KeyPair issuerKeys, PublicKey publicKey, GeneralName... generalNames)
            throws GeneralSecurityException, OperatorCreationException, CertIOException {
        // Create x509 certificate
        Date startDate = new Date(System.currentTimeMillis());
        Date endDate = new Date(System.currentTimeMillis() + 365L * 24L * 60L * 60L * 1000L);
        BigInteger serialNum = BigInteger.valueOf(System.currentTimeMillis());
        X509v3CertificateBuilder certBuilder =
                new JcaX509v3CertificateBuilder(issuer, serialNum, startDate, endDate, subject, publicKey);

        // Subject alternative name (part of SNI extension, used for hostname verification)
        Set<GeneralName> names = new HashSet<>();
        names.add(new GeneralName(GeneralName.dNSName, DEFAULT_HOST_NAME));
        names.addAll(Arrays.asList(generalNames));
        GeneralNames subjectAlternativeName = new GeneralNames(names.toArray(new GeneralName[0]));
        certBuilder.addExtension(Extension.subjectAlternativeName, false, subjectAlternativeName);
        certBuilder.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));

        // Get the certificate back
        ContentSigner signer = new JcaContentSignerBuilder("SHA512WithRSAEncryption").build(issuerKeys.getPrivate());
        X509CertificateHolder certHolder = certBuilder.build(signer);
        X509Certificate certificate =
                new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder);

        certificate.verify(issuerKeys.getPublic());
        return certificate;
    }

    public static class SelfSignedCertificateGenerator {
        private final KeyPair keyPair;
        private final X509Certificate certificate;

        public SelfSignedCertificateGenerator(GeneralName... generalNames)
                throws GeneralSecurityException, OperatorCreationException, CertIOException {
            // Create the public/private rsa key pair
            keyPair = generateKeyPair();

            // Create x509 certificate
            certificate = generateCert(
                    new X500Name("CN=" + DEFAULT_HOST_NAME),
                    new X500Name("CN=" + DEFAULT_HOST_NAME),
                    keyPair,
                    keyPair.getPublic(),
                    generalNames);
        }

        public void savePrivateKey(File saveTo) throws IOException {
            writePem("PRIVATE KEY", keyPair.getPrivate().getEncoded(), saveTo);
        }

        public void saveSelfSignedCertificate(File saveTo) throws CertificateEncodingException, IOException {
            writePem("CERTIFICATE", certificate.getEncoded(), saveTo);
        }

        public X509Certificate sign(PKCS10CertificationRequest csr, PublicKey csrPublicKey, GeneralName... generalNames)
                throws GeneralSecurityException, OperatorCreationException, CertIOException {
            return generateCert(
                    X500Name.getInstance(
                            this.certificate.getSubjectX500Principal().getEncoded()),
                    csr.getSubject(),
                    keyPair,
                    csrPublicKey,
                    generalNames);
        }
    }

    public static class CertificateSigningRequestGenerator {
        // ref: http://senthadev.com/generating-csr-using-java-and-bouncycastle-api.html
        private final KeyPair keyPair;
        private final PKCS10CertificationRequest csr;

        public CertificateSigningRequestGenerator() throws NoSuchAlgorithmException, OperatorCreationException {
            KeyPairGenerator gen = KeyPairGenerator.getInstance(DEFAULT_ENCRYPTION);
            gen.initialize(2048, new SecureRandom());
            keyPair = gen.generateKeyPair();

            X500Principal subject = new X500Principal("CN=" + DEFAULT_HOST_NAME);
            ContentSigner signGen = new JcaContentSignerBuilder("SHA512WithRSAEncryption").build(keyPair.getPrivate());

            PKCS10CertificationRequestBuilder builder =
                    new JcaPKCS10CertificationRequestBuilder(subject, keyPair.getPublic());
            csr = builder.build(signGen);
        }

        public PrivateKey privateKey() {
            return keyPair.getPrivate();
        }

        public PublicKey publicKey() {
            return keyPair.getPublic();
        }

        public PKCS10CertificationRequest certificateSigningRequest() {
            return csr;
        }

        public void savePrivateKey(File saveTo) throws IOException {
            writePem("PRIVATE KEY", keyPair.getPrivate().getEncoded(), saveTo);
        }
    }

    /**
     * Create a random certificate
     *
     * @return a random certificate
     * @throws GeneralSecurityException, OperatorCreationException
     */
    public static X509Certificate generateSelfSignedCertificate()
            throws GeneralSecurityException, OperatorCreationException, CertIOException {
        return new SelfSignedCertificateGenerator().certificate;
    }

    private static void writePem(String type, byte[] encodedContent, File path) throws IOException {
        if (path.getParentFile() != null && path.getParentFile().exists()) {
            path.getParentFile().mkdirs();
        }
        try (PemWriter writer = new PemWriter(new FileWriter(path))) {
            writer.writeObject(new PemObject(type, encodedContent));
            writer.flush();
        }
    }

    public static CertificateKeyPair<File, File> createNewCertificateAndKeySignedBy(
            CertificateKeyPair<File, File> root, GeneralName... generalNames) throws Throwable {
        Objects.requireNonNull(root.certGenerator);
        File cert = tempFile("driver", ".cert");
        File key = tempFile("driver", ".key");
        CertificateUtil.CertificateSigningRequestGenerator csrGenerator =
                new CertificateUtil.CertificateSigningRequestGenerator();
        X509Certificate signedCert = root.certGenerator.sign(
                csrGenerator.certificateSigningRequest(), csrGenerator.publicKey(), generalNames);
        csrGenerator.savePrivateKey(key);
        saveX509Cert(signedCert, cert);

        return new CertificateKeyPair<>(cert, key);
    }

    public static CertificateKeyPair<File, File> createNewCertificateAndKey(GeneralName... ipAddresses)
            throws Throwable {
        File cert = tempFile("driver", ".cert");
        File key = tempFile("driver", ".key");
        CertificateUtil.SelfSignedCertificateGenerator certGenerator =
                new CertificateUtil.SelfSignedCertificateGenerator(ipAddresses);
        certGenerator.saveSelfSignedCertificate(cert);
        certGenerator.savePrivateKey(key);

        return new CertificateKeyPair<>(cert, key, certGenerator);
    }

    public static class CertificateKeyPair<C, K> {
        private final Pair<C, K> pair;
        private final CertificateUtil.SelfSignedCertificateGenerator certGenerator;

        public CertificateKeyPair(C cert, K key) {
            this(cert, key, null);
        }

        public CertificateKeyPair(C cert, K key, CertificateUtil.SelfSignedCertificateGenerator certGenerator) {
            this.pair = InternalPair.of(cert, key);
            this.certGenerator = certGenerator;
        }

        public K key() {
            return pair.value();
        }

        public C cert() {
            return pair.key();
        }

        public CertificateUtil.SelfSignedCertificateGenerator certGenerator() {
            return this.certGenerator;
        }

        @Override
        public String toString() {
            return pair.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CertificateKeyPair<?, ?> that = (CertificateKeyPair<?, ?>) o;

            return pair.equals(that.pair);
        }

        @Override
        public int hashCode() {
            return pair.hashCode();
        }
    }
}
