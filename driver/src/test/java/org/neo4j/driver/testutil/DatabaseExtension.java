/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.testutil;

import static java.lang.Integer.parseInt;
import static org.neo4j.driver.testutil.Neo4jSettings.BOLT_TLS_LEVEL;
import static org.neo4j.driver.testutil.Neo4jSettings.BoltTlsLevel.OPTIONAL;
import static org.neo4j.driver.testutil.Neo4jSettings.BoltTlsLevel.REQUIRED;
import static org.neo4j.driver.testutil.Neo4jSettings.SSL_POLICY_BOLT_CLIENT_AUTH;
import static org.neo4j.driver.testutil.Neo4jSettings.SSL_POLICY_BOLT_ENABLED;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.bouncycastle.asn1.x509.GeneralName;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.testutil.CertificateUtil.CertificateKeyPair;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public class DatabaseExtension implements ExecutionCondition, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
    private static final int BOLT_PORT = 7687;
    private static final int HTTP_PORT = 7474;

    private static final boolean dockerAvailable;
    private static final DatabaseExtension instance;
    private static final URI boltUri;
    private static final URI httpUri;
    private static final AuthToken authToken;
    private static final File cert;
    private static final File key;
    private static final Network network;
    private static final GenericContainer<?> nginx;
    private static final Map<String, String> defaultConfig;

    private static Neo4jContainer<?> neo4jContainer;
    private static Driver driver;
    private static boolean nginxRunning;

    static {
        dockerAvailable = isDockerAvailable();
        instance = new DatabaseExtension();
        defaultConfig = new HashMap<>();
        defaultConfig.put(SSL_POLICY_BOLT_ENABLED, "true");
        defaultConfig.put(SSL_POLICY_BOLT_CLIENT_AUTH, "NONE");
        defaultConfig.put(BOLT_TLS_LEVEL, OPTIONAL.toString());

        if (dockerAvailable) {
            var pair = generateCertificateAndKey();
            cert = pair.cert();
            key = pair.key();

            network = Network.newNetwork();
            neo4jContainer = setupNeo4jContainer(cert, key, defaultConfig);
            neo4jContainer.start();
            nginx = setupNginxContainer();
            nginx.start();
            nginxRunning = true;

            var neo4jBoltUri = URI.create(neo4jContainer.getBoltUrl());
            var neo4jHttpUri = URI.create(neo4jContainer.getHttpUrl());

            boltUri = URI.create(String.format(
                    "%s://%s:%d", neo4jBoltUri.getScheme(), neo4jBoltUri.getHost(), nginx.getMappedPort(BOLT_PORT)));
            httpUri = URI.create(String.format(
                    "%s://%s:%d", neo4jHttpUri.getScheme(), neo4jHttpUri.getHost(), nginx.getMappedPort(HTTP_PORT)));

            authToken = AuthTokens.basic("neo4j", neo4jContainer.getAdminPassword());
            driver = GraphDatabase.driver(boltUri, authToken);
            waitForBoltAvailability();
        } else {
            // stub init, this is not usable when Docker is unavailable
            boltUri = URI.create("");
            httpUri = URI.create("");
            authToken = AuthTokens.none();
            cert = new File("");
            key = new File("");
            network = null;
            nginx = new GenericContainer<>(DockerImageName.parse("alpine:latest"));
        }
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return dockerAvailable
                ? ConditionEvaluationResult.enabled("Docker is available")
                : ConditionEvaluationResult.disabled("Docker is unavailable");
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        TestUtil.cleanDb(driver);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (!nginxRunning) {
            startProxy();
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        deleteAndStartNeo4j(Collections.emptyMap());
    }

    public Driver driver() {
        return driver;
    }

    public Driver customDriver(Config config) {
        return GraphDatabase.driver(boltUri, authToken, config);
    }

    public void deleteAndStartNeo4j(Map<String, String> config) {
        Map<String, String> updatedConfig = new HashMap<>(defaultConfig);
        updatedConfig.putAll(config);

        neo4jContainer.stop();
        neo4jContainer = setupNeo4jContainer(cert, key, updatedConfig);
        neo4jContainer.start();
        if (REQUIRED.toString().equals(config.get(BOLT_TLS_LEVEL))) {
            driver = GraphDatabase.driver(
                    boltUri,
                    authToken,
                    Config.builder()
                            .withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(cert))
                            .withEncryption()
                            .build());
        } else {
            driver = GraphDatabase.driver(boltUri, authToken);
        }
        waitForBoltAvailability();
    }

    public String addImportFile(String prefix, String suffix, String contents) throws IOException {
        var tmpFile = File.createTempFile(prefix, suffix, null);
        tmpFile.deleteOnExit();
        try (var out = new PrintWriter(tmpFile)) {
            out.println(contents);
        }
        var tmpFilePath = tmpFile.toPath();
        var targetPath =
                Paths.get("/var/lib/neo4j/import", tmpFilePath.getFileName().toString());
        neo4jContainer.copyFileToContainer(MountableFile.forHostPath(tmpFilePath), targetPath.toString());
        return String.format("file:///%s", tmpFile.getName());
    }

    public URI uri() {
        return boltUri;
    }

    public int httpPort() {
        return httpUri.getPort();
    }

    public int boltPort() {
        return boltUri.getPort();
    }

    public AuthTokenManager authTokenManager() {
        return new StaticAuthTokenManager(authToken);
    }

    public String adminPassword() {
        return neo4jContainer.getAdminPassword();
    }

    public BoltServerAddress address() {
        return new BoltServerAddress(boltUri);
    }

    public void updateEncryptionKeyAndCert(File key, File cert) {
        System.out.println("Updated neo4j key and certificate file.");
        neo4jContainer.stop();
        neo4jContainer = setupNeo4jContainer(cert, key, defaultConfig);
        neo4jContainer.start();
        driver = GraphDatabase.driver(boltUri, authToken);
        waitForBoltAvailability();
    }

    public File tlsCertFile() {
        return cert;
    }

    public void startProxy() {
        try {
            nginx.execInContainer("nginx");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        nginxRunning = true;
    }

    public void stopProxy() {
        try {
            nginx.execInContainer("nginx", "-s", "stop");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        nginxRunning = false;
    }

    public boolean isNeo4j44OrEarlier() {
        return isNeo4jVersionOrEarlier(4);
    }

    public boolean isNeo4j43OrEarlier() {
        return isNeo4jVersionOrEarlier(3);
    }

    private boolean isNeo4jVersionOrEarlier(int minor) {
        try (var session = driver.session()) {
            var neo4jVersion = session.executeRead(
                    tx -> tx.run("CALL dbms.components() YIELD versions " + "RETURN versions[0] AS version")
                            .single()
                            .get("version")
                            .asString());
            var versions = neo4jVersion.split("\\.");
            return parseInt(versions[0]) <= 4 && parseInt(versions[1]) <= minor;
        }
    }

    public static DatabaseExtension getInstance() {
        return instance;
    }

    public static GeneralName getDockerHostGeneralName() {
        var host = DockerClientFactory.instance().dockerHostIpAddress();
        GeneralName generalName;
        try {
            generalName = new GeneralName(GeneralName.iPAddress, host);
        } catch (IllegalArgumentException e) {
            generalName = new GeneralName(GeneralName.dNSName, host);
        }
        return generalName;
    }

    private static CertificateKeyPair<File, File> generateCertificateAndKey() {
        try {
            return CertificateUtil.createNewCertificateAndKey(getDockerHostGeneralName());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("resource")
    private static Neo4jContainer<?> setupNeo4jContainer(File cert, File key, Map<String, String> config) {
        var neo4JVersion = Optional.ofNullable(System.getenv("NEO4J_VERSION")).orElse("2025");

        var extendedNeo4jImage = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder -> builder.from(String.format("neo4j:%s-enterprise", neo4JVersion))
                        .run("mkdir /var/lib/neo4j/certificates/bolt")
                        .copy("public.crt", "/var/lib/neo4j/certificates/bolt/")
                        .copy("private.key", "/var/lib/neo4j/certificates/bolt/")
                        .build())
                .withFileFromPath("public.crt", cert.toPath())
                .withFileFromPath("private.key", key.toPath());

        var extendedNeo4jImageAsSubstitute =
                DockerImageName.parse(extendedNeo4jImage.get()).asCompatibleSubstituteFor("neo4j");

        neo4jContainer = new Neo4jContainer<>(extendedNeo4jImageAsSubstitute)
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withNetwork(network)
                .withNetworkAliases("neo4j");
        for (var entry : config.entrySet()) {
            neo4jContainer.withNeo4jConfig(entry.getKey(), entry.getValue());
        }

        return neo4jContainer;
    }

    @SuppressWarnings({"rawtypes", "resource"})
    private static GenericContainer<?> setupNginxContainer() {
        var extendedNginxImage = new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder -> builder.from("nginx:1.23.0-alpine")
                        .copy("nginx.conf", "/etc/nginx/")
                        .build())
                .withFileFromClasspath("nginx.conf", "nginx.conf");

        return new GenericContainer(extendedNginxImage.get())
                .withNetwork(network)
                .withExposedPorts(BOLT_PORT, HTTP_PORT)
                .withCommand("sh", "-c", "nginx && while sleep 3600; do :; done");
    }

    private static void waitForBoltAvailability() {
        var maxAttempts = 600;
        for (var attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                driver.verifyConnectivity();
                return;
            } catch (RuntimeException verificationException) {
                if (attempt == maxAttempts - 1) {
                    throw new RuntimeException(
                            "Timed out waiting for Neo4j to become available over Bolt", verificationException);
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException interruptedException) {
                    interruptedException.addSuppressed(verificationException);
                    throw new RuntimeException(
                            "Interrupted while waiting for Neo4j to become available over Bolt", interruptedException);
                }
            }
        }
    }

    @SuppressWarnings("resource")
    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }
}
