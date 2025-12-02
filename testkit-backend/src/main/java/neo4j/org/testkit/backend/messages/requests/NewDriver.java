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
package neo4j.org.testkit.backend.messages.requests;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.CustomDriverError;
import neo4j.org.testkit.backend.TestkitClock;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.DriverHolder;
import neo4j.org.testkit.backend.messages.requests.deserializer.HexByteArrayDeserializer;
import neo4j.org.testkit.backend.messages.responses.DomainNameResolutionRequired;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.DriverError;
import neo4j.org.testkit.backend.messages.responses.ResolverResolutionRequired;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.neo4j.bolt.connection.DefaultDomainNameResolver;
import org.neo4j.bolt.connection.DomainNameResolver;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.ClientCertificateManager;
import org.neo4j.driver.ClientCertificateManagers;
import org.neo4j.driver.ClientCertificates;
import org.neo4j.driver.Config;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.internal.InternalServerAddress;
import org.neo4j.driver.internal.SecuritySettings;
import org.neo4j.driver.internal.security.SecurityPlans;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.driver.observation.metrics.MetricsObservationProvider;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecord;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecordRepository;
import org.neo4j.driver.property_encryption.EncapsulatedKeyRecords;
import org.neo4j.driver.property_encryption.EnvelopePropertyEncryptionProfile;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.KeyEncapsulationServices;
import org.neo4j.driver.property_encryption.PropertyEncryptionProfile;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class NewDriver implements TestkitRequest {
    private NewDriverBody data;

    @SuppressWarnings("deprecation")
    @Override
    public TestkitResponse process(TestkitState testkitState) {
        var id = testkitState.newId();

        AuthTokenManager authTokenManager;
        if (data.getAuthTokenManagerId() != null) {
            authTokenManager = testkitState.getAuthProvider(data.getAuthTokenManagerId());
        } else {
            var authToken = AuthTokenUtil.parseAuthToken(data.getAuthorizationToken());
            authTokenManager = new StaticAuthTokenManager(authToken);
        }

        var configBuilder = Config.builder();
        if (data.isResolverRegistered()) {
            configBuilder.withResolver(callbackResolver(testkitState));
        }
        DomainNameResolver domainNameResolver = DefaultDomainNameResolver.getInstance();
        if (data.isDomainNameResolverRegistered()) {
            domainNameResolver = callbackDomainNameResolver(testkitState);
        }
        Optional.ofNullable(data.userAgent).ifPresent(configBuilder::withUserAgent);
        Optional.ofNullable(data.connectionTimeoutMs)
                .ifPresent(timeout -> configBuilder.withConnectionTimeout(timeout, TimeUnit.MILLISECONDS));
        Optional.ofNullable(data.fetchSize).ifPresent(configBuilder::withFetchSize);
        Optional.ofNullable(data.maxTxRetryTimeMs)
                .ifPresent(
                        retryTimeMs -> configBuilder.withMaxTransactionRetryTime(retryTimeMs, TimeUnit.MILLISECONDS));
        Optional.ofNullable(data.livenessCheckTimeoutMs)
                .ifPresent(timeout -> configBuilder.withConnectionLivenessCheckTimeout(timeout, TimeUnit.MILLISECONDS));
        Optional.ofNullable(data.maxConnectionPoolSize).ifPresent(configBuilder::withMaxConnectionPoolSize);
        Optional.ofNullable(data.connectionAcquisitionTimeoutMs)
                .ifPresent(timeout -> configBuilder.withConnectionAcquisitionTimeout(timeout, TimeUnit.MILLISECONDS));
        Optional.ofNullable(data.telemetryDisabled).ifPresent(configBuilder::withTelemetryDisabled);
        Optional.ofNullable(data.notificationsMinSeverity)
                .flatMap(InternalNotificationSeverity::valueOf)
                .ifPresent(configBuilder::withMinimumNotificationSeverity);
        Optional.ofNullable(data.notificationsDisabledCategories)
                .map(categories -> categories.stream()
                        .map(NotificationClassification::valueOf)
                        .collect(Collectors.toSet()))
                .ifPresent(configBuilder::withDisabledNotificationClassifications);
        Optional.ofNullable(data.maxConnectionLifetimeMs)
                .ifPresent(timeout -> configBuilder.withMaxConnectionLifetime(timeout, TimeUnit.MILLISECONDS));
        configBuilder.withAutoCommitRetriesDisabled(data.disableAutoCommitRetries);
        var metrics = MetricsObservationProvider.newInstance(configBuilder).metrics();
        var clientCertificateManager = Optional.ofNullable(data.getClientCertificateProviderId())
                .map(testkitState::getClientCertificateManager)
                .or(() -> Optional.ofNullable(data.getClientCertificate())
                        .map(ClientCertificate::getData)
                        .map(certificateData -> ClientCertificates.of(
                                Paths.get(certificateData.getCertfile()).toFile(),
                                Paths.get(certificateData.getKeyfile()).toFile(),
                                certificateData.getPassword()))
                        .map(ClientCertificateManagers::rotating))
                .orElse(null);
        try {
            configBuilder.withPropertyEncryptionProfiles(
                    configurePropertyEncryptionProfiles(data.getPropertyEncryptionProfiles()));
        } catch (IllegalArgumentException e) {
            throw new CustomDriverError(e);
        }
        org.neo4j.driver.Driver driver;
        var config = configBuilder.build();
        try {
            driver = driver(
                    URI.create(data.uri),
                    authTokenManager,
                    clientCertificateManager,
                    config,
                    domainNameResolver,
                    configureSecuritySettingsBuilder(),
                    testkitState,
                    id);
        } catch (RuntimeException e) {
            return handleExceptionAsErrorResponse(testkitState, e).orElseThrow(() -> e);
        }
        testkitState.addDriverHolder(id, new DriverHolder(driver, config, metrics));
        return Driver.builder().data(Driver.DriverBody.builder().id(id).build()).build();
    }

    private PropertyEncryptionProfile[] configurePropertyEncryptionProfiles(Set<EncryptionProfile> profiles) {
        if (profiles == null) {
            return null;
        }
        var provider = new BouncyCastleFipsProvider();
        SecureRandom secureRandom;
        try {
            secureRandom = SecureRandom.getInstance("NONCEANDIV", provider);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return profiles.stream()
                .map(profile -> {
                    var masterKey = Optional.ofNullable(profile.getKek())
                            .map(bytes -> (SecretKey) new SecretKeySpec(bytes, "AES"))
                            .orElseGet(() -> {
                                KeyGenerator keyGenerator;
                                try {
                                    keyGenerator = KeyGenerator.getInstance("AES");
                                } catch (NoSuchAlgorithmException e) {
                                    throw new RuntimeException(e);
                                }
                                keyGenerator.init(256);
                                return keyGenerator.generateKey();
                            });
                    KeyEncapsulationService encapsulationService;
                    try {
                        encapsulationService = KeyEncapsulationServices.local(masterKey, provider, secureRandom);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                    return EnvelopePropertyEncryptionProfile.builder(
                                    profile.getName(), encapsulationService, new InMemoryKeyRecordRepository())
                            .withCryptoContext(provider, secureRandom)
                            .build();
                })
                .toArray(PropertyEncryptionProfile[]::new);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return CompletableFuture.completedFuture(process(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.fromCompletionStage(processAsync(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return processReactive(testkitState);
    }

    private ServerAddressResolver callbackResolver(TestkitState testkitState) {
        return address -> {
            var callbackId = testkitState.newId();
            var body = ResolverResolutionRequired.ResolverResolutionRequiredBody.builder()
                    .id(callbackId)
                    .address(String.format("%s:%d", address.host(), address.port()))
                    .build();
            var response = ResolverResolutionRequired.builder().data(body).build();
            var c = dispatchTestkitCallback(testkitState, response);
            ResolverResolutionCompleted resolutionCompleted;
            try {
                resolutionCompleted =
                        (ResolverResolutionCompleted) c.toCompletableFuture().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return resolutionCompleted.getData().getAddresses().stream()
                    .map(InternalServerAddress::new)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        };
    }

    private DomainNameResolver callbackDomainNameResolver(TestkitState testkitState) {
        return address -> {
            var callbackId = testkitState.newId();
            var body = DomainNameResolutionRequired.DomainNameResolutionRequiredBody.builder()
                    .id(callbackId)
                    .name(address)
                    .build();
            var callback = DomainNameResolutionRequired.builder().data(body).build();

            var callbackStage = dispatchTestkitCallback(testkitState, callback);
            DomainNameResolutionCompleted resolutionCompleted;
            try {
                resolutionCompleted = (DomainNameResolutionCompleted)
                        callbackStage.toCompletableFuture().get();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected failure during Testkit callback", e);
            }

            return resolutionCompleted.getData().getAddresses().stream()
                    .map(addr -> {
                        try {
                            return InetAddress.getByName(addr);
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(InetAddress[]::new);
        };
    }

    private CompletionStage<TestkitCallbackResult> dispatchTestkitCallback(
            TestkitState testkitState, TestkitCallback response) {
        var future = new CompletableFuture<TestkitCallbackResult>();
        testkitState.getCallbackIdToFuture().put(response.getCallbackId(), future);
        testkitState.getResponseWriter().accept(response);
        return future;
    }

    private org.neo4j.driver.Driver driver(
            URI uri,
            AuthTokenManager authTokenManager,
            ClientCertificateManager clientCertificateManager,
            Config config,
            DomainNameResolver domainNameResolver,
            SecuritySettings.SecuritySettingsBuilder securitySettingsBuilder,
            TestkitState testkitState,
            String driverId) {
        var securitySettings = securitySettingsBuilder.build();
        @SuppressWarnings("deprecation")
        var securityPlan = SecurityPlans.createSecurityPlan(
                securitySettings, uri.getScheme(), clientCertificateManager, config.logging());
        return new DriverFactoryWithDomainNameResolver(domainNameResolver, testkitState, driverId)
                .newInstance(uri, authTokenManager, clientCertificateManager, config, securityPlan, null, null);
    }

    private Optional<TestkitResponse> handleExceptionAsErrorResponse(TestkitState testkitState, RuntimeException e) {
        Optional<TestkitResponse> response = Optional.empty();
        if (e instanceof IllegalArgumentException
                && e.getMessage().startsWith(DriverFactory.NO_ROUTING_CONTEXT_ERROR_MESSAGE)) {
            var id = testkitState.newId();
            var errorType = e.getClass().getName();
            response = Optional.of(DriverError.builder()
                    .data(DriverError.DriverErrorBody.builder()
                            .id(id)
                            .errorType(errorType)
                            .msg(e.getMessage())
                            .build())
                    .build());
        }
        return response;
    }

    private SecuritySettings.SecuritySettingsBuilder configureSecuritySettingsBuilder() {
        var securitySettingsBuilder = new SecuritySettings.SecuritySettingsBuilder();
        if (data.encrypted) {
            securitySettingsBuilder.withEncryption();
        } else {
            securitySettingsBuilder.withoutEncryption();
        }

        if (data.trustedCertificates != null) {
            if (!data.trustedCertificates.isEmpty()) {
                var certs = data.trustedCertificates.stream()
                        .map(cert -> "/usr/local/share/custom-ca-certificates/" + cert)
                        .map(Paths::get)
                        .map(Path::toFile)
                        .toArray(File[]::new);
                securitySettingsBuilder.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(certs));
            } else {
                securitySettingsBuilder.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
            }
        } else {
            securitySettingsBuilder.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
        }
        return securitySettingsBuilder;
    }

    @Setter
    @Getter
    public static class NewDriverBody {
        private String uri;
        private AuthorizationToken authorizationToken;
        private String authTokenManagerId;
        private String userAgent;
        private boolean resolverRegistered;
        private boolean domainNameResolverRegistered;
        private Long connectionTimeoutMs;
        private Integer fetchSize;
        private String notificationsMinSeverity;
        private Set<String> notificationsDisabledCategories;
        private Long maxTxRetryTimeMs;
        private Long livenessCheckTimeoutMs;
        private Integer maxConnectionPoolSize;
        private Long connectionAcquisitionTimeoutMs;
        private boolean encrypted;
        private List<String> trustedCertificates;
        private Boolean telemetryDisabled;
        private ClientCertificate clientCertificate;
        private String clientCertificateProviderId;
        private Long maxConnectionLifetimeMs;
        private boolean disableAutoCommitRetries;
        private Set<EncryptionProfile> propertyEncryptionProfiles;
    }

    @Setter
    @Getter
    public static class EncryptionProfile {
        private String name;

        @JsonDeserialize(using = HexByteArrayDeserializer.class)
        private byte[] kek;
    }

    public static class InMemoryKeyRecordRepository implements EncapsulatedKeyRecordRepository {
        private Map<String, Key> idToKey = new HashMap<>();
        private Map<String, String> aliasToId = new HashMap<>();

        @Override
        public CompletionStage<EncapsulatedKeyRecord> findById(String id) {
            var key = idToKey.get(id);
            if (key == null) {
                return CompletableFuture.completedFuture(null);
            }
            var alias = aliasToId.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(id))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
            return CompletableFuture.completedStage(
                    EncapsulatedKeyRecords.create(id, alias, key.encapsulation(), key.metadata()));
        }

        @Override
        public CompletionStage<EncapsulatedKeyRecord> findByAlias(String alias) {
            var id = aliasToId.get(alias);
            return findById(id);
        }

        public CompletionStage<EncapsulatedKeyRecord> save(
                String id, String alias, byte[] encapsulation, Map<String, String> metadata) {
            idToKey.put(id, new Key(encapsulation, metadata));
            aliasToId.put(alias, id);
            return CompletableFuture.completedStage(EncapsulatedKeyRecords.create(id, alias, encapsulation, metadata));
        }

        @Override
        public CompletionStage<EncapsulatedKeyRecord> create(
                String alias, byte[] encapsulation, Map<String, String> metadata) {
            return save(UUID.randomUUID().toString(), alias, encapsulation, metadata);
        }

        @Override
        public CompletionStage<Void> setAliasById(String id, String alias) {
            return null;
        }

        @Override
        public CompletionStage<Void> deleteById(String id) {
            idToKey.remove(id);
            return CompletableFuture.completedStage(null);
        }

        private record Key(byte[] encapsulation, Map<String, String> metadata) {}
    }

    @RequiredArgsConstructor
    private static class DriverFactoryWithDomainNameResolver extends DriverFactory {
        private final DomainNameResolver domainNameResolver;
        private final TestkitState testkitState;
        private final String driverId;

        @Override
        protected DomainNameResolver getDomainNameResolver() {
            return domainNameResolver;
        }

        @Override
        protected Clock createClock() {
            return TestkitClock.INSTANCE;
        }
    }
}
