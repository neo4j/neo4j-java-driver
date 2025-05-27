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

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.TestkitClock;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.DriverHolder;
import neo4j.org.testkit.backend.messages.responses.DomainNameResolutionRequired;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.DriverError;
import neo4j.org.testkit.backend.messages.responses.ResolverResolutionRequired;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
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
import org.neo4j.driver.internal.security.BoltSecurityPlanManager;
import org.neo4j.driver.internal.security.SecurityPlans;
import org.neo4j.driver.internal.security.StaticAuthTokenManager;
import org.neo4j.driver.net.ServerAddressResolver;
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
        configBuilder.withDriverMetrics();
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
        testkitState.addDriverHolder(id, new DriverHolder(driver, config));
        return Driver.builder().data(Driver.DriverBody.builder().id(id).build()).build();
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
                .newInstance(uri, authTokenManager, config, BoltSecurityPlanManager.from(securityPlan), null, null);
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
