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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.AuthTokenUtil;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.AsyncSessionHolder;
import neo4j.org.testkit.backend.holder.DriverHolder;
import neo4j.org.testkit.backend.holder.ReactiveSessionHolder;
import neo4j.org.testkit.backend.holder.ReactiveSessionStreamsHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.internal.InternalNotificationSeverity;
import org.neo4j.driver.reactive.ReactiveSession;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class NewSession implements TestkitRequest {
    private NewSessionBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return createSessionStateAndResponse(testkitState, this::createSessionState, testkitState::addSessionHolder);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return CompletableFuture.completedFuture(createSessionStateAndResponse(
                testkitState, this::createAsyncSessionState, testkitState::addAsyncSessionHolder));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.just(createSessionStateAndResponse(
                testkitState, this::createReactiveSessionState, testkitState::addReactiveSessionHolder));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return Mono.just(createSessionStateAndResponse(
                testkitState, this::createReactiveSessionStreamsState, testkitState::addReactiveSessionStreamsHolder));
    }

    private <T> TestkitResponse createSessionStateAndResponse(
            TestkitState testkitState,
            SessionStateProducer<T> sessionStateProducer,
            Function<T, String> addSessionHolder) {
        var driverHolder = testkitState.getDriverHolder(data.getDriverId());

        var builder = SessionConfig.builder();
        Optional.ofNullable(data.getAccessMode())
                .map(mode -> mode.equals("r") ? AccessMode.READ : AccessMode.WRITE)
                .ifPresent(builder::withDefaultAccessMode);

        Optional.ofNullable(data.bookmarks)
                .map(bookmarks -> bookmarks.stream().map(Bookmark::from).collect(Collectors.toList()))
                .ifPresent(builder::withBookmarks);

        Optional.ofNullable(data.database).ifPresent(builder::withDatabase);
        Optional.ofNullable(data.impersonatedUser).ifPresent(builder::withImpersonatedUser);

        if (data.getFetchSize() != 0) {
            builder.withFetchSize(data.getFetchSize());
        }

        Optional.ofNullable(data.bookmarkManagerId)
                .map(testkitState::getBookmarkManager)
                .ifPresent(builder::withBookmarkManager);

        Optional.ofNullable(data.notificationsMinSeverity)
                .flatMap(InternalNotificationSeverity::valueOf)
                .ifPresent(builder::withMinimumNotificationSeverity);
        Optional.ofNullable(data.notificationsDisabledCategories)
                .map(categories -> categories.stream()
                        .map(NotificationClassification::valueOf)
                        .collect(Collectors.toSet()))
                .ifPresent(builder::withDisabledNotificationClassifications);

        var userSwitchAuthToken = data.getAuthorizationToken() != null
                ? AuthTokenUtil.parseAuthToken(data.getAuthorizationToken())
                : null;

        var sessionStateHolder = sessionStateProducer.apply(driverHolder, builder.build(), userSwitchAuthToken);
        var newId = addSessionHolder.apply(sessionStateHolder);

        return Session.builder()
                .data(Session.SessionBody.builder().id(newId).build())
                .build();
    }

    @SuppressWarnings("resource")
    private SessionHolder createSessionState(
            DriverHolder driverHolder, SessionConfig sessionConfig, AuthToken userSwitchAuthToken) {
        return new SessionHolder(
                driverHolder,
                driverHolder.driver().session(org.neo4j.driver.Session.class, sessionConfig, userSwitchAuthToken),
                sessionConfig);
    }

    @SuppressWarnings("resource")
    private AsyncSessionHolder createAsyncSessionState(
            DriverHolder driverHolder, SessionConfig sessionConfig, AuthToken userSwitchAuthToken) {
        return new AsyncSessionHolder(
                driverHolder,
                driverHolder.driver().session(AsyncSession.class, sessionConfig, userSwitchAuthToken),
                sessionConfig);
    }

    @SuppressWarnings("resource")
    private ReactiveSessionHolder createReactiveSessionState(
            DriverHolder driverHolder, SessionConfig sessionConfig, AuthToken userSwitchAuthToken) {
        return new ReactiveSessionHolder(
                driverHolder,
                driverHolder.driver().session(ReactiveSession.class, sessionConfig, userSwitchAuthToken),
                sessionConfig);
    }

    @SuppressWarnings("resource")
    private ReactiveSessionStreamsHolder createReactiveSessionStreamsState(
            DriverHolder driverHolder, SessionConfig sessionConfig, AuthToken userSwitchAuthToken) {
        return new ReactiveSessionStreamsHolder(
                driverHolder,
                driverHolder
                        .driver()
                        .session(
                                org.neo4j.driver.reactivestreams.ReactiveSession.class,
                                sessionConfig,
                                userSwitchAuthToken),
                sessionConfig);
    }

    @Setter
    @Getter
    public static class NewSessionBody {
        private String driverId;
        private String accessMode;
        private List<String> bookmarks;
        private String database;
        private String impersonatedUser;
        private int fetchSize;
        private String bookmarkManagerId;
        private String notificationsMinSeverity;
        private Set<String> notificationsDisabledCategories;
        private AuthorizationToken authorizationToken;
    }

    @FunctionalInterface
    private interface SessionStateProducer<T> {
        T apply(DriverHolder driverHolder, SessionConfig sessionConfig, AuthToken userSwitchAuthToken);
    }
}
