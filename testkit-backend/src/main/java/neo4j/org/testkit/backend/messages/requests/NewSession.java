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
package neo4j.org.testkit.backend.messages.requests;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.holder.AsyncSessionHolder;
import neo4j.org.testkit.backend.holder.DriverHolder;
import neo4j.org.testkit.backend.holder.ReactiveSessionHolder;
import neo4j.org.testkit.backend.holder.ReactiveSessionStreamsHolder;
import neo4j.org.testkit.backend.holder.RxSessionHolder;
import neo4j.org.testkit.backend.holder.SessionHolder;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.RxSession;
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
    public Mono<TestkitResponse> processRx(TestkitState testkitState) {
        return Mono.just(createSessionStateAndResponse(
                testkitState, this::createRxSessionState, testkitState::addRxSessionHolder));
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

    protected <T> TestkitResponse createSessionStateAndResponse(
            TestkitState testkitState,
            BiFunction<DriverHolder, SessionConfig, T> sessionStateProducer,
            Function<T, String> addSessionHolder) {
        DriverHolder driverHolder = testkitState.getDriverHolder(data.getDriverId());
        AccessMode formattedAccessMode = data.getAccessMode().equals("r") ? AccessMode.READ : AccessMode.WRITE;
        SessionConfig.Builder builder = SessionConfig.builder().withDefaultAccessMode(formattedAccessMode);

        Optional.ofNullable(data.bookmarks)
                .map(bookmarks ->
                        bookmarks.stream().map(InternalBookmark::parse).collect(Collectors.toList()))
                .ifPresent(builder::withBookmarks);

        Optional.ofNullable(data.database).ifPresent(builder::withDatabase);
        Optional.ofNullable(data.impersonatedUser).ifPresent(builder::withImpersonatedUser);

        if (data.getFetchSize() != 0) {
            builder.withFetchSize(data.getFetchSize());
        }

        Optional.ofNullable(data.bookmarkManagerId)
                .map(testkitState::getBookmarkManager)
                .ifPresent(builder::withBookmarkManager);

        Optional.ofNullable(data.notificationFilters)
                .map(NewDriver::toNotificationFilter)
                .ifPresent(builder::withNotificationFilterConfig);

        T sessionStateHolder = sessionStateProducer.apply(driverHolder, builder.build());
        String newId = addSessionHolder.apply(sessionStateHolder);

        return Session.builder()
                .data(Session.SessionBody.builder().id(newId).build())
                .build();
    }

    private SessionHolder createSessionState(DriverHolder driverHolder, SessionConfig sessionConfig) {
        return new SessionHolder(driverHolder, driverHolder.getDriver().session(sessionConfig), sessionConfig);
    }

    private AsyncSessionHolder createAsyncSessionState(DriverHolder driverHolder, SessionConfig sessionConfig) {
        return new AsyncSessionHolder(
                driverHolder, driverHolder.getDriver().session(AsyncSession.class, sessionConfig), sessionConfig);
    }

    @SuppressWarnings("deprecation")
    private RxSessionHolder createRxSessionState(DriverHolder driverHolder, SessionConfig sessionConfig) {
        return new RxSessionHolder(
                driverHolder, driverHolder.getDriver().session(RxSession.class, sessionConfig), sessionConfig);
    }

    private ReactiveSessionHolder createReactiveSessionState(DriverHolder driverHolder, SessionConfig sessionConfig) {
        return new ReactiveSessionHolder(
                driverHolder, driverHolder.getDriver().session(ReactiveSession.class, sessionConfig), sessionConfig);
    }

    private ReactiveSessionStreamsHolder createReactiveSessionStreamsState(
            DriverHolder driverHolder, SessionConfig sessionConfig) {
        return new ReactiveSessionStreamsHolder(
                driverHolder,
                driverHolder.getDriver().session(org.neo4j.driver.reactivestreams.ReactiveSession.class, sessionConfig),
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
        private List<String> notificationFilters;
    }
}
