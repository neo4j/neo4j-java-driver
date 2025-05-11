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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.BookmarkManager;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkManagers;
import reactor.core.publisher.Mono;

@Setter
@Getter
public class NewBookmarkManager implements TestkitRequest {
    private NewBookmarkManagerBody data;

    @Override
    public TestkitResponse process(TestkitState testkitState) {
        return createBookmarkManagerAndResponse(testkitState);
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync(TestkitState testkitState) {
        return CompletableFuture.completedFuture(createBookmarkManagerAndResponse(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactive(TestkitState testkitState) {
        return Mono.just(createBookmarkManagerAndResponse(testkitState));
    }

    @Override
    public Mono<TestkitResponse> processReactiveStreams(TestkitState testkitState) {
        return Mono.just(createBookmarkManagerAndResponse(testkitState));
    }

    private BookmarkManager createBookmarkManagerAndResponse(TestkitState testkitState) {
        var id = testkitState.newId();
        var initialBookmarks = Optional.ofNullable(data.getInitialBookmarks()).orElseGet(Collections::emptySet).stream()
                .map(Bookmark::from)
                .collect(Collectors.toSet());
        var managerConfigBuilder =
                org.neo4j.driver.BookmarkManagerConfig.builder().withInitialBookmarks(initialBookmarks);
        if (data.isBookmarksSupplierRegistered()) {
            managerConfigBuilder.withBookmarksSupplier(
                    new TestkitBookmarksSupplier(id, testkitState, this::dispatchTestkitCallback));
        }
        if (data.isBookmarksConsumerRegistered()) {
            managerConfigBuilder.withBookmarksConsumer(
                    new TestkitBookmarksConsumer(id, testkitState, this::dispatchTestkitCallback));
        }
        var manager = BookmarkManagers.defaultManager(managerConfigBuilder.build());

        testkitState.addBookmarkManager(id, manager);

        return BookmarkManager.builder()
                .data(BookmarkManager.BookmarkManagerBody.builder().id(id).build())
                .build();
    }

    private CompletionStage<TestkitCallbackResult> dispatchTestkitCallback(
            TestkitState testkitState, TestkitCallback response) {
        var future = new CompletableFuture<TestkitCallbackResult>();
        testkitState.getCallbackIdToFuture().put(response.getCallbackId(), future);
        testkitState.getResponseWriter().accept(response);
        return future;
    }

    @Setter
    @Getter
    public static class NewBookmarkManagerBody {
        private Set<String> initialBookmarks;
        private boolean bookmarksSupplierRegistered;
        private boolean bookmarksConsumerRegistered;
    }
}
