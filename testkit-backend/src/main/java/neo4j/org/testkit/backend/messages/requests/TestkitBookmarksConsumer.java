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

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.BookmarksConsumerRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import org.neo4j.driver.Bookmark;

@RequiredArgsConstructor
class TestkitBookmarksConsumer implements Consumer<Set<Bookmark>> {
    private final String bookmarkManagerId;
    private final TestkitState testkitState;
    private final BiFunction<TestkitState, TestkitCallback, CompletionStage<TestkitCallbackResult>> dispatchFunction;

    @Override
    public void accept(Set<Bookmark> bookmarks) {
        var callbackId = testkitState.newId();
        var body = BookmarksConsumerRequest.BookmarksConsumerRequestBody.builder()
                .id(callbackId)
                .bookmarkManagerId(bookmarkManagerId)
                .bookmarks(bookmarks.stream().map(Bookmark::value).collect(Collectors.toUnmodifiableSet()))
                .build();
        var callback = BookmarksConsumerRequest.builder().data(body).build();

        var callbackStage = dispatchFunction.apply(testkitState, callback);
        try {
            callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }
    }
}
