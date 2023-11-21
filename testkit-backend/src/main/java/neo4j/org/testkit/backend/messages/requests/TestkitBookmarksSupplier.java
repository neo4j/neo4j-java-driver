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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.BookmarksSupplierRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import org.neo4j.driver.Bookmark;

@RequiredArgsConstructor
class TestkitBookmarksSupplier implements Supplier<Set<Bookmark>> {
    private final String bookmarkManagerId;
    private final TestkitState testkitState;
    private final BiFunction<TestkitState, TestkitCallback, CompletionStage<TestkitCallbackResult>> dispatchFunction;

    public Set<Bookmark> get() {
        var callbackId = testkitState.newId();
        var bodyBuilder = BookmarksSupplierRequest.BookmarksSupplierRequestBody.builder()
                .id(callbackId)
                .bookmarkManagerId(bookmarkManagerId);

        var callback =
                BookmarksSupplierRequest.builder().data(bodyBuilder.build()).build();

        var callbackStage = dispatchFunction.apply(testkitState, callback);
        BookmarksSupplierCompleted resolutionCompleted;
        try {
            resolutionCompleted = (BookmarksSupplierCompleted)
                    callbackStage.toCompletableFuture().get();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected failure during Testkit callback", e);
        }

        return resolutionCompleted.getData().getBookmarks().stream()
                .map(Bookmark::from)
                .collect(Collectors.toUnmodifiableSet());
    }
}
