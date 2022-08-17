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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.BookmarksSupplierRequest;
import neo4j.org.testkit.backend.messages.responses.TestkitCallback;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.BookmarkSupplier;

@RequiredArgsConstructor
class TestkitBookmarkSupplier implements BookmarkSupplier {
    private final TestkitState testkitState;
    private final BiFunction<TestkitState, TestkitCallback, CompletionStage<TestkitCallbackResult>> dispatchFunction;

    @Override
    public Set<Bookmark> getBookmarks(String database) {
        Objects.requireNonNull(database, "database must not be null");
        return getBookmarksFromTestkit(database);
    }

    @Override
    public Set<Bookmark> getAllBookmarks() {
        return getBookmarksFromTestkit(null);
    }

    private Set<Bookmark> getBookmarksFromTestkit(String database) {
        var callbackId = testkitState.newId();
        var bodyBuilder =
                BookmarksSupplierRequest.BookmarksSupplierRequestBody.builder().id(callbackId);
        if (database != null) {
            bodyBuilder = bodyBuilder.database(database);
        }
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
