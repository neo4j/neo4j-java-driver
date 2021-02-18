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

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Bookmarks;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Optional;

import org.neo4j.driver.Bookmark;

@Setter
@Getter
@NoArgsConstructor
public class SessionLastBookmarks implements TestkitRequest
{
    private SessionLastBookmarksBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        return Optional.ofNullable( testkitState.getSessionStates().getOrDefault( data.sessionId, null ) )
                       .map( SessionState::getSession )
                       .map( session ->
                             {
                                 Bookmark bookmark = testkitState.getSessionStates().get( data.getSessionId() ).getSession().lastBookmark();
                                 return Bookmarks.builder().data( Bookmarks.BookmarksBody.builder().bookmarks( bookmark ).build() ).build();
                             } )
                       .orElseThrow( () -> new RuntimeException( "Could not find session" ) );
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class SessionLastBookmarksBody
    {
        private String sessionId;
    }
}
