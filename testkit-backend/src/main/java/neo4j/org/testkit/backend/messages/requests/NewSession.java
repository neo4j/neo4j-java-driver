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
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.internal.InternalBookmark;

@Setter
@Getter
@NoArgsConstructor
public class NewSession implements TestkitRequest
{
    private NewSessionBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        Driver driver = testkitState.getDrivers().get( data.getDriverId() );
        AccessMode formattedAccessMode = data.getAccessMode().equals( "r" ) ? AccessMode.READ : AccessMode.WRITE;
        SessionConfig.Builder builder = SessionConfig.builder()
                                                     .withDefaultAccessMode( formattedAccessMode );

        Optional.ofNullable( data.bookmarks )
                .map( bookmarks -> bookmarks.stream().map( InternalBookmark::parse ).collect( Collectors.toList() ) )
                .ifPresent( builder::withBookmarks );
        
        Optional.ofNullable( data.database ).ifPresent( builder::withDatabase );

        if ( data.getFetchSize() != 0 )
        {
            builder.withFetchSize( data.getFetchSize() );
        }

        org.neo4j.driver.Session session = driver.session( builder.build() );
        String newId = testkitState.newId();
        testkitState.getSessionStates().put( newId, new SessionState( session ) );

        return Session.builder().data( Session.SessionBody.builder().id( newId ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class NewSessionBody
    {
        private String driverId;
        private String accessMode;
        private List<String> bookmarks;
        private String database;
        private int fetchSize;
    }
}
