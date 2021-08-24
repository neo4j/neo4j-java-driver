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
import neo4j.org.testkit.backend.AsyncSessionState;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
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
        return createSessionStateAndResponse( testkitState, this::createSessionState, testkitState.getSessionStates() );
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        return CompletableFuture.completedFuture( createSessionStateAndResponse( testkitState, this::createAsyncSessionState, testkitState.getAsyncSessionStates() ) );
    }

    private <T> TestkitResponse createSessionStateAndResponse( TestkitState testkitState, BiFunction<Driver,SessionConfig,T> sessionStateProducer,
                                                               Map<String,T> sessionStateContainer )
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

        String newId = testkitState.newId();
        T sessionState = sessionStateProducer.apply( driver, builder.build() );
        sessionStateContainer.put( newId, sessionState );

        return Session.builder().data( Session.SessionBody.builder().id( newId ).build() ).build();
    }

    private SessionState createSessionState( Driver driver, SessionConfig sessionConfig )
    {
        return new SessionState( driver.session( sessionConfig ) );
    }

    private AsyncSessionState createAsyncSessionState( Driver driver, SessionConfig sessionConfig )
    {
        return new AsyncSessionState( driver.asyncSession( sessionConfig ) );
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
