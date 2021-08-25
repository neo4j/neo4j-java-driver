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
import lombok.Setter;
import neo4j.org.testkit.backend.AsyncSessionState;
import neo4j.org.testkit.backend.RxSessionState;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Setter
@Getter
public class RetryableNegative implements TestkitRequest
{
    private RetryableNegativeBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        SessionState sessionState = testkitState.getSessionStates().getOrDefault( data.sessionId, null );
        if ( sessionState == null )
        {
            throw new RuntimeException( "Could not find session" );
        }
        Throwable throwable;
        if ( !"".equals( data.getErrorId() ) )
        {
            throwable = testkitState.getErrors().get( data.getErrorId() );
        }
        else
        {
            throwable = new RuntimeException( "Error from client in retryable tx" );
        }
        sessionState.getTxWorkFuture().completeExceptionally( throwable );
        return null;
    }

    @Override
    public CompletionStage<TestkitResponse> processAsync( TestkitState testkitState )
    {
        AsyncSessionState sessionState = testkitState.getAsyncSessionStates().get( data.getSessionId() );
        Throwable throwable;
        if ( !"".equals( data.getErrorId() ) )
        {
            throwable = testkitState.getErrors().get( data.getErrorId() );
        }
        else
        {
            throwable = new RuntimeException( "Error from client in retryable tx" );
        }
        sessionState.getTxWorkFuture().completeExceptionally( throwable );
        return CompletableFuture.completedFuture( null );
    }

    @Override
    public Mono<TestkitResponse> processRx( TestkitState testkitState )
    {
        RxSessionState sessionState = testkitState.getRxSessionStates().get( data.getSessionId() );
        Throwable throwable;
        if ( !"".equals( data.getErrorId() ) )
        {
            throwable = testkitState.getErrors().get( data.getErrorId() );
        }
        else
        {
            throwable = new RuntimeException( "Error from client in retryable tx" );
        }
        sessionState.getTxWorkFuture().completeExceptionally( throwable );
        return Mono.empty();
    }

    @Setter
    @Getter
    public static class RetryableNegativeBody
    {
        private String sessionId;
        private String errorId;
    }
}
