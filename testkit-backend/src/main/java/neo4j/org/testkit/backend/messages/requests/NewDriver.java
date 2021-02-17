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
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.TestkitErrorResponse;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Optional;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.GraphDatabase;

@Setter
@Getter
@NoArgsConstructor
public class NewDriver implements TestkitRequest
{
    private NewDriverBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        String id = testkitState.newId();

        AuthToken authToken;
        switch ( data.getAuthorizationToken().getTokens().get( "scheme" ) )
        {
        case "basic":
            authToken = AuthTokens.basic( data.authorizationToken.getTokens().get( "principal" ),
                                          data.authorizationToken.getTokens().get( "credentials" ),
                                          data.authorizationToken.getTokens().get( "realm" ) );
            break;
        default:
            return TestkitErrorResponse.builder().errorMessage( "Auth scheme " + data.authorizationToken.getTokens().get( "scheme" ) + "not implemented" )
                                       .build();
        }

        Config.ConfigBuilder configBuilder = Config.builder();
        Optional.ofNullable( data.userAgent ).ifPresent( configBuilder::withUserAgent );
        testkitState.getDrivers().putIfAbsent( id, GraphDatabase.driver( data.uri, authToken, configBuilder.build() ) );
        return Driver.builder().data( Driver.DriverBody.builder().id( id ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class NewDriverBody
    {
        private String uri;
        private AuthorizationToken authorizationToken;
        private String userAgent;
    }
}
