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
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.DomainNameResolutionRequired;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.ResolverResolutionRequired;
import neo4j.org.testkit.backend.messages.responses.TestkitErrorResponse;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.internal.DefaultDomainNameResolver;
import org.neo4j.driver.internal.DomainNameResolver;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.SecuritySettings;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.net.ServerAddressResolver;

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
        if ( data.isResolverRegistered() )
        {
            configBuilder.withResolver( callbackResolver( testkitState ) );
        }
        DomainNameResolver domainNameResolver = DefaultDomainNameResolver.getInstance();
        if ( data.isDomainNameResolverRegistered() )
        {
            domainNameResolver = callbackDomainNameResolver( testkitState );
        }
        Optional.ofNullable( data.userAgent ).ifPresent( configBuilder::withUserAgent );
        Optional.ofNullable( data.connectionTimeoutMs ).ifPresent( timeout -> configBuilder.withConnectionTimeout( timeout, TimeUnit.MILLISECONDS ) );
        testkitState.getDrivers().putIfAbsent( id, driver( URI.create( data.uri ), authToken, configBuilder.build(), domainNameResolver ) );
        return Driver.builder().data( Driver.DriverBody.builder().id( id ).build() ).build();
    }

    private ServerAddressResolver callbackResolver( TestkitState testkitState )
    {
        return address ->
        {
            String callbackId = testkitState.newId();
            ResolverResolutionRequired.ResolverResolutionRequiredBody body =
                    ResolverResolutionRequired.ResolverResolutionRequiredBody.builder()
                                                                             .id( callbackId )
                                                                             .address( String.format( "%s:%d", address.host(), address.port() ) )
                                                                             .build();
            ResolverResolutionRequired response =
                    ResolverResolutionRequired.builder()
                                              .data( body )
                                              .build();
            testkitState.getResponseWriter().accept( response );
            testkitState.getProcessor().get();
            return testkitState.getIdToServerAddresses().remove( callbackId );
        };
    }

    private DomainNameResolver callbackDomainNameResolver( TestkitState testkitState )
    {
        return address ->
        {
            String callbackId = testkitState.newId();
            DomainNameResolutionRequired.DomainNameResolutionRequiredBody body =
                    DomainNameResolutionRequired.DomainNameResolutionRequiredBody.builder()
                                                                                 .id( callbackId )
                                                                                 .name( address )
                                                                                 .build();
            DomainNameResolutionRequired response =
                    DomainNameResolutionRequired.builder()
                                                .data( body )
                                                .build();
            testkitState.getResponseWriter().accept( response );
            testkitState.getProcessor().get();
            return testkitState.getIdToResolvedAddresses().remove( callbackId );
        };
    }

    private org.neo4j.driver.Driver driver( URI uri, AuthToken authToken, Config config, DomainNameResolver domainNameResolver )
    {
        RoutingSettings routingSettings = RoutingSettings.DEFAULT;
        RetrySettings retrySettings = RetrySettings.DEFAULT;
        SecuritySettings.SecuritySettingsBuilder securitySettingsBuilder = new SecuritySettings.SecuritySettingsBuilder();
        SecuritySettings securitySettings = securitySettingsBuilder.build();
        SecurityPlan securityPlan = securitySettings.createSecurityPlan( uri.getScheme() );
        return new DriverFactoryWithDomainNameResolver( domainNameResolver )
                .newInstance( uri, authToken, routingSettings, retrySettings, config, securityPlan );
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class NewDriverBody
    {
        private String uri;
        private AuthorizationToken authorizationToken;
        private String userAgent;
        private boolean resolverRegistered;
        private boolean domainNameResolverRegistered;
        private Long connectionTimeoutMs;
    }

    @RequiredArgsConstructor
    private static class DriverFactoryWithDomainNameResolver extends DriverFactory
    {
        private final DomainNameResolver domainNameResolver;

        @Override
        protected DomainNameResolver getDomainNameResolver()
        {
            return domainNameResolver;
        }
    }
}
