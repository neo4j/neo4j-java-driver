/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1;

import java.util.Map;

import org.neo4j.driver.internal.security.InternalAuthToken;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * This is a listing of the various methods of authentication supported by this
 * driver. The scheme used must be supported by the Neo4j Instance you are connecting
 * to.
 * @see GraphDatabase#driver(String, AuthToken)
 * @since 1.0
 */
public class AuthTokens
{
    /**
     * The basic authentication scheme, using a username and a password.
     * @param username this is the "principal", identifying who this token represents
     * @param password this is the "credential", proving the identity of the user
     * @return an authentication token that can be used to connect to Neo4j
     * @see GraphDatabase#driver(String, AuthToken)
     */
    public static AuthToken basic( String username, String password )
    {
        return new InternalAuthToken( parameters(
                "scheme", "basic",
                "principal", username,
                "credentials", password ).asMap( Values.ofValue() ) );
    }

    /**
     * The basic authentication scheme, using a username and a password.
     * @param username this is the "principal", identifying who this token represents
     * @param password this is the "credential", proving the identity of the user
     * @param realm this is the "realm", specifies the authentication provider
     * @return an authentication token that can be used to connect to Neo4j
     * @see GraphDatabase#driver(String, AuthToken)
     */
    public static AuthToken basic( String username, String password, String realm )
    {
        return new InternalAuthToken( parameters(
                "scheme", "basic",
                "principal", username,
                "credentials", password,
                "realm", realm).asMap( Values.ofValue() ) );
    }


    /**
     * A custom authentication token used for doing custom authentication on the server side.
     * @param principal this used to identify who this token represents
     * @param credentials this is credentials authenticating the principal
     * @param realm this is the "realm:, specifying the authentication provider.
     * @param scheme this it the authentication scheme, specifying what kind of authentication that should be used
     * @return an authentication token that can be used to connect to Neo4j
     * * @see GraphDatabase#driver(String, AuthToken)
     */
    public static AuthToken custom( String principal, String credentials, String realm, String scheme)
    {
        return new InternalAuthToken( parameters(
                "scheme", scheme,
                "principal", principal,
                "credentials", credentials,
                "realm", realm).asMap( Values.ofValue() ) );
    }

    /**
     * A custom authentication token used for doing custom authentication on the server side.
     * @param principal this used to identify who this token represents
     * @param credentials this is credentials authenticating the principal
     * @param realm this is the "realm:, specifying the authentication provider.
     * @param scheme this it the authentication scheme, specifying what kind of authentication that shoud be used
     * @param parameters extra parameters to be sent along the authentication provider.
     * @return an authentication token that can be used to connect to Neo4j
     * * @see GraphDatabase#driver(String, AuthToken)
     */
    public static AuthToken custom( String principal, String credentials, String realm, String scheme, Map<String, Object> parameters)
    {
        return new InternalAuthToken( parameters(
                "scheme", scheme,
                "principal", principal,
                "credentials", credentials,
                "realm", realm,
                "parameters", parameters).asMap( Values.ofValue() ) );
    }

    /**
     * No authentication scheme. This will only work if authentication is disabled
     * on the Neo4j Instance we are connecting to.
     * @return an authentication token that can be used to connect to Neo4j instances with auth disabled
     * @see GraphDatabase#driver(String, AuthToken)
     */
    public static AuthToken none()
    {
        return new InternalAuthToken( parameters( "scheme", "none" ).asMap( Values.ofValue() ) );
    }
}
