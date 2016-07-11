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
