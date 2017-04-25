/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.cluster;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class RoutingContext
{
    public static final RoutingContext EMPTY = new RoutingContext();

    private final Map<String,String> context;

    private RoutingContext()
    {
        this.context = emptyMap();
    }

    public RoutingContext( URI uri )
    {
        this.context = unmodifiableMap( parseParameters( uri ) );
    }

    public boolean isDefined()
    {
        return !context.isEmpty();
    }

    public Map<String,String> asMap()
    {
        return context;
    }

    @Override
    public String toString()
    {
        return "RoutingContext" + context;
    }

    private static Map<String,String> parseParameters( URI uri )
    {
        String query = uri.getQuery();

        if ( query == null || query.isEmpty() )
        {
            return emptyMap();
        }

        Map<String,String> parameters = new HashMap<>();
        String[] pairs = query.split( "&" );
        for ( String pair : pairs )
        {
            String[] keyValue = pair.split( "=" );
            if ( keyValue.length != 2 )
            {
                throw new IllegalArgumentException(
                        "Invalid parameters: '" + pair + "' in URI '" + uri + "'" );
            }

            String key = trimAndVerify( keyValue[0], "key", uri );
            String value = trimAndVerify( keyValue[1], "value", uri );

            String previousValue = parameters.put( key, value );
            if ( previousValue != null )
            {
                throw new IllegalArgumentException(
                        "Duplicated query parameters with key '" + key + "' in URI '" + uri + "'" );
            }
        }
        return parameters;
    }

    private static String trimAndVerify( String string, String name, URI uri )
    {
        String result = string.trim();
        if ( result.isEmpty() )
        {
            throw new IllegalArgumentException( "Illegal empty " + name + " in URI query '" + uri + "'" );
        }
        return result;
    }
}
