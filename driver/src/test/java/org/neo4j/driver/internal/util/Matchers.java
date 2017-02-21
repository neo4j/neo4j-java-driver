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
package org.neo4j.driver.internal.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.neo4j.driver.internal.cluster.RoundRobinAddressSet;
import org.neo4j.driver.internal.cluster.RoutingTable;
import org.neo4j.driver.internal.net.BoltServerAddress;

public final class Matchers
{
    private Matchers()
    {
    }

    public static Matcher<RoutingTable> containsRouter( final BoltServerAddress address )
    {
        return new TypeSafeMatcher<RoutingTable>()
        {
            @Override
            protected boolean matchesSafely( RoutingTable routingTable )
            {
                for ( int i = 0; i < routingTable.routerSize(); i++ )
                {
                    if ( routingTable.nextRouter().equals( address ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "routing table that contains router " ).appendValue( address );
            }
        };
    }

    public static Matcher<RoutingTable> containsReader( final BoltServerAddress address )
    {
        return new TypeSafeMatcher<RoutingTable>()
        {
            @Override
            protected boolean matchesSafely( RoutingTable routingTable )
            {
                return contains( routingTable.readers(), address );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "routing table that contains reader " ).appendValue( address );
            }
        };
    }

    public static Matcher<RoutingTable> containsWriter( final BoltServerAddress address )
    {
        return new TypeSafeMatcher<RoutingTable>()
        {
            @Override
            protected boolean matchesSafely( RoutingTable routingTable )
            {
                return contains( routingTable.writers(), address );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "routing table that contains writer " ).appendValue( address );
            }
        };
    }

    private static boolean contains( RoundRobinAddressSet set, BoltServerAddress address )
    {
        for ( int i = 0; i < set.size(); i++ )
        {
            if ( set.next().equals( address ) )
            {
                return true;
            }
        }
        return false;
    }
}
