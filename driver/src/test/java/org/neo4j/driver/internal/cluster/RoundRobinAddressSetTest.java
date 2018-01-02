/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.driver.internal.net.BoltServerAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class RoundRobinAddressSetTest
{
    @Test
    public void shouldReturnNullWhenEmpty() throws Exception
    {
        // given
        RoundRobinAddressSet set = new RoundRobinAddressSet();

        // then
        assertNull( set.next() );
    }

    @Test
    public void shouldReturnRoundRobin() throws Exception
    {
        // given
        RoundRobinAddressSet set = new RoundRobinAddressSet();
        Set<BoltServerAddress> addresses = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );

        set.update( addresses );

        // when
        BoltServerAddress a = set.next();
        BoltServerAddress b = set.next();
        BoltServerAddress c = set.next();

        // then
        assertEquals( a, set.next() );
        assertEquals( b, set.next() );
        assertEquals( c, set.next() );
        assertEquals( a, set.next() );
        assertEquals( b, set.next() );
        assertEquals( c, set.next() );
        assertNotEquals( a, c );
        assertNotEquals( b, a );
        assertNotEquals( c, b );
    }

    @Test
    public void shouldPreserveOrderWhenAdding() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );
        RoundRobinAddressSet set = new RoundRobinAddressSet();
        set.update( servers );

        List<BoltServerAddress> order = new ArrayList<>();
        for ( int i = 3 * 4 + 1; i-- > 0; )
        {
            BoltServerAddress server = set.next();
            if ( !order.contains( server ) )
            {
                order.add( server );
            }
        }
        assertEquals( 3, order.size() );

        // when
        servers.add( new BoltServerAddress( "fyr" ) );
        set.update( servers );

        // then
        assertEquals( order.get( 1 ), set.next() );
        assertEquals( order.get( 2 ), set.next() );
        BoltServerAddress next = set.next();
        assertNotEquals( order.get( 0 ), next );
        assertNotEquals( order.get( 1 ), next );
        assertNotEquals( order.get( 2 ), next );
        assertEquals( order.get( 0 ), set.next() );
        // ... and once more
        assertEquals( order.get( 1 ), set.next() );
        assertEquals( order.get( 2 ), set.next() );
        assertEquals( next, set.next() );
        assertEquals( order.get( 0 ), set.next() );
    }

    @Test
    public void shouldPreserveOrderWhenRemoving() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );
        RoundRobinAddressSet set = new RoundRobinAddressSet();
        set.update( servers );

        List<BoltServerAddress> order = new ArrayList<>();
        for ( int i = 3 * 2 + 1; i-- > 0; )
        {
            BoltServerAddress server = set.next();
            if ( !order.contains( server ) )
            {
                order.add( server );
            }
        }
        assertEquals( 3, order.size() );

        // when
        set.remove( order.get( 1 ) );

        // then
        assertEquals( order.get( 2 ), set.next() );
        assertEquals( order.get( 0 ), set.next() );
        assertEquals( order.get( 2 ), set.next() );
        assertEquals( order.get( 0 ), set.next() );
    }

    @Test
    public void shouldPreserveOrderWhenRemovingThroughUpdate() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = new LinkedHashSet<>( asList(
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ) ) );
        RoundRobinAddressSet set = new RoundRobinAddressSet();
        set.update( servers );

        List<BoltServerAddress> order = new ArrayList<>();
        for ( int i = 3 * 2 + 1; i-- > 0; )
        {
            BoltServerAddress server = set.next();
            if ( !order.contains( server ) )
            {
                order.add( server );
            }
        }
        assertEquals( 3, order.size() );

        // when
        servers.remove( order.get( 1 ) );
        set.update( servers );

        // then
        assertEquals( order.get( 2 ), set.next() );
        assertEquals( order.get( 0 ), set.next() );
        assertEquals( order.get( 2 ), set.next() );
        assertEquals( order.get( 0 ), set.next() );
    }



    @Test
    public void shouldPreserveOrderEvenWhenIntegerOverflows() throws Exception
    {
        // given
        RoundRobinAddressSet set = new RoundRobinAddressSet();

        for ( int div = 1; div <= 1024; div++ )
        {
            // when - white box testing!
            set.setOffset( Integer.MAX_VALUE - 1 );
            int a = set.next( div );
            int b = set.next( div );

            // then
            if ( b != (a + 1) % div )
            {
                fail( String.format( "a=%d, b=%d, div=%d, (a+1)%%div=%d", a, b, div, (a + 1) % div ) );
            }
        }
    }
}
