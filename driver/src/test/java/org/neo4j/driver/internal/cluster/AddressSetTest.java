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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.driver.internal.BoltServerAddress;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AddressSetTest
{
    @Test
    void shouldPreserveOrderWhenAdding() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = addresses( "one", "two", "tre" );

        AddressSet set = new AddressSet();
        set.update( servers );

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );

        // when
        servers.add( new BoltServerAddress( "fyr" ) );
        set.update( servers );

        // then
        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" ),
                new BoltServerAddress( "fyr" )}, set.toArray() );
    }

    @Test
    void shouldPreserveOrderWhenRemoving() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = addresses( "one", "two", "tre" );
        AddressSet set = new AddressSet();
        set.update( servers );

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );

        // when
        set.remove( new BoltServerAddress( "one" ) );

        // then
        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );
    }

    @Test
    void shouldPreserveOrderWhenRemovingThroughUpdate() throws Exception
    {
        // given
        Set<BoltServerAddress> servers = addresses( "one", "two", "tre" );
        AddressSet set = new AddressSet();
        set.update( servers );

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );

        // when
        servers.remove( new BoltServerAddress( "one" ) );
        set.update( servers );

        // then
        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, set.toArray() );
    }

    @Test
    void shouldExposeEmptyArrayWhenEmpty()
    {
        AddressSet addressSet = new AddressSet();

        BoltServerAddress[] addresses = addressSet.toArray();

        assertEquals( 0, addresses.length );
    }

    @Test
    void shouldExposeCorrectArray()
    {
        AddressSet addressSet = new AddressSet();
        addressSet.update( addresses( "one", "two", "tre" ) );

        BoltServerAddress[] addresses = addressSet.toArray();

        assertArrayEquals( new BoltServerAddress[]{
                new BoltServerAddress( "one" ),
                new BoltServerAddress( "two" ),
                new BoltServerAddress( "tre" )}, addresses );
    }

    @Test
    void shouldHaveSizeZeroWhenEmpty()
    {
        AddressSet addressSet = new AddressSet();

        assertEquals( 0, addressSet.size() );
    }

    @Test
    void shouldHaveCorrectSize()
    {
        AddressSet addressSet = new AddressSet();
        addressSet.update( addresses( "one", "two" ) );

        assertEquals( 2, addressSet.size() );
    }

    private static Set<BoltServerAddress> addresses( String... strings )
    {
        Set<BoltServerAddress> set = new LinkedHashSet<>();
        for ( String string : strings )
        {
            set.add( new BoltServerAddress( string ) );
        }
        return set;
    }
}
