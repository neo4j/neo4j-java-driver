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

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.driver.internal.net.BoltServerAddress;

public class RoundRobinAddressSet
{
    private static final BoltServerAddress[] NONE = {};
    private final AtomicInteger offset = new AtomicInteger();
    private volatile BoltServerAddress[] addresses = NONE;

    public int size()
    {
        return addresses.length;
    }

    public BoltServerAddress next()
    {
        BoltServerAddress[] addresses = this.addresses;
        if ( addresses.length == 0 )
        {
            return null;
        }
        return addresses[next( addresses.length )];
    }

    int next( int divisor )
    {
        int index = offset.getAndIncrement();
        for ( ; index == Integer.MAX_VALUE; index = offset.getAndIncrement() )
        {
            offset.compareAndSet( Integer.MIN_VALUE, index % divisor );
        }
        return index % divisor;
    }

    public synchronized void update( Set<BoltServerAddress> addresses, Set<BoltServerAddress> removed )
    {
        BoltServerAddress[] prev = this.addresses;
        if ( addresses.isEmpty() )
        {
            this.addresses = NONE;
            return;
        }
        if ( prev.length == 0 )
        {
            this.addresses = addresses.toArray( NONE );
            return;
        }
        BoltServerAddress[] copy = null;
        if ( addresses.size() != prev.length )
        {
            copy = new BoltServerAddress[addresses.size()];
        }
        int j = 0;
        for ( int i = 0; i < prev.length; i++ )
        {
            if ( addresses.remove( prev[i] ) )
            {
                if ( copy != null )
                {
                    copy[j++] = prev[i];
                }
            }
            else
            {
                removed.add( prev[i] );
                if ( copy == null )
                {
                    copy = new BoltServerAddress[prev.length];
                    System.arraycopy( prev, 0, copy, 0, i );
                    j = i;
                }
            }
        }
        if ( copy == null )
        {
            return;
        }
        for ( BoltServerAddress address : addresses )
        {
            copy[j++] = address;
        }
        this.addresses = copy;
    }

    public synchronized void remove( BoltServerAddress address )
    {
        BoltServerAddress[] addresses = this.addresses;
        if ( addresses != null )
        {
            for ( int i = 0; i < addresses.length; i++ )
            {
                if ( addresses[i].equals( address ) )
                {
                    if ( addresses.length == 1 )
                    {
                        this.addresses = NONE;
                        return;
                    }
                    BoltServerAddress[] copy = new BoltServerAddress[addresses.length - 1];
                    System.arraycopy( addresses, 0, copy, 0, i );
                    System.arraycopy( addresses, i + 1, copy, i, addresses.length - i - 1 );
                    this.addresses = copy;
                    return;
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return "RoundRobinAddressSet=" + Arrays.toString( addresses );
    }

    /** breaking encapsulation in order to perform white-box testing of boundary case */
    void setOffset( int target )
    {
        offset.set( target );
    }
}
