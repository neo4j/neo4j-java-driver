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

import java.util.Arrays;
import java.util.Set;

import org.neo4j.driver.internal.BoltServerAddress;

public class AddressSet
{
    private static final BoltServerAddress[] NONE = {};

    private volatile BoltServerAddress[] addresses = NONE;

    public BoltServerAddress[] toArray()
    {
        return addresses;
    }

    public int size()
    {
        return addresses.length;
    }

    public synchronized void update( Set<BoltServerAddress> addresses )
    {
        this.addresses = addresses.toArray( NONE );
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
        return "AddressSet=" + Arrays.toString( addresses );
    }
}
