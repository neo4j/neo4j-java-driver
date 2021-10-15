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
import java.util.Iterator;
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

    /**
     * Updates addresses using the provided set.
     * <p>
     * It aims to retain existing addresses by checking if they are present in the new set. To benefit from this, the provided set MUST contain specifically
     * {@link BoltServerAddress} instances with equal host and connection host values.
     *
     * @param newAddresses the new address set.
     */
    public synchronized void retainAllAndAdd( Set<BoltServerAddress> newAddresses )
    {
        BoltServerAddress[] addressesArr = new BoltServerAddress[newAddresses.size()];
        int insertionIdx = 0;
        for ( BoltServerAddress address : addresses )
        {
            BoltServerAddress lookupAddress =
                    BoltServerAddress.class.equals( address.getClass() ) ? address : new BoltServerAddress( address.host(), address.port() );
            if ( newAddresses.remove( lookupAddress ) )
            {
                addressesArr[insertionIdx] = address;
                insertionIdx++;
            }
        }
        Iterator<BoltServerAddress> addressIterator = newAddresses.iterator();
        for ( ; insertionIdx < addressesArr.length && addressIterator.hasNext(); insertionIdx++ )
        {
            addressesArr[insertionIdx] = addressIterator.next();
        }
        addresses = addressesArr;
    }

    public synchronized void replaceIfPresent( BoltServerAddress oldAddress, BoltServerAddress newAddress )
    {
        for ( int i = 0; i < addresses.length; i++ )
        {
            if ( addresses[i].equals( oldAddress ) )
            {
                addresses[i] = newAddress;
            }
        }
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
