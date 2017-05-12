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

package org.neo4j.driver.internal.summary;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.v1.summary.ServerInfo;

public class InternalServerInfo implements ServerInfo
{
    private final BoltServerAddress address;
    private final String version;
    private final String product;
    private final int major;
    private final int minor;

    public InternalServerInfo( BoltServerAddress address, String version )
    {
        this.address = address;
        this.version = version;
        if (version != null)
        {
            int slash = version.indexOf("/");
            if (slash >= 0)
            {
                product = version.substring(0, slash);
                String[] numberStrings = version.substring(slash + 1).split("\\.");
                int[] numbers = new int[2];
                for (int i = 0; i < 2; i++)
                {
                    try
                    {
                        numbers[i] = Integer.parseInt(numberStrings[i]);
                    }
                    catch ( IndexOutOfBoundsException | NumberFormatException e )
                    {
                        numbers[i] = 0;
                    }
                }
                major = numbers[0];
                minor = numbers[1];
            }
            else
            {
                product = null;
                major = 0;
                minor = 0;
            }
        }
        else
        {
            product = null;
            major = 0;
            minor = 0;
        }
    }

    public BoltServerAddress boltServerAddress()
    {
        return this.address;
    }

    @Override
    public String address()
    {
        return this.address.toString();
    }

    @Override
    public String version()
    {
        return version;
    }

    @Override
    public String product()
    {
        return product;
    }

    @Override
    public int major()
    {
        return major;
    }

    @Override
    public int minor()
    {
        return minor;
    }

    @Override
    public boolean atLeast( String product, int major, int minor )
    {
        return this.product.equals(product) && (this.major > major || (this.major == major && this.minor >= minor));
    }
}
