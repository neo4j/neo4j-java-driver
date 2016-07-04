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

package org.neo4j.driver.internal.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class AddressUtil
{
    /**
     * Return true if the host provided matches "localhost" or "127.x.x.x".
     *
     * @param host the host name to test
     * @return true if localhost, false otherwise
     */
    public static boolean isLocalHost( String host )
    {
        try
        {
            // confirmed to work as desired with both "localhost" and "127.x.x.x"
            return InetAddress.getByName( host ).isLoopbackAddress();
        }
        catch ( UnknownHostException e )
        {
            // if it's unknown, it's not local so we can safely return false
            return false;
        }
    }

}
