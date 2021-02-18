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
package org.neo4j.driver.internal.messaging;

import java.util.Objects;

public class BoltProtocolVersion implements Comparable<BoltProtocolVersion>
{
    private final int majorVersion;
    private final int minorVersion;

    public BoltProtocolVersion( int majorVersion, int minorVersion )
    {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    public static BoltProtocolVersion fromRawBytes( int rawVersion )
    {
        int major = rawVersion & 0x000000FF;
        int minor = ( rawVersion >> 8 ) & 0x000000FF;

        return new BoltProtocolVersion( major, minor );
    }

    public long getMinorVersion()
    {
        return minorVersion;
    }

    public long getMajorVersion()
    {
        return majorVersion;
    }

    public int toInt()
    {
        int shiftedMinor = minorVersion << 8;
        return shiftedMinor | majorVersion;
    }

    /**
     * @return the version in format X.Y where X is the major version and Y is the minor version
     */
    @Override
    public String toString()
    {
        return String.format( "%d.%d", majorVersion, minorVersion );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( minorVersion, majorVersion );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == this )
        {
            return true;
        }
        else if ( !(o instanceof BoltProtocolVersion) )
        {
            return false;
        }
        else
        {
            BoltProtocolVersion other = (BoltProtocolVersion) o;
            return this.getMajorVersion() == other.getMajorVersion() && this.getMinorVersion() == other.getMinorVersion();
        }
    }

    @Override
    public int compareTo( BoltProtocolVersion other )
    {
        int result = Integer.compare( majorVersion, other.majorVersion );

        if ( result == 0 )
        {
            return Integer.compare( minorVersion, other.minorVersion );
        }

        return result;
    }

    public static boolean isHttp( BoltProtocolVersion protocolVersion )
    {
        // server would respond with `HTTP..` We read 4 bytes to figure out the version. The first two are not used
        // and therefore parse the `P` (80) for major and `T` (84) for minor.
        return protocolVersion.getMajorVersion() == 80 && protocolVersion.getMinorVersion() == 84;
    }
}
