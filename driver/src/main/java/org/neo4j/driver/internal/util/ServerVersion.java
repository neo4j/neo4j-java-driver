/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import static java.lang.Integer.compare;

public class ServerVersion
{
    public static final ServerVersion v3_5_0 = new ServerVersion( 3, 5, 0 );
    public static final ServerVersion v3_4_0 = new ServerVersion( 3, 4, 0 );
    public static final ServerVersion v3_2_0 = new ServerVersion( 3, 2, 0 );
    public static final ServerVersion v3_1_0 = new ServerVersion( 3, 1, 0 );
    public static final ServerVersion v3_0_0 = new ServerVersion( 3, 0, 0 );
    public static final ServerVersion vInDev = new ServerVersion( Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE );

    private static final String NEO4J_IN_DEV_VERSION_STRING = "Neo4j/dev";
    private static final Pattern PATTERN =
            Pattern.compile( "(Neo4j/)?(\\d+)\\.(\\d+)(?:\\.)?(\\d*)(\\.|-|\\+)?([0-9A-Za-z-.]*)?" );

    private final int major;
    private final int minor;
    private final int patch;
    private final String stringValue;

    private ServerVersion( int major, int minor, int patch )
    {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.stringValue = stringValue( major, minor, patch );
    }

    public static ServerVersion version( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            String versionString = session.run( "RETURN 1" ).consume().server().version();
            return version( versionString );
        }
    }

    public static ServerVersion version( String server )
    {
        if ( server == null )
        {
            return v3_0_0;
        }
        else
        {
            Matcher matcher = PATTERN.matcher( server );
            if ( matcher.matches() )
            {
                int major = Integer.valueOf( matcher.group( 2 ) );
                int minor = Integer.valueOf( matcher.group( 3 ) );
                String patchString = matcher.group( 4 );
                int patch = 0;
                if ( patchString != null && !patchString.isEmpty() )
                {
                    patch = Integer.valueOf( patchString );
                }
                return new ServerVersion( major, minor, patch );
            }
            else if ( server.equalsIgnoreCase( NEO4J_IN_DEV_VERSION_STRING ) )
            {
                return vInDev;
            }
            else
            {
                throw new IllegalArgumentException( "Cannot parse " + server );
            }
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        ServerVersion that = (ServerVersion) o;

        if ( major != that.major )
        { return false; }
        if ( minor != that.minor )
        { return false; }
        return patch == that.patch;
    }

    @Override
    public int hashCode()
    {
        int result = major;
        result = 31 * result + minor;
        result = 31 * result + patch;
        return result;
    }

    public boolean greaterThan(ServerVersion other)
    {
        return compareTo( other ) > 0;
    }

    public boolean greaterThanOrEqual(ServerVersion other)
    {
        return compareTo( other ) >= 0;
    }

    public boolean lessThan(ServerVersion other)
    {
        return compareTo( other ) < 0;
    }

    public boolean lessThanOrEqual(ServerVersion other)
    {
        return compareTo( other ) <= 0;
    }

    private int compareTo( ServerVersion o )
    {
        int c = compare( major, o.major );
        if (c == 0)
        {
            c = compare( minor, o.minor );
            if (c == 0)
            {
                c = compare( patch, o.patch );
            }
        }

        return c;
    }

    @Override
    public String toString()
    {
        return stringValue;
    }

    private static String stringValue( int major, int minor, int patch )
    {
        if ( major == Integer.MAX_VALUE && minor == Integer.MAX_VALUE && patch == Integer.MAX_VALUE )
        {
            return NEO4J_IN_DEV_VERSION_STRING;
        }
        return String.format( "Neo4j/%s.%s.%s", major, minor, patch );
    }
}
