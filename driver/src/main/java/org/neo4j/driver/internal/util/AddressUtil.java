/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.driver.internal.util;

import java.util.regex.Pattern;

public class AddressUtil
{
    private static final Pattern LOCALHOST = Pattern.compile( "^(localhost|127(\\.\\d+){3})$", Pattern.CASE_INSENSITIVE );

    /**
     * Return true if the host provided matches "localhost" or "127.x.x.x".
     *
     * @param host the host name to test
     * @return true if localhost, false otherwise
     */
    public static boolean isLocalhost( String host )
    {
        return LOCALHOST.matcher( host ).matches();
    }

}
