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
package org.neo4j.driver.v1.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.driver.internal.util.Iterables.map;

public class Neo4jSettings
{
    public static final String AUTH_ENABLED = "dbms.security.auth_enabled";
    public static final String DATA_DIR = "dbms.directories.data";
    public static final String CERT_DIR = "dbms.directories.certificates";

    private static final String DEFAULT_CERT_DIR = "certificates";
    private static final String DEFAULT_TLS_CERT_PATH = DEFAULT_CERT_DIR + "/neo4j.cert";
    private static final String DEFAULT_TLS_KEY_PATH = DEFAULT_CERT_DIR + "/neo4j.key";

    public static final File DEFAULT_TLS_KEY_FILE = new File( Neo4jInstaller.neo4jHomeDir, DEFAULT_TLS_KEY_PATH );
    public static final File DEFAULT_TLS_CERT_FILE = new File( Neo4jInstaller.neo4jHomeDir, DEFAULT_TLS_CERT_PATH );


    private final Map<String, String> settings;

    public static Neo4jSettings DEFAULT = new Neo4jSettings( map(
            CERT_DIR, DEFAULT_CERT_DIR,
            AUTH_ENABLED, "false" ) );

    private Neo4jSettings( Map<String, String> settings )
    {
        this.settings = settings;
    }

    public Map<String, String> propertiesMap()
    {
        return settings;
    }

    public Neo4jSettings updateWith( Neo4jSettings other )
    {
        return updateWith( other.settings );
    }

    public Neo4jSettings updateWith( String key, String value )
    {
        return updateWith( map(key, value) );
    }

    private Neo4jSettings updateWith( Map<String,String> updates )
    {
        HashMap<String,String> newSettings = new HashMap<>( settings );
        for ( Map.Entry<String,String> entry : updates.entrySet() )
        {
            newSettings.put( entry.getKey(), entry.getValue() );
        }
        return new Neo4jSettings( newSettings );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }

        Neo4jSettings that = (Neo4jSettings) o;

        return settings.equals( that.settings );
    }

    @Override
    public int hashCode()
    {
        return settings.hashCode();
    }
}
