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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.neo4j.driver.internal.util.Iterables.map;

public class Neo4jSettings
{
    public static final String AUTH_ENABLED = "dbms.security.auth_enabled";
    public static final String DATA_DIR = "dbms.directories.data";
    public static final String CERT_DIR = "dbms.directories.certificates";
    public static final String IMPORT_DIR = "dbms.directories.import";

    private static final String DEFAULT_IMPORT_DIR = "import";
    private static final String DEFAULT_CERT_DIR = "certificates";
    private static final String DEFAULT_TLS_CERT_PATH = DEFAULT_CERT_DIR + "/neo4j.cert";
    private static final String DEFAULT_TLS_KEY_PATH = DEFAULT_CERT_DIR + "/neo4j.key";

    public static final String DEFAULT_DATA_DIR = "data";
    public static final File DEFAULT_TLS_KEY_FILE = new File( Neo4jRunner.NEO4J_HOME, DEFAULT_TLS_KEY_PATH );
    public static final File DEFAULT_TLS_CERT_FILE = new File( Neo4jRunner.NEO4J_HOME, DEFAULT_TLS_CERT_PATH );


    private final Map<String, String> settings;
    private final Set<String> excludes;

    public static Neo4jSettings DEFAULT_SETTINGS = new Neo4jSettings( new HashMap<String, String>(), Collections.<String>emptySet() );
    public static Neo4jSettings TEST_SETTINGS = new Neo4jSettings( map(
            CERT_DIR, DEFAULT_CERT_DIR,
            DATA_DIR, DEFAULT_DATA_DIR,
            IMPORT_DIR, DEFAULT_IMPORT_DIR,
            AUTH_ENABLED, "false" ), Collections.<String>emptySet() );

    private Neo4jSettings( Map<String, String> settings, Set<String> excludes )
    {
        this.settings = settings;
        this.excludes = excludes;
    }

    public Map<String, String> propertiesMap()
    {
        return settings;
    }

    public Neo4jSettings updateWith( Neo4jSettings other )
    {
        return updateWith( other.settings, other.excludes );
    }

    public Neo4jSettings updateWith( String key, String value )
    {
        return updateWith( map(key, value), excludes );
    }

    private Neo4jSettings updateWith( Map<String,String> updates, Set<String> excludes )
    {
        HashMap<String,String> newSettings = new HashMap<>( settings );
        for ( Map.Entry<String,String> entry : updates.entrySet() )
        {
            newSettings.put( entry.getKey(), entry.getValue() );
        }
        for ( String exclude : excludes )
        {
            newSettings.remove( exclude );
        }
        return new Neo4jSettings( newSettings, excludes );
    }

    public Neo4jSettings without(String key)
    {
        Set<String> newExcludes = new HashSet<>( excludes );
        newExcludes.add( key );
        Map<String,String> newMap = new HashMap<>( this.settings );
        newMap.remove( key );
        Neo4jSettings newSettings = new Neo4jSettings( newMap, newExcludes );
        return newSettings;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        Neo4jSettings that = (Neo4jSettings) o;

        if ( !settings.equals( that.settings ) )
        { return false; }
        return excludes.equals( that.excludes );

    }

    @Override
    public int hashCode()
    {
        return settings.hashCode();
    }

    public Set<String> excludes()
    {
        return excludes;
    }
}
