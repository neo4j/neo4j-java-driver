/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.driver.util;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.driver.internal.util.Iterables.map;

public class Neo4jSettings
{
    public static final String AUTH_ENABLED = "dbms.security.auth_enabled";
    public static final String BOLT_TLS_LEVEL = "dbms.connector.bolt.tls_level";

    public static final String DATA_DIR = "dbms.directories.data";
    private static final String DEFAULT_DATA_DIR = "data";

    static final String DEFAULT_LOG_DIR = "log";
    static final String DEFAULT_IMPORT_DIR = "import";
    static final String DEFAULT_CERT_DIR = "certificates";

    static final String DEFAULT_TLS_CERT_FILE = "neo4j.cert";
    static final String DEFAULT_TLS_KEY_FILE = "neo4j.key";
    private static final String DEFAULT_BOLT_TLS_LEVEL = BoltTlsLevel.OPTIONAL.toString();

    static final int TEST_JVM_ID = Integer.getInteger( "testJvmId", 0 );

    private static final int DEFAULT_HTTP_PORT = 7000;
    private static final int DEFAULT_HTTPS_PORT = 8000;
    private static final int DEFAULT_BOLT_PORT = 9000;

    static final int CURRENT_HTTP_PORT = DEFAULT_HTTP_PORT + TEST_JVM_ID;
    static final int CURRENT_HTTPS_PORT = DEFAULT_HTTPS_PORT + TEST_JVM_ID;
    static final int CURRENT_BOLT_PORT = DEFAULT_BOLT_PORT + TEST_JVM_ID;

    private final Map<String, String> settings;

    public static final Neo4jSettings TEST_SETTINGS = new Neo4jSettings( map(
//            DATA_DIR, DEFAULT_DATA_DIR,
            AUTH_ENABLED, "true",
            BOLT_TLS_LEVEL, DEFAULT_BOLT_TLS_LEVEL ) );

    public enum BoltTlsLevel
    {
        OPTIONAL,
        REQUIRED,
        DISABLED
    }

    public Neo4jSettings( Map<String,String> settings )
    {
        this.settings = settings;
    }

    public Map<String,String> propertiesMap()
    {
        return settings;
    }

    public Neo4jSettings updateWith( Neo4jSettings other )
    {
        return updateWith( other.settings );
    }

    public Neo4jSettings updateWith( String key, String value )
    {
        return updateWith( map( key, value ) );
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
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Neo4jSettings that = (Neo4jSettings) o;

        return settings.equals( that.settings );
    }

    @Override
    public int hashCode()
    {
        return settings.hashCode();
    }
}
