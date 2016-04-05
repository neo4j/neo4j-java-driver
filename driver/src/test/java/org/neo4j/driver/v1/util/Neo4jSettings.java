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

    private static final String TLS_CERT_KEY = "dbms.security.tls_certificate_file";
    private static final String TLS_KEY_KEY = "dbms.security.tls_key_file";

    private static final String DEFAULT_TLS_CERT_PATH = "conf/ssl/snakeoil.cert";
    private static final String DEFAULT_TLS_KEY_PATH = "conf/ssl/snakeoil.key";

    public static final File DEFAULT_TLS_CERT_FILE = new File( Neo4jInstaller.neo4jHomeDir, DEFAULT_TLS_CERT_PATH );


    private final Map<String, String> settings;

    public static Neo4jSettings DEFAULT = new Neo4jSettings( map(
            TLS_CERT_KEY, DEFAULT_TLS_CERT_PATH,
            TLS_KEY_KEY, DEFAULT_TLS_KEY_PATH,
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

    public Neo4jSettings usingEncryptionKeyAndCert( File key, File cert )
    {
        return updateWith( map(
                TLS_CERT_KEY, cert.getAbsolutePath().replaceAll("\\\\", "/"),
                TLS_KEY_KEY, key.getAbsolutePath().replaceAll("\\\\", "/")
        ));
    }

    @Override
    public int hashCode()
    {
        return settings.hashCode();
    }
}
