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
package org.neo4j.driver.util.cc;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.util.TestUtil;

import static org.neo4j.driver.internal.Scheme.NEO4J_URI_SCHEME;

public class LocalOrRemoteClusterExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback
{
    private static final String CLUSTER_URI_SYSTEM_PROPERTY_NAME = "externalClusterUri";
    private static final String NEO4J_USER_PASSWORD_PROPERTY_NAME = "neo4jUserPassword";

    private ClusterExtension localClusterExtension;
    private URI clusterUri;

    public LocalOrRemoteClusterExtension()
    {
        assertValidSystemPropertiesDefined();
    }

    public URI getClusterUri()
    {
        return clusterUri;
    }

    public AuthToken getAuthToken()
    {
        if ( remoteClusterExists() )
        {
            return AuthTokens.basic( "neo4j", neo4jUserPasswordFromSystemProperty() );
        }
        return localClusterExtension.getDefaultAuthToken();
    }

    public Config.ConfigBuilder config( Config.ConfigBuilder builder )
    {
        if ( remoteClusterExists() )
        {
            builder.withEncryption();
        }
        else
        {
            builder.withoutEncryption();
        }

        return builder;
    }

    @Override
    public void beforeAll( ExtensionContext context ) throws Exception
    {
        if ( remoteClusterExists() )
        {
            clusterUri = remoteClusterUriFromSystemProperty();
            deleteDataInRemoteCluster();
        }
        else
        {
            localClusterExtension = new ClusterExtension();
            localClusterExtension.beforeAll( context );
            clusterUri = localClusterExtension.getCluster().leader().getRoutingUri();
        }
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        if ( remoteClusterExists() )
        {
            deleteDataInRemoteCluster();
        }
        else
        {
            localClusterExtension.afterEach( context );
        }
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        if ( !remoteClusterExists() )
        {
            localClusterExtension.afterAll( context );
        }
    }

    public void dumpClusterLogs()
    {
        if ( localClusterExtension != null )
        {
            localClusterExtension.getCluster().dumpClusterDebugLog();
        }
    }

    private void deleteDataInRemoteCluster()
    {
        Config.ConfigBuilder builder = Config.builder();
        builder.withEventLoopThreads( 1 );

        try ( Driver driver = GraphDatabase.driver( getClusterUri(), getAuthToken(), config( builder ).build() ) )
        {
            TestUtil.cleanDb( driver );
        }
    }

    private static void assertValidSystemPropertiesDefined()
    {
        URI uri = remoteClusterUriFromSystemProperty();
        String password = neo4jUserPasswordFromSystemProperty();
        if ( (uri != null && password == null) || (uri == null && password != null) )
        {
            throw new IllegalStateException(
                    "Both cluster uri and 'neo4j' user password system properties should be set. " +
                    "Uri: '" + uri + "', Password: '" + password + "'" );
        }
        if ( uri != null && !NEO4J_URI_SCHEME.equals( uri.getScheme() ) )
        {
            throw new IllegalStateException( "Cluster uri should have neo4j scheme: '" + uri + "'" );
        }
    }

    private static boolean remoteClusterExists()
    {
        return remoteClusterUriFromSystemProperty() != null;
    }

    private static URI remoteClusterUriFromSystemProperty()
    {
        String uri = System.getProperty( CLUSTER_URI_SYSTEM_PROPERTY_NAME );
        return uri == null ? null : URI.create( uri );
    }

    private static String neo4jUserPasswordFromSystemProperty()
    {
        return System.getProperty( NEO4J_USER_PASSWORD_PROPERTY_NAME );
    }
}
