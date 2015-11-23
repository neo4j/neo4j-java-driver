/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;

public class Neo4jSettings
{
    private final Boolean usingTLS;

    public static Neo4jSettings DEFAULT = new Neo4jSettings( false );

    private Neo4jSettings( Boolean usingTLS )
    {
        this.usingTLS = usingTLS;
    }

    public Neo4jSettings usingTLS( Boolean usingTLS )
    {
        return new Neo4jSettings( usingTLS );
    }

    public Boolean isUsingTLS()
    {
        return usingTLS;
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

        return !(usingTLS != null ? !usingTLS.equals( that.usingTLS ) : that.usingTLS != null);

    }

    @Override
    public int hashCode()
    {
        return usingTLS != null ? usingTLS.hashCode() : 0;
    }

    public Map<String, Object> propertiesMap()
    {
        Map<String, Object> props = new HashMap<>( 1 );
        putProperty( props, "dbms.bolt.tls.enabled", usingTLS );
        return props;
    }

    public Neo4jSettings updateWith( Neo4jSettings other )
    {
        return new Neo4jSettings( updateWith( usingTLS, other.isUsingTLS() ) );
    }

    private void putProperty( Map<String, Object> props, String key, Object value )
    {
        if ( value != null )
        {
            props.put( key, value );
        }
    }

    private <T> T updateWith( T left, T right )
    {
        return right == null ? left : right;
    }
}
