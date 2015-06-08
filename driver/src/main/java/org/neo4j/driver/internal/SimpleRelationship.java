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
package org.neo4j.driver.internal;

import java.util.Collections;
import java.util.Map;

import org.neo4j.driver.Identity;
import org.neo4j.driver.Relationship;
import org.neo4j.driver.Value;

/**
 * {@link org.neo4j.driver.Relationship} implementation that directly contains type and properties
 * along with {@link org.neo4j.driver.Identity} values for start and end nodes.
 */
public class SimpleRelationship extends SimpleEntity implements Relationship
{
    private final Identity start;
    private final Identity end;
    private final String type;

    public SimpleRelationship( String id, String start, String end, String type )
    {
        this( Identities.identity( id ), Identities.identity( start ),
                Identities.identity( end ), type,
                Collections.<String,Value>emptyMap() );
    }

    public SimpleRelationship( String id, String start, String end, String type,
            Map<String,Value> properties )
    {
        this( Identities.identity( id ), Identities.identity( start ),
                Identities.identity( end ), type, properties );
    }

    public SimpleRelationship( Identity id, Identity start, Identity end, String type,
            Map<String,Value> properties )
    {
        super( id, properties );
        this.start = start;
        this.end = end;
        this.type = type;
    }

    @Override
    public Identity start()
    {
        return start;
    }

    @Override
    public Identity end()
    {
        return end;
    }

    @Override
    public String type()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "Relationship{" + super.toString() + ", " +
               "start=" + start +
               ", end=" + end +
               ", type='" + type + '\'' +
               '}';
    }
}
