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
package org.neo4j.driver.v1.internal.summary;

import org.neo4j.driver.v1.Function;
import org.neo4j.driver.v1.InputPosition;
import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Value;

public class SimpleNotification implements Notification
{
    public static final Function<Value, Notification> VALUE_TO_NOTIFICATION = new Function<Value,Notification>()
    {
        @Override
        public Notification apply( Value value )
        {
            String code = value.get( "code" ).javaString();
            String title = value.get( "title" ).javaString();
            String description = value.get( "description" ).javaString();

            Value posValue = value.get( "position" );
            InputPosition position = null;
            if( posValue != null )
            {
                position = new SimpleInputPosition( posValue.get( "offset" ).javaInteger(),
                                                    posValue.get( "line" ).javaInteger(),
                                                    posValue.get( "column" ).javaInteger() );
            }

            Notification notification = new SimpleNotification( code, title, description, position );
            return notification;
        }
    };

    private final String code;
    private final String title;
    private final String description;
    private final InputPosition position;

    public SimpleNotification( String code, String title, String description, InputPosition position )
    {
        this.code = code;
        this.title = title;
        this.description = description;
        this.position = position;
    }

    @Override
    public String code()
    {
        return code;
    }

    @Override
    public String title()
    {
        return title;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public InputPosition position()
    {
        return position;
    }

    @Override
    public String toString()
    {
        String info = "code=" + code + ", title=" + title + ", description=" + description;
        return position == null ? info : info + ", position={" + position + "}";
    }
}
