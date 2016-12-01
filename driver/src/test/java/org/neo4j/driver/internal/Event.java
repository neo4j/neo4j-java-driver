/*
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
package org.neo4j.driver.internal;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;

public abstract class Event<Handler>
{
    final Class<Handler> handlerType;

    @SuppressWarnings( "unchecked" )
    public Event()
    {
        this.handlerType = (Class<Handler>) handlerType( getClass() );
    }

    @Override
    public String toString()
    {
        StringWriter res = new StringWriter();
        try ( PrintWriter out = new PrintWriter( res ) )
        {
            EventHandler.write( this, out );
        }
        return res.toString();
    }

    public abstract void dispatch( Handler handler );

    private static Class<?> handlerType( Class<? extends Event> type )
    {
        for ( Class<?> c = type; c != Object.class; c = c.getSuperclass() )
        {
            for ( Method method : c.getDeclaredMethods() )
            {
                if ( method.getName().equals( "dispatch" )
                        && method.getParameterTypes().length == 1
                        && !method.isSynthetic() )
                {
                    return method.getParameterTypes()[0];
                }
            }
        }
        throw new Error( "Cannot determine Handler type from dispatch(Handler) method." );
    }
}
