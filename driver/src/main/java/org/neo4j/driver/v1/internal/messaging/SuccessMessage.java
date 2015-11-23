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
package org.neo4j.driver.v1.internal.messaging;

import java.io.IOException;
import java.util.Map;

import org.neo4j.driver.v1.Value;

import static java.lang.String.format;

/**
 * SUCCESS response message
 * <p>
 * Sent by the server to signal a successful operation.
 * Terminates response sequence.
 */
public class SuccessMessage implements Message
{
    private final Map<String,Value> metadata;

    public SuccessMessage( Map<String,Value> metadata )
    {
        this.metadata = metadata;
    }

    @Override
    public void dispatch( MessageHandler handler ) throws IOException
    {
        handler.handleSuccessMessage( metadata );
    }

    @Override
    public String toString()
    {
        return format( "[SUCCESS %s]", metadata );
    }

    @Override
    public boolean equals( Object obj )
    {
        return obj != null && obj.getClass() == getClass();
    }

    @Override
    public int hashCode()
    {
        return 1;
    }
}
