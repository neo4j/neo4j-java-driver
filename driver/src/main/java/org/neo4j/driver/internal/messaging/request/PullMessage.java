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
package org.neo4j.driver.internal.messaging.request;

import static org.neo4j.driver.internal.util.MetadataExtractor.ABSENT_QUERY_ID;

/**
 * PULL request message
 * <p>
 * Sent by clients to pull the entirety of the remaining stream down.
 */
public class PullMessage extends AbstractStreamingMessage
{
    public static final byte SIGNATURE = 0x3F;
    public static final PullMessage PULL_ALL = new PullMessage( STREAM_LIMIT_UNLIMITED, ABSENT_QUERY_ID );

    public PullMessage( long n, long id )
    {
        super( n, id );
    }

    @Override
    protected String name()
    {
        return "PULL";
    }

    @Override
    public byte signature()
    {
        return SIGNATURE;
    }
}
