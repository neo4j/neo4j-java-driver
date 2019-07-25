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
package org.neo4j.driver.internal.handlers;

import java.util.Map;

import org.neo4j.driver.internal.BookmarkHolder;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.MetadataExtractor;
import org.neo4j.driver.Statement;
import org.neo4j.driver.Value;

import static java.util.Objects.requireNonNull;

public class SessionPullAllResponseHandler extends AbstractPullAllResponseHandler
{
    private final BookmarkHolder bookmarkHolder;

    public SessionPullAllResponseHandler( Statement statement, RunResponseHandler runResponseHandler,
            Connection connection, BookmarkHolder bookmarkHolder, MetadataExtractor metadataExtractor )
    {
        super( statement, runResponseHandler, connection, metadataExtractor );
        this.bookmarkHolder = requireNonNull( bookmarkHolder );
    }

    @Override
    protected void afterSuccess( Map<String,Value> metadata )
    {
        releaseConnection();
        bookmarkHolder.setBookmark( metadataExtractor.extractBookmarks( metadata ) );
    }

    @Override
    protected void afterFailure( Throwable error )
    {
        releaseConnection();
    }

    private void releaseConnection()
    {
        connection.release(); // release in background
    }
}
