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
package org.neo4j.driver.stress;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.summary.ResultSummary;

public abstract class AbstractContext
{
    private volatile boolean stopped;
    private volatile Bookmark bookmark;
    private final AtomicLong readNodesCount = new AtomicLong();
    private final AtomicLong createdNodesCount = new AtomicLong();
    private final AtomicInteger bookmarkFailures = new AtomicInteger();

    public final boolean isStopped()
    {
        return stopped;
    }

    public final void stop()
    {
        this.stopped = true;
    }

    public final Bookmark getBookmark()
    {
        return bookmark;
    }

    public final void setBookmark( Bookmark bookmark )
    {
        this.bookmark = bookmark;
    }

    public final void nodeCreated()
    {
        createdNodesCount.incrementAndGet();
    }

    public final long getCreatedNodesCount()
    {
        return createdNodesCount.get();
    }

    public final void readCompleted( ResultSummary summary )
    {
        readNodesCount.incrementAndGet();
    }

    public long getReadNodesCount()
    {
        return readNodesCount.get();
    }

    public final void bookmarkFailed()
    {
        bookmarkFailures.incrementAndGet();
    }

    public final int getBookmarkFailures()
    {
        return bookmarkFailures.get();
    }
}
