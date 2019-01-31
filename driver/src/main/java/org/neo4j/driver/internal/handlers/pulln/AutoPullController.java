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
package org.neo4j.driver.internal.handlers.pulln;

import org.reactivestreams.Subscription;

public class AutoPullController implements PullController
{
    private final Subscription subscription;

    private static final int RECORD_BUFFER_LOW_WATERMARK = Integer.getInteger( "recordBufferLowWatermark", 3000 );
    private static final int RECORD_BUFFER_HIGH_WATERMARK = Integer.getInteger( "recordBufferHighWatermark", 10000 );
    private static final int AUTO_PULL_SIZE = RECORD_BUFFER_HIGH_WATERMARK;

    private boolean autoPullEnabled = true;

    public AutoPullController( Subscription subscription )
    {
        this.subscription = subscription;
        autoPull();
    }

    @Override
    public void handleSuccessWithHasMore()
    {
        if ( autoPullEnabled )
        {
            autoPull();
        }
    }

    @Override
    public void afterEnqueueRecord( int queueSize )
    {
        if ( queueSize >= RECORD_BUFFER_HIGH_WATERMARK )
        {
            autoPullEnabled = false;
        }
    }

    @Override
    public void afterDequeueRecord( int queueSize, boolean isStreamingPaused )
    {
        if ( queueSize < RECORD_BUFFER_LOW_WATERMARK )
        {
            autoPullEnabled = true;

            if ( isStreamingPaused )
            {
                // only pull if streaming is paused
                // if streaming is still on, then handleSuccessWithHasMore wil pull for us if there are still more records
                autoPull();
            }
        }
    }

    private void autoPull()
    {
        subscription.request( AUTO_PULL_SIZE );
    }
}
