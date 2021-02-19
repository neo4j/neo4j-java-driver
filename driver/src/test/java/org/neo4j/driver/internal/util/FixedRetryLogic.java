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
package org.neo4j.driver.internal.util;

import io.netty.util.concurrent.EventExecutorGroup;

import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;

import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

public class FixedRetryLogic extends ExponentialBackoffRetryLogic
{
    private final int retryCount;
    private int invocationCount;

    public FixedRetryLogic( int retryCount )
    {
        this( retryCount, new ImmediateSchedulingEventExecutor() );
    }

    public FixedRetryLogic( int retryCount, EventExecutorGroup eventExecutorGroup )
    {
        super( new RetrySettings( Long.MAX_VALUE ), eventExecutorGroup, new SleeplessClock(), DEV_NULL_LOGGING );
        this.retryCount = retryCount;
    }

    @Override
    protected boolean canRetryOn( Throwable error )
    {
        return invocationCount++ < retryCount;
    }
}
