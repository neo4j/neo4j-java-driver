/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.retry;

public final class RetrySettings
{
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final int DEFAULT_DELAY_MS = 2_000;
    public static final RetrySettings DEFAULT = new RetrySettings( DEFAULT_MAX_ATTEMPTS, DEFAULT_DELAY_MS );

    private final int maxAttempts;
    private final long delayMs;

    public RetrySettings( int maxAttempts, long delayMs )
    {
        this.maxAttempts = maxAttempts;
        this.delayMs = delayMs;
    }

    public int maxAttempts()
    {
        return maxAttempts;
    }

    public long delayMs()
    {
        return delayMs;
    }
}
