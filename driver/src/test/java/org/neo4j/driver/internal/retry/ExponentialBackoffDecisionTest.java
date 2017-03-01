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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExponentialBackoffDecisionTest
{
    @Test
    public void shouldRetryAfterConstruction()
    {
        ExponentialBackoffDecision decision = new ExponentialBackoffDecision( 0, 0 );
        assertTrue( decision.shouldRetry() );
    }

    @Test
    public void startTimestampAfterConstruction()
    {
        ExponentialBackoffDecision decision = new ExponentialBackoffDecision( 42, 0 );
        assertEquals( 42, decision.startTimestamp() );
    }

    @Test
    public void delayAfterConstruction()
    {
        ExponentialBackoffDecision decision = new ExponentialBackoffDecision( 0, 42 );
        assertEquals( 42, decision.delay() );
    }

    @Test
    public void withDelayChangesTheDelay()
    {
        ExponentialBackoffDecision decision = new ExponentialBackoffDecision( 0, 42 );

        decision.withDelay( 4242 );

        assertEquals( 4242, decision.delay() );
    }

    @Test
    public void shouldNotRetryAfterStopRetrying()
    {
        ExponentialBackoffDecision decision = new ExponentialBackoffDecision( 0, 0 );

        decision.stopRetrying();

        assertFalse( decision.shouldRetry() );
    }
}
