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

public class RetryWithDelayDecisionTest
{
    @Test
    public void newlyCreatedDecisionShouldRetry()
    {
        assertTrue( new RetryWithDelayDecision().shouldRetry() );
    }

    @Test
    public void newlyCreatedDecisionIsFirstAttempt()
    {
        assertEquals( 1, new RetryWithDelayDecision().attempt() );
    }

    @Test
    public void incrementAttempt()
    {
        RetryWithDelayDecision decision = new RetryWithDelayDecision();

        decision.incrementAttempt();
        decision.incrementAttempt();
        decision.incrementAttempt();

        assertEquals( 4, decision.attempt() );
    }

    @Test
    public void stopRetrying()
    {
        RetryWithDelayDecision decision = new RetryWithDelayDecision();
        assertTrue( decision.shouldRetry() );

        decision.stopRetrying();

        assertFalse( decision.shouldRetry() );
    }

    @Test
    public void stopRetryingWhenOverflow()
    {
        RetryWithDelayDecision decision = new RetryWithDelayDecision( Integer.MAX_VALUE - 1 );
        assertTrue( decision.shouldRetry() );

        decision.incrementAttempt();

        assertFalse( decision.shouldRetry() );
    }
}
