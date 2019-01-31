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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class AutoPullControllerTest
{
    @Test
    void shouldAutoPullAtCreation() throws Throwable
    {
        // Given
        Subscription subscription = mock( Subscription.class );

        // When
        new AutoPullController( subscription );

        // Then
        verify( subscription ).request( anyLong() );
    }

    @Test
    void shouldAutoPullAfterSuccessWithHasMore() throws Throwable
    {
        // Given
        Subscription subscription = mock( Subscription.class );
        AutoPullController controller = new AutoPullController( subscription );
        clearInvocations( subscription );

        // When
        controller.handleSuccessWithHasMore();

        // Then
        verify( subscription ).request( anyLong() );
    }


    @Test
    void shouldDisableAutoPullUntilBufferedRecordsAreConsumed() throws Throwable
    {
        // Given
        Subscription subscription = mock( Subscription.class );
        AutoPullController controller = new AutoPullController( subscription );
        clearInvocations( subscription );

        // When
        controller.afterEnqueueRecord( 10000000 ); // The queue is really huge!

        // Then
        controller.handleSuccessWithHasMore(); // streaming is paused
        verifyNoMoreInteractions( subscription ); // no auto pull as too many records unconsumed.

        controller.afterDequeueRecord( 1, true ); // now we finally get the bottom of the queue
        verify( subscription ).request( anyLong() );
    }
}
