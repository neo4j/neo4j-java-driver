/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.basicimpl.async.connection;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.internal.util.Iterables.count;
import static org.neo4j.driver.internal.util.Matchers.blockingOperationInEventLoopError;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventLoopGroupFactoryTest {
    private EventLoopGroup eventLoopGroup;

    @AfterEach
    void tearDown() {
        shutdown(eventLoopGroup);
    }

    @Test
    void shouldReturnCorrectChannelClass() {
        assertEquals(NioSocketChannel.class, EventLoopGroupFactory.channelClass());
    }

    @Test
    void shouldCreateEventLoopGroupWithSpecifiedThreadCount() {
        var threadCount = 2;
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup(threadCount);
        assertEquals(threadCount, count(eventLoopGroup));
        assertThat(eventLoopGroup, instanceOf(NioEventLoopGroup.class));
    }

    @Test
    void shouldAssertNotInEventLoopThread() {
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup(1);

        // current thread is not an event loop thread, assertion should not throw
        EventLoopGroupFactory.assertNotInEventLoopThread();

        // submit assertion to the event loop thread, it should fail there
        var assertFuture = eventLoopGroup.submit(EventLoopGroupFactory::assertNotInEventLoopThread);

        var error = assertThrows(ExecutionException.class, () -> assertFuture.get(30, SECONDS));
        assertThat(error.getCause(), is(blockingOperationInEventLoopError()));
    }

    @Test
    void shouldCheckIfEventLoopThread() throws Exception {
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup(1);

        var eventLoopThread = getThread(eventLoopGroup);
        assertTrue(EventLoopGroupFactory.isEventLoopThread(eventLoopThread));

        assertFalse(EventLoopGroupFactory.isEventLoopThread(Thread.currentThread()));
    }

    /**
     * Test verifies that our event loop group uses same kind of thread as Netty does by default.
     * It's needed because default Netty setup has good performance.
     */
    @Test
    void shouldUseSameThreadClassAsNioEventLoopGroupDoesByDefault() throws Exception {
        var nioEventLoopGroup = new NioEventLoopGroup(1);
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup(1);
        try {
            var defaultThread = getThread(nioEventLoopGroup);
            var driverThread = getThread(eventLoopGroup);

            assertEquals(defaultThread.getClass(), driverThread.getClass().getSuperclass());
            assertEquals(defaultThread.getPriority(), driverThread.getPriority());
        } finally {
            shutdown(nioEventLoopGroup);
        }
    }

    private static Thread getThread(EventLoopGroup eventLoopGroup) throws Exception {
        return eventLoopGroup.submit(Thread::currentThread).get(10, SECONDS);
    }

    private static void shutdown(EventLoopGroup group) {
        if (group != null) {
            try {
                group.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable ignore) {
            }
        }
    }
}
