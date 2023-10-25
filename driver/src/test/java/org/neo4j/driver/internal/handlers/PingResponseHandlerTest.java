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
package org.neo4j.driver.internal.handlers;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;

import io.netty.channel.Channel;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Value;

class PingResponseHandlerTest {
    @Test
    void shouldResolvePromiseOnSuccess() {
        var promise = newPromise();
        var handler = newHandler(promise);

        handler.onSuccess(emptyMap());

        assertTrue(promise.isSuccess());
        assertTrue(promise.getNow());
    }

    @Test
    void shouldResolvePromiseOnFailure() {
        var promise = newPromise();
        var handler = newHandler(promise);

        handler.onFailure(new RuntimeException());

        assertTrue(promise.isSuccess());
        assertFalse(promise.getNow());
    }

    @Test
    void shouldNotSupportRecordMessages() {
        var handler = newHandler(newPromise());

        assertThrows(UnsupportedOperationException.class, () -> handler.onRecord(new Value[0]));
    }

    private static Promise<Boolean> newPromise() {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }

    private static PingResponseHandler newHandler(Promise<Boolean> result) {
        return new PingResponseHandler(result, mock(Channel.class), DEV_NULL_LOGGING);
    }
}
