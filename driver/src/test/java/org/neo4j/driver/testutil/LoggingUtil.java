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
package org.neo4j.driver.testutil;

import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import java.time.LocalDateTime;
import java.util.List;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.bolt.basicimpl.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.bolt.basicimpl.async.outbound.OutboundMessageHandler;

public class LoggingUtil {
    public static Logging boltLogging(List<String> messages) {
        var logging = mock(Logging.class);
        var noopLogger = mock(Logger.class);
        var accumulatingLogger = mock(Logger.class);
        given(logging.getLog(argThat(not(InboundMessageDispatcher.class)))).willReturn(noopLogger);
        given(logging.getLog(argThat(not(OutboundMessageHandler.class)))).willReturn(noopLogger);
        given(logging.getLog(InboundMessageDispatcher.class)).willReturn(accumulatingLogger);
        given(logging.getLog(OutboundMessageHandler.class)).willReturn(accumulatingLogger);
        given(accumulatingLogger.isDebugEnabled()).willReturn(true);
        willAnswer(invocationOnMock -> {
                    var message = (String) invocationOnMock.getArgument(0);
                    if (message.contains("C: ") || message.contains("S: ")) {
                        var formattedMessage = String.format(
                                LocalDateTime.now() + " " + message,
                                invocationOnMock.getArgument(1).toString());
                        messages.add(formattedMessage);
                    }
                    return null;
                })
                .given(accumulatingLogger)
                .debug(any(String.class), any(Object.class));
        return logging;
    }
}
