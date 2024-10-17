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
package org.neo4j.driver.internal.bolt.basicimpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;

class NettyLoggerTest {
    NettyLogger log;

    @Mock
    System.Logger upstreamLog;

    @SuppressWarnings("resource")
    @BeforeEach
    void beforeEach() {
        openMocks(this);
        log = new NettyLogger("name", upstreamLog);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldDelegateIsTraceEnabled(boolean enabled) {
        given(upstreamLog.isLoggable(System.Logger.Level.TRACE)).willReturn(enabled);

        var traceEnabled = log.isTraceEnabled();

        assertEquals(enabled, traceEnabled);
        then(upstreamLog).should().isLoggable(System.Logger.Level.TRACE);
    }

    @Test
    void shouldDelegateTrace() {
        var message = "message";

        log.trace(message);

        then(upstreamLog).should().log(System.Logger.Level.TRACE, message);
    }

    @Test
    void shouldDelegateTraceWithObject() {
        var format = "message";
        var object = new Object();

        log.trace(format, object);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.TRACE,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object);
    }

    @Test
    void shouldDelegateTraceWithTwoObjects() {
        var format = "message";
        var object1 = new Object();
        var object2 = new Object();

        log.trace(format, object1, object2);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.TRACE,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object1,
                        object2);
    }

    @Test
    void shouldDelegateTraceWithObjects() {
        var format = "message";
        var objects = new Object[0];

        log.trace(format, objects);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.TRACE,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        objects);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldDelegateIsDebugEnabled(boolean enabled) {
        given(upstreamLog.isLoggable(System.Logger.Level.DEBUG)).willReturn(enabled);

        var traceEnabled = log.isDebugEnabled();

        assertEquals(enabled, traceEnabled);
        then(upstreamLog).should().isLoggable(System.Logger.Level.DEBUG);
    }

    @Test
    void shouldDelegateDebug() {
        var message = "message";

        log.debug(message);

        then(upstreamLog).should().log(System.Logger.Level.DEBUG, message);
    }

    @Test
    void shouldDelegateDebugWithObject() {
        var format = "message";
        var object = new Object();

        log.debug(format, object);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.DEBUG,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object);
    }

    @Test
    void shouldDelegateDebugWithTwoObjects() {
        var format = "message";
        var object1 = new Object();
        var object2 = new Object();

        log.debug(format, object1, object2);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.DEBUG,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object1,
                        object2);
    }

    @Test
    void shouldDelegateDebugWithObjects() {
        var format = "message";
        var objects = new Object[0];

        log.debug(format, objects);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.DEBUG,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        objects);
    }

    @Test
    void shouldDelegateDebugWithThrowable() {
        var message = "message";
        var exception = new Exception();

        log.debug(message, exception);

        then(upstreamLog).should().log(System.Logger.Level.DEBUG, "%s%n%s", message, exception);
    }

    @Test
    void shouldDelegateInfo() {
        var message = "message";

        log.info(message);

        then(upstreamLog).should().log(System.Logger.Level.INFO, message);
    }

    @Test
    void shouldDelegateInfoWithObject() {
        var format = "message";
        var object = new Object();

        log.info(format, object);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.INFO,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object);
    }

    @Test
    void shouldDelegateInfoWithTwoObjects() {
        var format = "message";
        var object1 = new Object();
        var object2 = new Object();

        log.info(format, object1, object2);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.INFO,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object1,
                        object2);
    }

    @Test
    void shouldDelegateInfoWithObjects() {
        var format = "message";
        var objects = new Object[0];

        log.info(format, objects);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.INFO,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        objects);
    }

    @Test
    void shouldDelegateInfoWithThrowable() {
        var message = "message";
        var exception = new Exception();

        log.info(message, exception);

        then(upstreamLog).should().log(System.Logger.Level.INFO, "%s%n%s", message, exception);
    }

    @Test
    void shouldDelegateWarn() {
        var message = "message";

        log.warn(message);

        then(upstreamLog).should().log(System.Logger.Level.WARNING, message);
    }

    @Test
    void shouldDelegateWarnWithObject() {
        var format = "message";
        var object = new Object();

        log.warn(format, object);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.WARNING,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object);
    }

    @Test
    void shouldDelegateWarnWithTwoObjects() {
        var format = "message";
        var object1 = new Object();
        var object2 = new Object();

        log.warn(format, object1, object2);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.WARNING,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        object1,
                        object2);
    }

    @Test
    void shouldDelegateWarnWithObjects() {
        var format = "message";
        var objects = new Object[0];

        log.warn(format, objects);

        then(upstreamLog)
                .should()
                .log(
                        System.Logger.Level.WARNING,
                        Pattern.compile("\\{}").matcher(format).replaceAll("%s"),
                        objects);
    }

    @Test
    void shouldDelegateWarnWithThrowable() {
        var message = "message";
        var exception = new Exception();

        log.warn(message, exception);

        then(upstreamLog).should().log(System.Logger.Level.WARNING, "%s%n%s", message, exception);
    }

    @Test
    void shouldDelegateError() {
        var message = "message";

        log.error(message);

        then(upstreamLog).should().log(System.Logger.Level.ERROR, message, (Throwable) null);
    }
}
