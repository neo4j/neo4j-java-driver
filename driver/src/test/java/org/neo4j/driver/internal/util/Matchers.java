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
package org.neo4j.driver.internal.util;

import java.util.concurrent.TimeUnit;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.ResultSummary;

public final class Matchers {
    private Matchers() {}

    public static Matcher<ResultSummary> containsResultAvailableAfterAndResultConsumedAfter() {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(ResultSummary summary) {
                return summary.resultAvailableAfter(TimeUnit.MILLISECONDS) >= 0L
                        && summary.resultConsumedAfter(TimeUnit.MILLISECONDS) >= 0L;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("resultAvailableAfter and resultConsumedAfter ");
            }
        };
    }

    public static Matcher<Throwable> arithmeticError() {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Throwable error) {
                return error instanceof ClientException
                        && ((ClientException) error).code().contains("ArithmeticError");
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("client error with code 'ArithmeticError' ");
            }
        };
    }

    public static Matcher<Throwable> syntaxError() {
        return syntaxError(null);
    }

    public static Matcher<Throwable> syntaxError(String messagePrefix) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Throwable error) {
                if (error instanceof ClientException clientError) {
                    return clientError.code().contains("SyntaxError")
                            && (messagePrefix == null
                                    || clientError.getMessage().startsWith(messagePrefix));
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("client error with code 'SyntaxError' and prefix '" + messagePrefix + "' ");
            }
        };
    }

    public static Matcher<Throwable> connectionAcquisitionTimeoutError(int timeoutMillis) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Throwable error) {
                if (error instanceof ClientException) {
                    var expectedMessage = "Unable to acquire connection from the pool within "
                            + "configured maximum time of " + timeoutMillis + "ms";
                    return expectedMessage.equals(error.getMessage());
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("acquisition timeout error with " + timeoutMillis + "ms");
            }
        };
    }

    public static Matcher<Throwable> blockingOperationInEventLoopError() {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Throwable error) {
                return error instanceof IllegalStateException
                        && error.getMessage() != null
                        && error.getMessage().startsWith("Blocking operation can't be executed in IO thread");
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("IllegalStateException about blocking operation in event loop thread ");
            }
        };
    }
}
