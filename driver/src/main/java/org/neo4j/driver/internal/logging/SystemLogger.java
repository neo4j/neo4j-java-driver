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
package org.neo4j.driver.internal.logging;

import java.util.Objects;
import org.neo4j.driver.Logger;

@SuppressWarnings("deprecation")
final class SystemLogger implements Logger {
    private final System.Logger delegate;
    private final boolean isDebugEnabled;
    private final boolean isTraceEnabled;

    SystemLogger(System.Logger delegate) {
        this.delegate = Objects.requireNonNull(delegate);
        this.isDebugEnabled = delegate.isLoggable(System.Logger.Level.DEBUG);
        this.isTraceEnabled = delegate.isLoggable(System.Logger.Level.TRACE);
    }

    @Override
    public void error(String message, Throwable cause) {
        delegate.log(System.Logger.Level.ERROR, message, cause);
    }

    @Override
    public void info(String message, Object... params) {
        try {
            delegate.log(System.Logger.Level.INFO, message.formatted(params));
        } catch (RuntimeException ignored) {
        }
    }

    @Override
    public void warn(String message, Object... params) {
        try {
            delegate.log(System.Logger.Level.WARNING, message.formatted(params));
        } catch (RuntimeException ignored) {
        }
    }

    @Override
    public void warn(String message, Throwable cause) {
        delegate.log(System.Logger.Level.WARNING, message, cause);
    }

    @Override
    public void debug(String message, Object... params) {
        try {
            delegate.log(System.Logger.Level.DEBUG, message.formatted(params));
        } catch (RuntimeException ignored) {
        }
    }

    @Override
    public void debug(String message, Throwable throwable) {
        delegate.log(System.Logger.Level.DEBUG, message, throwable);
    }

    @Override
    public void trace(String message, Object... params) {
        try {
            delegate.log(System.Logger.Level.TRACE, message.formatted(params));
        } catch (RuntimeException ignored) {
        }
    }

    @Override
    public boolean isTraceEnabled() {
        return isTraceEnabled;
    }

    @Override
    public boolean isDebugEnabled() {
        return isDebugEnabled;
    }
}
