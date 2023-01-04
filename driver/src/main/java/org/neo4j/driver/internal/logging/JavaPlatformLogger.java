/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.logging;

import org.neo4j.driver.Logger;

import java.lang.System.Logger.Level;
import java.util.Objects;

public class JavaPlatformLogger implements Logger {
    private final java.lang.System.Logger delegate;

    public JavaPlatformLogger(System.Logger delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void error(String message, Throwable cause) {
        if (delegate.isLoggable(Level.ERROR)) {
            delegate.log(Level.ERROR, message, cause);
        }
    }

    @Override
    public void info(String format, Object... params) {
        if (delegate.isLoggable(Level.INFO)) {
            delegate.log(Level.INFO, String.format(format, params));
        }
    }

    @Override
    public void warn(String format, Object... params) {
        if (delegate.isLoggable(Level.WARNING)) {
            delegate.log(Level.WARNING, String.format(format, params));
        }
    }

    @Override
    public void warn(String message, Throwable cause) {
        if (delegate.isLoggable(Level.WARNING)) {
            delegate.log(Level.WARNING, message, cause);
        }
    }

    @Override
    public void debug(String format, Object... params) {
        if (isDebugEnabled()) {
            delegate.log(Level.DEBUG, String.format(format, params));
        }
    }

    @Override
    public void debug(String message, Throwable throwable) {
        if (isDebugEnabled()) {
            delegate.log(Level.DEBUG, message, throwable);
        }
    }

    @Override
    public void trace(String format, Object... params) {
        if (isTraceEnabled()) {
            delegate.log(Level.TRACE, String.format(format, params));
        }
    }

    @Override
    public boolean isTraceEnabled() {
        return delegate.isLoggable(Level.TRACE);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isLoggable(Level.DEBUG);
    }
}
