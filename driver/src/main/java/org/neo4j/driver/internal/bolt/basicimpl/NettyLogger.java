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

import static java.lang.String.format;

import io.netty.util.internal.logging.AbstractInternalLogger;
import java.io.Serial;
import java.util.regex.Pattern;

public class NettyLogger extends AbstractInternalLogger {
    @Serial
    private static final long serialVersionUID = -1466889786216191159L;

    private final System.Logger log;
    private static final Pattern PLACE_HOLDER_PATTERN = Pattern.compile("\\{}");

    public NettyLogger(String name, System.Logger log) {
        super(name);
        this.log = log;
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isLoggable(System.Logger.Level.TRACE);
    }

    @Override
    public void trace(String msg) {
        log.log(System.Logger.Level.TRACE, msg);
    }

    @Override
    public void trace(String format, Object arg) {
        log.log(System.Logger.Level.TRACE, toDriverLoggerFormat(format), arg);
    }

    @Override
    public void trace(String format, Object argA, Object argB) {
        log.log(System.Logger.Level.TRACE, toDriverLoggerFormat(format), argA, argB);
    }

    @Override
    public void trace(String format, Object... arguments) {
        log.log(System.Logger.Level.TRACE, toDriverLoggerFormat(format), arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        log.log(System.Logger.Level.TRACE, "%s%n%s", msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isLoggable(System.Logger.Level.DEBUG);
    }

    @Override
    public void debug(String msg) {
        log.log(System.Logger.Level.DEBUG, msg);
    }

    @Override
    public void debug(String format, Object arg) {
        log.log(System.Logger.Level.DEBUG, toDriverLoggerFormat(format), arg);
    }

    @Override
    public void debug(String format, Object argA, Object argB) {
        log.log(System.Logger.Level.DEBUG, toDriverLoggerFormat(format), argA, argB);
    }

    @Override
    public void debug(String format, Object... arguments) {
        log.log(System.Logger.Level.DEBUG, toDriverLoggerFormat(format), arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        log.log(System.Logger.Level.DEBUG, "%s%n%s", msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public void info(String msg) {
        log.log(System.Logger.Level.INFO, msg);
    }

    @Override
    public void info(String format, Object arg) {
        log.log(System.Logger.Level.INFO, toDriverLoggerFormat(format), arg);
    }

    @Override
    public void info(String format, Object argA, Object argB) {
        log.log(System.Logger.Level.INFO, toDriverLoggerFormat(format), argA, argB);
    }

    @Override
    public void info(String format, Object... arguments) {
        log.log(System.Logger.Level.INFO, toDriverLoggerFormat(format), arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        log.log(System.Logger.Level.INFO, "%s%n%s", msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public void warn(String msg) {
        log.log(System.Logger.Level.WARNING, msg);
    }

    @Override
    public void warn(String format, Object arg) {
        log.log(System.Logger.Level.WARNING, toDriverLoggerFormat(format), arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        log.log(System.Logger.Level.WARNING, toDriverLoggerFormat(format), arguments);
    }

    @Override
    public void warn(String format, Object argA, Object argB) {
        log.log(System.Logger.Level.WARNING, toDriverLoggerFormat(format), argA, argB);
    }

    @Override
    public void warn(String msg, Throwable t) {
        log.log(System.Logger.Level.WARNING, "%s%n%s", msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public void error(String msg) {
        log.log(System.Logger.Level.ERROR, msg, (Throwable) null);
    }

    @Override
    public void error(String format, Object arg) {
        error(format, new Object[] {arg});
    }

    @Override
    public void error(String format, Object argA, Object argB) {
        error(format, new Object[] {argA, argB});
    }

    @Override
    public void error(String format, Object... arguments) {
        format = toDriverLoggerFormat(format);
        if (arguments.length == 0) {
            log.log(System.Logger.Level.ERROR, format, (Throwable) null);
            return;
        }

        var arg = arguments[arguments.length - 1];
        if (arg instanceof Throwable) {
            // still give all arguments to string format,
            // for the worst case, the redundant parameter will be ignored.
            log.log(System.Logger.Level.ERROR, format(format, arguments), (Throwable) arg);
        }
    }

    @Override
    public void error(String msg, Throwable t) {
        log.log(System.Logger.Level.ERROR, msg, t);
    }

    private String toDriverLoggerFormat(String format) {
        return PLACE_HOLDER_PATTERN.matcher(format).replaceAll("%s");
    }
}
