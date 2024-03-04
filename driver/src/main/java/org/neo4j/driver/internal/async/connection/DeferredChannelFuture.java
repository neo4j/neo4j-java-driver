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
package org.neo4j.driver.internal.async.connection;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.internal.util.Futures;

/**
 * This implementation is for {@link io.netty.channel.pool.SimpleChannelPool} usage only. It defers listener
 * notification until the supplied {@link CompletionStage} completes.
 */
class DeferredChannelFuture implements ChannelFuture {
    private final Logger logger;

    @SuppressWarnings("rawtypes")
    private final CompletableFuture<GenericFutureListener> listenerFuture = new CompletableFuture<>();

    @SuppressWarnings("unchecked")
    public DeferredChannelFuture(CompletionStage<ChannelFuture> channelFutureCompletionStage, Logging logging) {
        this.logger = logging.getLog(getClass());

        listenerFuture.thenCompose(ignored -> channelFutureCompletionStage).whenComplete((channelFuture, throwable) -> {
            var listener = listenerFuture.join();
            if (throwable != null) {
                throwable = Futures.completionExceptionCause(throwable);
                try {
                    listener.operationComplete(new FailedChannelFuture(throwable));
                } catch (Throwable e) {
                    logger.error("An error occured while notifying listener.", e);
                }
            } else {
                channelFuture.addListener(listener);
            }
        });
    }

    @Override
    public Channel channel() {
        return null;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public boolean isCancellable() {
        return false;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        listenerFuture.complete(listener);
        return this;
    }

    @SafeVarargs
    @Override
    public final ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return null;
    }

    @Override
    public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
        return null;
    }

    @SafeVarargs
    @Override
    public final ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return null;
    }

    @Override
    public ChannelFuture sync() {
        return null;
    }

    @Override
    public ChannelFuture syncUninterruptibly() {
        return null;
    }

    @Override
    public ChannelFuture await() {
        return null;
    }

    @Override
    public ChannelFuture awaitUninterruptibly() {
        return null;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public boolean await(long timeoutMillis) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long timeoutMillis) {
        return false;
    }

    @Override
    public Void getNow() {
        return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public Void get() {
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public boolean isVoid() {
        return false;
    }

    private record FailedChannelFuture(Throwable throwable) implements ChannelFuture {

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public boolean isCancellable() {
            return false;
        }

        @Override
        public Throwable cause() {
            return throwable;
        }

        @Override
        public ChannelFuture addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
        }

        @SafeVarargs
        @Override
        public final ChannelFuture addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
        }

        @Override
        public ChannelFuture removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
        }

        @SafeVarargs
        @Override
        public final ChannelFuture removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
        }

        @Override
        public ChannelFuture sync() {
            return null;
        }

        @Override
        public ChannelFuture syncUninterruptibly() {
            return null;
        }

        @Override
        public ChannelFuture await() {
            return null;
        }

        @Override
        public ChannelFuture awaitUninterruptibly() {
            return null;
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public boolean await(long timeoutMillis) {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public boolean awaitUninterruptibly(long timeoutMillis) {
            return false;
        }

        @Override
        public Void getNow() {
            return null;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }

        @Override
        public boolean isVoid() {
            return false;
        }
    }
}
