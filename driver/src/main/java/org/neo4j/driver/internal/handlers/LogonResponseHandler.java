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
package org.neo4j.driver.internal.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import java.util.Map;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.internal.spi.ResponseHandler;

public class LogonResponseHandler implements ResponseHandler {

    private final ChannelPromise connectionInitializedPromise;
    private final Channel channel;

    public LogonResponseHandler(ChannelPromise connectionInitializedPromise) {
        this.connectionInitializedPromise = connectionInitializedPromise;
        this.channel = connectionInitializedPromise.channel();
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        connectionInitializedPromise.setSuccess();
    }

    @Override
    public void onFailure(Throwable error) {
        channel.close().addListener(future -> connectionInitializedPromise.setFailure(error));
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new ProtocolException("records not supported");
    }
}
