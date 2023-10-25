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

import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;
import java.util.Map;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.ResponseHandler;

public class PingResponseHandler implements ResponseHandler {
    private final Promise<Boolean> result;
    private final Channel channel;
    private final Logger log;

    public PingResponseHandler(Promise<Boolean> result, Channel channel, Logging logging) {
        this.result = result;
        this.channel = channel;
        this.log = logging.getLog(getClass());
    }

    @Override
    public void onSuccess(Map<String, Value> metadata) {
        log.trace("Channel %s pinged successfully", channel);
        result.setSuccess(true);
    }

    @Override
    public void onFailure(Throwable error) {
        log.trace("Channel %s failed ping %s", channel, error);
        result.setSuccess(false);
    }

    @Override
    public void onRecord(Value[] fields) {
        throw new UnsupportedOperationException();
    }
}
