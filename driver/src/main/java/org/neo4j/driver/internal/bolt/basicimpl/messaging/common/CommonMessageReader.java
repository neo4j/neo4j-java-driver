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
package org.neo4j.driver.internal.bolt.basicimpl.messaging.common;

import java.io.IOException;
import java.util.Map;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.bolt.api.GqlError;
import org.neo4j.driver.internal.bolt.api.GqlStatusError;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.MessageFormat;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.ValueUnpacker;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.FailureMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.RecordMessage;
import org.neo4j.driver.internal.bolt.basicimpl.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.bolt.basicimpl.packstream.PackInput;

public class CommonMessageReader implements MessageFormat.Reader {
    protected final ValueUnpacker unpacker;

    public CommonMessageReader(PackInput input, boolean dateTimeUtcEnabled) {
        this(new CommonValueUnpacker(input, dateTimeUtcEnabled));
    }

    protected CommonMessageReader(ValueUnpacker unpacker) {
        this.unpacker = unpacker;
    }

    @Override
    public void read(ResponseMessageHandler handler) throws IOException {
        unpacker.unpackStructHeader();
        var type = unpacker.unpackStructSignature();
        switch (type) {
            case SuccessMessage.SIGNATURE -> unpackSuccessMessage(handler);
            case FailureMessage.SIGNATURE -> unpackFailureMessage(handler);
            case IgnoredMessage.SIGNATURE -> unpackIgnoredMessage(handler);
            case RecordMessage.SIGNATURE -> unpackRecordMessage(handler);
            default -> throw new IOException("Unknown message type: " + type);
        }
    }

    private void unpackSuccessMessage(ResponseMessageHandler output) throws IOException {
        var map = unpacker.unpackMap();
        output.handleSuccessMessage(map);
    }

    protected void unpackFailureMessage(ResponseMessageHandler output) throws IOException {
        var params = unpacker.unpackMap();
        var code = params.get("code").asString();
        var message = params.get("message").asString();
        var diagnosticRecord = Map.ofEntries(
                Map.entry("CURRENT_SCHEMA", Values.value("/")),
                Map.entry("OPERATION", Values.value("")),
                Map.entry("OPERATION_CODE", Values.value("0")));
        var gqlError = new GqlError(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                code,
                message,
                diagnosticRecord,
                null);
        output.handleFailureMessage(gqlError);
    }

    private void unpackIgnoredMessage(ResponseMessageHandler output) {
        output.handleIgnoredMessage();
    }

    private void unpackRecordMessage(ResponseMessageHandler output) throws IOException {
        var fields = unpacker.unpackArray();
        output.handleRecordMessage(fields);
    }
}
