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
package org.neo4j.driver.internal.bolt.basicimpl.messaging;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractMessageWriter implements MessageFormat.Writer {
    private final ValuePacker packer;
    private final Map<Byte, MessageEncoder> encodersByMessageSignature;

    protected AbstractMessageWriter(ValuePacker packer, Map<Byte, MessageEncoder> encodersByMessageSignature) {
        this.packer = requireNonNull(packer);
        this.encodersByMessageSignature = requireNonNull(encodersByMessageSignature);
    }

    @Override
    public final void write(Message msg) throws IOException {
        var signature = msg.signature();
        var encoder = encodersByMessageSignature.get(signature);
        if (encoder == null) {
            throw new IOException("No encoder found for message " + msg + " with signature " + signature);
        }
        encoder.encode(msg, packer);
    }
}
