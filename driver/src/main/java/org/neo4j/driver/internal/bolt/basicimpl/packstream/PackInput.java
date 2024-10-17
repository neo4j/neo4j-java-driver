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
package org.neo4j.driver.internal.bolt.basicimpl.packstream;

import java.io.IOException;

/**
 * This is what {@link PackStream} uses to ingest data, implement this on top of any data source of your choice to
 * deserialize the stream with {@link PackStream}.
 */
public interface PackInput {
    /** Consume one byte */
    byte readByte() throws IOException;

    /** Consume a 2-byte signed integer */
    short readShort() throws IOException;

    /** Consume a 4-byte signed integer */
    int readInt() throws IOException;

    /** Consume an 8-byte signed integer */
    long readLong() throws IOException;

    /** Consume an 8-byte IEEE 754 "double format" floating-point number */
    double readDouble() throws IOException;

    /** Consume a specified number of bytes */
    void readBytes(byte[] into, int offset, int toRead) throws IOException;

    /** Get the next byte without forwarding the internal pointer */
    byte peekByte() throws IOException;
}
