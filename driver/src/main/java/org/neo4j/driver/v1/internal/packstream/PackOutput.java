/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.internal.packstream;

import java.io.IOException;

/**
 * This is where {@link PackStream} writes its output to.
 */
public interface PackOutput
{
    /** If implementation has been buffering data, it should flush those buffers now. */
    PackOutput flush() throws IOException;

    /** Produce a single byte */
    PackOutput writeByte( byte value ) throws IOException;

    /** Produce binary data */
    PackOutput writeBytes( byte[] data, int offset, int amountToWrite ) throws IOException;

    /** Produce a 4-byte signed integer */
    PackOutput writeShort( short value ) throws IOException;

    /** Produce a 4-byte signed integer */
    PackOutput writeInt( int value ) throws IOException;

    /** Produce an 8-byte signed integer */
    PackOutput writeLong( long value ) throws IOException;

    /** Produce an 8-byte IEEE 754 "double format" floating-point number */
    PackOutput writeDouble( double value ) throws IOException;
}
