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
package org.neo4j.driver.exceptions;

import java.util.NoSuchElementException;

/**
 * Thrown whenever a client expected to read a record that was not available (i.e. because it wasn't returned by the server).
 *
 * This usually indicates an expectation mismatch between client code and database application logic.
 *
 * @since 1.0
 */
public class NoSuchRecordException extends NoSuchElementException
{
    private static final long serialVersionUID = 9091962868264042491L;

    public NoSuchRecordException( String message )
    {
        super( message );
    }
}
