/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.driver.StatementRunner;

/**
 * A user is trying to access resources that are no longer valid due to
 * the resources have already been consumed or
 * the {@link StatementRunner} where the resources are created has already been closed.
 */
public class ResultConsumedException extends ClientException
{
    public ResultConsumedException()
    {
        super( "Cannot access records on this result any more as the result has already been consumed " +
                "or the statement runner where the result is created has already been closed." );
    }
}
