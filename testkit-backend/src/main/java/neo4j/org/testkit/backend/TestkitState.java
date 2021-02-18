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
package neo4j.org.testkit.backend;

import lombok.Getter;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;

@Getter
public class TestkitState
{
    private final Map<String,Driver> drivers = new HashMap<>();
    private final Map<String,SessionState> sessionStates = new HashMap<>();
    private final Map<String,Result> results = new HashMap<>();
    private final Map<String,Transaction> transactions = new HashMap<>();
    private final Map<String,Neo4jException> errors = new HashMap<>();
    private int idGenerator = 0;
    private final Consumer<TestkitResponse> responseWriter;
    private final Supplier<Boolean> processor;

    public TestkitState( Consumer<TestkitResponse> responseWriter, Supplier<Boolean> processor )
    {
        this.responseWriter = responseWriter;
        this.processor = processor;
    }

    public String newId()
    {
        return String.valueOf( idGenerator++ );
    }
}
