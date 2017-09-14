/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.async;

import java.util.Collections;
import java.util.List;

import org.neo4j.driver.internal.handlers.PullAllResponseHandler;
import org.neo4j.driver.internal.handlers.RunResponseHandler;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Response;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.summary.ResultSummary;

public class InternalStatementResultCursor implements StatementResultCursor
{
    private final RunResponseHandler runResponseHandler;
    private final PullAllResponseHandler pullAllHandler;

    public InternalStatementResultCursor( RunResponseHandler runResponseHandler, PullAllResponseHandler pullAllHandler )
    {
        this.runResponseHandler = runResponseHandler;
        this.pullAllHandler = pullAllHandler;
    }

    @Override
    public List<String> keys()
    {
        List<String> keys = runResponseHandler.statementKeys();
        return keys == null ? Collections.<String>emptyList() : Collections.unmodifiableList( keys );
    }

    @Override
    public Response<ResultSummary> summaryAsync()
    {
        return pullAllHandler.summaryAsync().asTask();
    }

    @Override
    public Response<Boolean> fetchAsync()
    {
        return pullAllHandler.fetchRecordAsync().asTask();
    }

    @Override
    public Record current()
    {
        return pullAllHandler.currentRecord();
    }
}
