/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.TestNeo4jSession;

import static org.junit.Assert.assertTrue;

@RunWith( DocTestRunner.class )
public class ResultDocIT
{
    @Rule
    public TestNeo4jSession session = new TestNeo4jSession();

    /** @see StatementResult */
    public void summarizeUsage( DocSnippet snippet )
    {
        // Given
        snippet.set( "session", session );

        // When I run the snippet
        snippet.run();

        // Then a summary should've been created
        ResultSummary summary = snippet.get( "summary" );

        assertTrue( summary.hasPlan() );
        assertTrue( summary.hasProfile() );
    }
}
