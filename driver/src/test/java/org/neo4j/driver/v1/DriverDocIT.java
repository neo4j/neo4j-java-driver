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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.driver.v1.util.StubServer;

import static org.hamcrest.core.IsEqual.equalTo;

@RunWith( DocTestRunner.class )
public class DriverDocIT
{
    @Ignore
    /** @see Driver */
    @SuppressWarnings("unchecked")
    public void exampleUsage( DocSnippet snippet ) throws IOException, InterruptedException, StubServer.ForceKilled
    {
        // given
        StubServer server = StubServer.start( "driver_snippet.script", 7687 );
        snippet.addImport( List.class );
        snippet.addImport( LinkedList.class );

        // when
        snippet.run();

        // then
        Assert.assertThat( server.exitStatus(), equalTo( 0 ) );
    }
}
