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
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.driver.v1.Values.parameters;

@RunWith( DocTestRunner.class )
public class ValueDocIT
{
    private final Value exampleValue = Values.value(
            parameters( "users", asList( parameters( "name", "Anders" ), parameters( "name", "John" ) ) ));

    public void classDocTreeExample( DocSnippet snippet )
    {
        // given
        snippet.set( "value", exampleValue );

        // when
        snippet.run();

        // then
        assertThat( snippet.get("username"), equalTo( (Object)"John" ));
    }

    public void classDocIterationExample( DocSnippet snippet )
    {
        // given
        snippet.addImport( LinkedList.class );
        snippet.addImport( List.class );
        snippet.set( "value", exampleValue );

        // when
        snippet.run();

        // then
        assertThat( snippet.get("names"), equalTo( (Object)asList("Anders","John") ));
    }
}
