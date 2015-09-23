/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver;

import javadoctest.DocSnippet;
import javadoctest.DocTestRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;

import org.neo4j.driver.util.TestSession;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith( DocTestRunner.class )
public class StatementRunnerDocIT
{
    @Rule
    public TestSession session = new TestSession();

    public void parameterTest( DocSnippet snippet )
    {
        // given
        snippet.set( "session", session );

        // when
        snippet.run();

        // then
        assertThat( snippet.get( "res" ), notNullValue() );
    }
}
