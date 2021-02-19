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
package org.neo4j.driver.integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.util.DatabaseExtension;
import org.neo4j.driver.util.Neo4jSettings;
import org.neo4j.driver.util.ParallelizableIT;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.driver.Values.parameters;

@ParallelizableIT
class LoadCSVIT
{
    @RegisterExtension
    static final DatabaseExtension neo4j = new DatabaseExtension( Neo4jSettings.TEST_SETTINGS.without( Neo4jSettings.IMPORT_DIR ) );

    @Test
    void shouldLoadCSV() throws Throwable
    {
        try( Driver driver =  GraphDatabase.driver( neo4j.uri(), neo4j.authToken() );
             Session session = driver.session() )
        {
            String csvFileUrl = createLocalIrisData( session );

            // When
            Result result = session.run(
                    "USING PERIODIC COMMIT 40\n" +
                    "LOAD CSV WITH HEADERS FROM $csvFileUrl AS l\n" +
                    "MATCH (c:Class {name: l.class_name})\n" +
                    "CREATE (s:Sample {sepal_length: l.sepal_length, sepal_width: l.sepal_width, petal_length: l.petal_length, petal_width: l.petal_width})\n" +

                    "CREATE (c)<-[:HAS_CLASS]-(s) " +
                    "RETURN count(*) AS c",
                    parameters( "csvFileUrl", csvFileUrl ) );

            // Then
            assertThat( result.next().get( "c" ).asInt(), equalTo( 150 ) );
            assertFalse( result.hasNext() );
        }
    }

    private String createLocalIrisData( Session session ) throws IOException
    {
        for ( String className : IRIS_CLASS_NAMES )
        {
            session.run( "CREATE (c:Class {name: $className}) RETURN c", parameters( "className", className ) );
        }

        return neo4j.putTmpFile( "iris", ".csv", IRIS_DATA ).toExternalForm();
    }

    private static String[] IRIS_CLASS_NAMES =
            new String[] {
                    "Iris-setosa",
                    "Iris-versicolor",
                    "Iris-virginica"
            };

    private static String IRIS_DATA =
            "sepal_length,sepal_width,petal_length,petal_width,class_name\n" +
            "5.1,3.5,1.4,0.2,Iris-setosa\n" +
            "4.9,3.0,1.4,0.2,Iris-setosa\n" +
            "4.7,3.2,1.3,0.2,Iris-setosa\n" +
            "4.6,3.1,1.5,0.2,Iris-setosa\n" +
            "5.0,3.6,1.4,0.2,Iris-setosa\n" +
            "5.4,3.9,1.7,0.4,Iris-setosa\n" +
            "4.6,3.4,1.4,0.3,Iris-setosa\n" +
            "5.0,3.4,1.5,0.2,Iris-setosa\n" +
            "4.4,2.9,1.4,0.2,Iris-setosa\n" +
            "4.9,3.1,1.5,0.1,Iris-setosa\n" +
            "5.4,3.7,1.5,0.2,Iris-setosa\n" +
            "4.8,3.4,1.6,0.2,Iris-setosa\n" +
            "4.8,3.0,1.4,0.1,Iris-setosa\n" +
            "4.3,3.0,1.1,0.1,Iris-setosa\n" +
            "5.8,4.0,1.2,0.2,Iris-setosa\n" +
            "5.7,4.4,1.5,0.4,Iris-setosa\n" +
            "5.4,3.9,1.3,0.4,Iris-setosa\n" +
            "5.1,3.5,1.4,0.3,Iris-setosa\n" +
            "5.7,3.8,1.7,0.3,Iris-setosa\n" +
            "5.1,3.8,1.5,0.3,Iris-setosa\n" +
            "5.4,3.4,1.7,0.2,Iris-setosa\n" +
            "5.1,3.7,1.5,0.4,Iris-setosa\n" +
            "4.6,3.6,1.0,0.2,Iris-setosa\n" +
            "5.1,3.3,1.7,0.5,Iris-setosa\n" +
            "4.8,3.4,1.9,0.2,Iris-setosa\n" +
            "5.0,3.0,1.6,0.2,Iris-setosa\n" +
            "5.0,3.4,1.6,0.4,Iris-setosa\n" +
            "5.2,3.5,1.5,0.2,Iris-setosa\n" +
            "5.2,3.4,1.4,0.2,Iris-setosa\n" +
            "4.7,3.2,1.6,0.2,Iris-setosa\n" +
            "4.8,3.1,1.6,0.2,Iris-setosa\n" +
            "5.4,3.4,1.5,0.4,Iris-setosa\n" +
            "5.2,4.1,1.5,0.1,Iris-setosa\n" +
            "5.5,4.2,1.4,0.2,Iris-setosa\n" +
            "4.9,3.1,1.5,0.2,Iris-setosa\n" +
            "5.0,3.2,1.2,0.2,Iris-setosa\n" +
            "5.5,3.5,1.3,0.2,Iris-setosa\n" +
            "4.9,3.6,1.4,0.1,Iris-setosa\n" +
            "4.4,3.0,1.3,0.2,Iris-setosa\n" +
            "5.1,3.4,1.5,0.2,Iris-setosa\n" +
            "5.0,3.5,1.3,0.3,Iris-setosa\n" +
            "4.5,2.3,1.3,0.3,Iris-setosa\n" +
            "4.4,3.2,1.3,0.2,Iris-setosa\n" +
            "5.0,3.5,1.6,0.6,Iris-setosa\n" +
            "5.1,3.8,1.9,0.4,Iris-setosa\n" +
            "4.8,3.0,1.4,0.3,Iris-setosa\n" +
            "5.1,3.8,1.6,0.2,Iris-setosa\n" +
            "4.6,3.2,1.4,0.2,Iris-setosa\n" +
            "5.3,3.7,1.5,0.2,Iris-setosa\n" +
            "5.0,3.3,1.4,0.2,Iris-setosa\n" +
            "7.0,3.2,4.7,1.4,Iris-versicolor\n" +
            "6.4,3.2,4.5,1.5,Iris-versicolor\n" +
            "6.9,3.1,4.9,1.5,Iris-versicolor\n" +
            "5.5,2.3,4.0,1.3,Iris-versicolor\n" +
            "6.5,2.8,4.6,1.5,Iris-versicolor\n" +
            "5.7,2.8,4.5,1.3,Iris-versicolor\n" +
            "6.3,3.3,4.7,1.6,Iris-versicolor\n" +
            "4.9,2.4,3.3,1.0,Iris-versicolor\n" +
            "6.6,2.9,4.6,1.3,Iris-versicolor\n" +
            "5.2,2.7,3.9,1.4,Iris-versicolor\n" +
            "5.0,2.0,3.5,1.0,Iris-versicolor\n" +
            "5.9,3.0,4.2,1.5,Iris-versicolor\n" +
            "6.0,2.2,4.0,1.0,Iris-versicolor\n" +
            "6.1,2.9,4.7,1.4,Iris-versicolor\n" +
            "5.6,2.9,3.6,1.3,Iris-versicolor\n" +
            "6.7,3.1,4.4,1.4,Iris-versicolor\n" +
            "5.6,3.0,4.5,1.5,Iris-versicolor\n" +
            "5.8,2.7,4.1,1.0,Iris-versicolor\n" +
            "6.2,2.2,4.5,1.5,Iris-versicolor\n" +
            "5.6,2.5,3.9,1.1,Iris-versicolor\n" +
            "5.9,3.2,4.8,1.8,Iris-versicolor\n" +
            "6.1,2.8,4.0,1.3,Iris-versicolor\n" +
            "6.3,2.5,4.9,1.5,Iris-versicolor\n" +
            "6.1,2.8,4.7,1.2,Iris-versicolor\n" +
            "6.4,2.9,4.3,1.3,Iris-versicolor\n" +
            "6.6,3.0,4.4,1.4,Iris-versicolor\n" +
            "6.8,2.8,4.8,1.4,Iris-versicolor\n" +
            "6.7,3.0,5.0,1.7,Iris-versicolor\n" +
            "6.0,2.9,4.5,1.5,Iris-versicolor\n" +
            "5.7,2.6,3.5,1.0,Iris-versicolor\n" +
            "5.5,2.4,3.8,1.1,Iris-versicolor\n" +
            "5.5,2.4,3.7,1.0,Iris-versicolor\n" +
            "5.8,2.7,3.9,1.2,Iris-versicolor\n" +
            "6.0,2.7,5.1,1.6,Iris-versicolor\n" +
            "5.4,3.0,4.5,1.5,Iris-versicolor\n" +
            "6.0,3.4,4.5,1.6,Iris-versicolor\n" +
            "6.7,3.1,4.7,1.5,Iris-versicolor\n" +
            "6.3,2.3,4.4,1.3,Iris-versicolor\n" +
            "5.6,3.0,4.1,1.3,Iris-versicolor\n" +
            "5.5,2.5,4.0,1.3,Iris-versicolor\n" +
            "5.5,2.6,4.4,1.2,Iris-versicolor\n" +
            "6.1,3.0,4.6,1.4,Iris-versicolor\n" +
            "5.8,2.6,4.0,1.2,Iris-versicolor\n" +
            "5.0,2.3,3.3,1.0,Iris-versicolor\n" +
            "5.6,2.7,4.2,1.3,Iris-versicolor\n" +
            "5.7,3.0,4.2,1.2,Iris-versicolor\n" +
            "5.7,2.9,4.2,1.3,Iris-versicolor\n" +
            "6.2,2.9,4.3,1.3,Iris-versicolor\n" +
            "5.1,2.5,3.0,1.1,Iris-versicolor\n" +
            "5.7,2.8,4.1,1.3,Iris-versicolor\n" +
            "6.3,3.3,6.0,2.5,Iris-virginica\n" +
            "5.8,2.7,5.1,1.9,Iris-virginica\n" +
            "7.1,3.0,5.9,2.1,Iris-virginica\n" +
            "6.3,2.9,5.6,1.8,Iris-virginica\n" +
            "6.5,3.0,5.8,2.2,Iris-virginica\n" +
            "7.6,3.0,6.6,2.1,Iris-virginica\n" +
            "4.9,2.5,4.5,1.7,Iris-virginica\n" +
            "7.3,2.9,6.3,1.8,Iris-virginica\n" +
            "6.7,2.5,5.8,1.8,Iris-virginica\n" +
            "7.2,3.6,6.1,2.5,Iris-virginica\n" +
            "6.5,3.2,5.1,2.0,Iris-virginica\n" +
            "6.4,2.7,5.3,1.9,Iris-virginica\n" +
            "6.8,3.0,5.5,2.1,Iris-virginica\n" +
            "5.7,2.5,5.0,2.0,Iris-virginica\n" +
            "5.8,2.8,5.1,2.4,Iris-virginica\n" +
            "6.4,3.2,5.3,2.3,Iris-virginica\n" +
            "6.5,3.0,5.5,1.8,Iris-virginica\n" +
            "7.7,3.8,6.7,2.2,Iris-virginica\n" +
            "7.7,2.6,6.9,2.3,Iris-virginica\n" +
            "6.0,2.2,5.0,1.5,Iris-virginica\n" +
            "6.9,3.2,5.7,2.3,Iris-virginica\n" +
            "5.6,2.8,4.9,2.0,Iris-virginica\n" +
            "7.7,2.8,6.7,2.0,Iris-virginica\n" +
            "6.3,2.7,4.9,1.8,Iris-virginica\n" +
            "6.7,3.3,5.7,2.1,Iris-virginica\n" +
            "7.2,3.2,6.0,1.8,Iris-virginica\n" +
            "6.2,2.8,4.8,1.8,Iris-virginica\n" +
            "6.1,3.0,4.9,1.8,Iris-virginica\n" +
            "6.4,2.8,5.6,2.1,Iris-virginica\n" +
            "7.2,3.0,5.8,1.6,Iris-virginica\n" +
            "7.4,2.8,6.1,1.9,Iris-virginica\n" +
            "7.9,3.8,6.4,2.0,Iris-virginica\n" +
            "6.4,2.8,5.6,2.2,Iris-virginica\n" +
            "6.3,2.8,5.1,1.5,Iris-virginica\n" +
            "6.1,2.6,5.6,1.4,Iris-virginica\n" +
            "7.7,3.0,6.1,2.3,Iris-virginica\n" +
            "6.3,3.4,5.6,2.4,Iris-virginica\n" +
            "6.4,3.1,5.5,1.8,Iris-virginica\n" +
            "6.0,3.0,4.8,1.8,Iris-virginica\n" +
            "6.9,3.1,5.4,2.1,Iris-virginica\n" +
            "6.7,3.1,5.6,2.4,Iris-virginica\n" +
            "6.9,3.1,5.1,2.3,Iris-virginica\n" +
            "5.8,2.7,5.1,1.9,Iris-virginica\n" +
            "6.8,3.2,5.9,2.3,Iris-virginica\n" +
            "6.7,3.3,5.7,2.5,Iris-virginica\n" +
            "6.7,3.0,5.2,2.3,Iris-virginica\n" +
            "6.3,2.5,5.0,1.9,Iris-virginica\n" +
            "6.5,3.0,5.2,2.0,Iris-virginica\n" +
            "6.2,3.4,5.4,2.3,Iris-virginica\n" +
            "5.9,3.0,5.1,1.8,Iris-virginica\n" +
            "\n";
}
