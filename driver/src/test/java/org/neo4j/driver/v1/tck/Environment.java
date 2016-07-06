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
package org.neo4j.driver.v1.tck;

import cucumber.api.java.After;
import cucumber.api.java.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.tck.tck.util.Types;
import org.neo4j.driver.v1.tck.tck.util.runners.CypherStatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.MappedParametersRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StatementRunner;
import org.neo4j.driver.v1.tck.tck.util.runners.StringRunner;

import static org.neo4j.driver.v1.tck.DriverComplianceIT.neo4j;

public class Environment
{
    public static MappedParametersRunner mappedParametersRunner;
    public static StatementRunner statementRunner;
    public static StringRunner stringRunner;
    public static List<CypherStatementRunner> runners;
    public static Object expectedJavaValue;
    public static Value expectedBoltValue;
    public static List<Object> listOfObjects;
    public static Map<String,Object> mapOfObjects;
    public static Map<String,Types.Type> mappedTypes;

    public static Driver driver;


    @Before
    public void resetValues()
    {
        listOfObjects = new ArrayList<>();
        mapOfObjects = new HashMap<>();
        expectedJavaValue = null;
        expectedBoltValue = null;
        mappedParametersRunner = null;
        statementRunner = null;
        stringRunner = null;
        runners = new ArrayList<>();
        mappedTypes = new HashMap<>();
        driver = neo4j.driver();
    }

    @After
    public void closeRunners()
    {
        for (CypherStatementRunner runner : runners)
        {
            runner.close();
        }
    }

    @Before("@reset_database")
    public void emptyDatabase()
    {
        Driver driver = neo4j.driver();
        try ( Session session = driver.session())
        {
            session.run( "MATCH (n) DETACH DELETE n" );
        }
    }
}
