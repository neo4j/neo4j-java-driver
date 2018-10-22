/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.v1.tck;

import cucumber.api.CucumberOptions;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.file.Files;

import org.neo4j.driver.v1.tck.tck.util.DatabaseRule;
import org.neo4j.driver.v1.util.ParallelizableJUnit4IT;

/**
 * The base class to run all cucumber tests
 */
@RunWith( DriverCucumberAdapter.class )
@CucumberOptions( features = {"target/resources/features"}, strict=true, tags={"~@db", "~@fixed_session_pool"},
        format = {"default_summary"})
@Category( ParallelizableJUnit4IT.class )
public class DriverComplianceIT
{
    @ClassRule
    public static final DatabaseRule neo4j = new DatabaseRule();

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        neo4j.stopDb();
        Files.deleteIfExists( neo4j.tlsCertFile().toPath() );
        Files.deleteIfExists( neo4j.tlsKeyFile().toPath() );
    }
}
