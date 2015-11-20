/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.spi;

import java.util.List;

import org.neo4j.driver.v1.Notification;
import org.neo4j.driver.v1.Plan;
import org.neo4j.driver.v1.ProfiledPlan;
import org.neo4j.driver.v1.StatementType;
import org.neo4j.driver.v1.UpdateStatistics;
import org.neo4j.driver.v1.Value;

public interface StreamCollector
{
    StreamCollector NO_OP = new StreamCollector()
    {
        @Override
        public void fieldNames( String[] names )
        {
        }

        @Override
        public void record( Value[] fields )
        {
        }

        @Override
        public void statementType( StatementType type )
        {
        }

        @Override
        public void statementStatistics( UpdateStatistics statistics )
        {
        }

        @Override
        public void plan( Plan plan )
        {
        }

        @Override
        public void profile( ProfiledPlan plan )
        {

        }

        @Override
        public void notifications( List<Notification> notifications )
        {

        }
    };

    // TODO: This should be modified to simply have head/record/tail methods

    void fieldNames( String[] names );

    void record( Value[] fields );

    void statementType( StatementType type);

    void statementStatistics( UpdateStatistics statistics );

    void plan( Plan plan );

    void profile( ProfiledPlan plan );

    void notifications( List<Notification> notifications );
}

