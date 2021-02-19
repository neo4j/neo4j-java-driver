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
package org.neo4j.driver.util;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.driver.Session;
import org.neo4j.driver.async.AsyncSession;

import static org.neo4j.driver.util.TestUtil.await;

/**
 * A little utility for integration testing, this provides tests with sessions they can work with.
 * If you want more direct control, have a look at {@link DatabaseExtension} instead.
 */
public class DriverExtension extends DatabaseExtension implements BeforeEachCallback, AfterEachCallback
{
    private AsyncSession asyncSession;
    private Session session;

    public AsyncSession asyncSession()
    {
        return asyncSession;
    }

    public Session session()
    {
        return session;
    }

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        super.beforeEach( context );
        asyncSession = driver().asyncSession();
        session = driver().session();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        if ( asyncSession != null )
        {
            await( asyncSession.closeAsync() );
        }
        if ( session != null )
        {
            session.close();
        }
    }
}
