/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.driver.internal.messaging;

import org.junit.Test;

import org.neo4j.driver.internal.packstream.PackOutput;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PackStreamMessageFormatV2Test
{
    private final PackStreamMessageFormatV2 messageFormat = new PackStreamMessageFormatV2();

    @Test
    public void shouldFailToCreateWriterWithoutByteArraySupport()
    {
        PackOutput output = mock( PackOutput.class );

        try
        {
            messageFormat.newWriter( output, false );
            fail( "Exception expected" );
        }
        catch ( IllegalArgumentException ignore )
        {
        }
    }
}
