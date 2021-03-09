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
package org.neo4j.driver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

public class NativeApplicationIT
{

    @Test
    public void f() throws IOException
    {
        List<String> values = new ArrayList<>();
        values.add( "1" );
        values.add( "true" );
        values.add( "string" );

        Process p = new ProcessBuilder( Paths.get( ".", "target", "application" ).toAbsolutePath().normalize().toString() )
                .start();

        try ( BufferedReader in = new BufferedReader( new InputStreamReader( p.getInputStream() ) ) )
        {
            Set<String> generatedValues = in.lines().collect( Collectors.toSet() );

            System.out.println( "Native Program Output:" );
            System.out.println( generatedValues );

            Assertions.assertTrue( generatedValues.containsAll( values ) );
        } catch ( Throwable t )
        {
            //dump log output for debugging
            try ( BufferedReader in = new BufferedReader( new InputStreamReader( p.getErrorStream() ) ) )
            {
                Set<String> errorOutput = in.lines().collect( Collectors.toSet() );

                System.out.println( "Native Program Error Output:" );
                errorOutput.forEach( System.out::println );
                fail();
            }
        }
    }
}
