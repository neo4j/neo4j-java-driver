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

public class Application {

    public static void main( String[] args )
    {
        String serverAddress = System.getenv( "NATIVE_IMAGE_NEO4J_URL" );
        String serverPass = System.getenv( "NATIVE_IMAGE_NEO4J_PASS" );
        String uri = serverAddress +":7687";
        AuthToken auth = AuthTokens.basic( "neo4j", serverPass );
        Config config = Config.builder().build();

        try ( Driver driver = GraphDatabase.driver( uri, auth, config ) )
        {
            try ( Session session = driver.session() )
            {
                session.run( "RETURN 1 as i, true as b, \"string\" as s" )
                       .next()
                       .asMap()
                       .forEach( (k,v) -> System.out.println( v ) );
            }
        }
    }
}
