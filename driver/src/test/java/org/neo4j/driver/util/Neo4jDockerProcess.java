/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;

public class Neo4jDockerProcess extends DockerProcess
{
    public Neo4jDockerProcess( String neo4jVersion, String user, String password, int boltPort, int httpPort,
            File logFolder, File certFolder, File importFolder, File pluginFolder )
    {
        super( String.format( "bolt server -v -i %s -a %s:%s -B %s -H %s", neo4jVersion, user, password, boltPort, httpPort ),
                logFolder, certFolder, importFolder, pluginFolder );
    }
}
