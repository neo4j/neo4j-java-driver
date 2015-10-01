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
package org.neo4j.driver.tck;


import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;

import org.neo4j.driver.util.FileTools;

@RunWith(TCKComplianceIT.Runner.class)
@CucumberOptions(plugin = {"pretty"}, features = "target/tck")
public class TCKComplianceIT
{
    /**
     * This is a workaround to allow downloading the TCK features before the cumber runner goes looking for the
     * features. It will download the driver TCK and extract it into the feature directory specified by the test using
     * this runner via {@link cucumber.api.CucumberOptions#features()}.
     */
    public static class Runner extends Cucumber
    {
        public Runner( Class clazz ) throws InitializationError, IOException
        {
            super( downloadTCKFor( clazz ) );
        }

        private static Class downloadTCKFor( Class clazz ) throws IOException
        {
            CucumberOptions ann = (CucumberOptions) clazz.getAnnotation( CucumberOptions.class );


            String featureDirectoryName = ann.features()[0];
            File featureDir = new File( featureDirectoryName );

            if( !featureDir.exists())
            {
                File workDir = featureDir.getParentFile();

                File tarball = new File( workDir, "driver-tck.tar.gz" );
                FileTools.streamFileTo( "http://alpha.neohq.net/dist/driver-tck.tar.gz", tarball );
                FileTools.extractTarball( tarball, workDir, featureDir );
            }

            return clazz;
        }
    }
}
