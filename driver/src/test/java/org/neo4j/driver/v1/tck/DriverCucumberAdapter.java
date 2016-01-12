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

import cucumber.api.junit.Cucumber;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;

import static org.neo4j.driver.v1.util.FileTools.deleteRecursively;
import static org.neo4j.driver.v1.util.FileTools.extractTarball;
import static org.neo4j.driver.v1.util.FileTools.streamFileTo;

public class DriverCucumberAdapter extends Cucumber
{
    public DriverCucumberAdapter( Class clazz ) throws InitializationError, IOException
    {
        super( ensureFeatureFilesDownloaded( clazz ) );
    }

    public static Class ensureFeatureFilesDownloaded( Class clazz ) throws IOException
    {
        String featureUrl =
                "https://s3-eu-west-1.amazonaws.com/remoting.neotechnology.com/driver-compliance/tck.tar.gz";
        File resourceFolder = new File( "target/resources" );
        File featureTarball = new File( resourceFolder, "tck.tar.gz" );
        File featureFolder = new File( resourceFolder, "/features/" );

        if ( resourceFolder.exists() )
        {
            deleteRecursively( resourceFolder );
        }

        System.out.println( "Starting download of feature files.." );
        streamFileTo( featureUrl, featureTarball );
        System.out.println( "Feature tar file downloaded to " + featureTarball.getCanonicalPath() );

        extractTarball( featureTarball, featureFolder );
        System.out.println( "Unpacked feature file to " + featureFolder.getCanonicalPath() );
        return clazz;
    }
}
