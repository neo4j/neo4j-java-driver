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
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import static org.neo4j.driver.util.Neo4jRunner.debug;

public class CertificateExtension extends DatabaseExtension implements AfterEachCallback
{
    private Path originalKeyFile;
    private Path originalCertFile;

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        super.beforeEach( context );

        originalKeyFile = Files.createTempFile( "key-file-", "" );
        originalCertFile = Files.createTempFile( "cert-file-", "" );

        Files.copy( tlsKeyFile().toPath(), originalKeyFile, StandardCopyOption.REPLACE_EXISTING );
        Files.copy( tlsCertFile().toPath(), originalCertFile, StandardCopyOption.REPLACE_EXISTING );
    }

    @Override
    public void afterEach( ExtensionContext context ) throws Exception
    {
        // if the key and cert file changed, then we restore the file and restart the server.
        if ( !smallFileContentEquals( tlsKeyFile().toPath(), originalKeyFile ) || !smallFileContentEquals( tlsCertFile().toPath(), originalCertFile ) )
        {
            debug( "Restoring original key and certificate file after certificate test." );
            updateEncryptionKeyAndCert( originalKeyFile.toFile(), originalCertFile.toFile() );
        }
        Files.deleteIfExists( originalKeyFile );
        Files.deleteIfExists( originalCertFile );
    }

    private boolean smallFileContentEquals( Path path, Path pathAnother ) throws IOException
    {
        byte[] fileContent = Files.readAllBytes( path );
        byte[] fileContentAnother = Files.readAllBytes( pathAnother );
        return Arrays.equals( fileContent, fileContentAnother );
    }
}
