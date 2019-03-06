/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.v1.util;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class CertificateExtension extends DatabaseExtension implements BeforeAllCallback, AfterAllCallback
{
    private static Path originalKeyFile;
    private static Path originalCertFile;

    @Override
    public void beforeAll( ExtensionContext extensionContext ) throws Exception
    {
        originalKeyFile = Files.createTempFile( "key-file-", "" );
        originalCertFile = Files.createTempFile( "cert-file-", "" );

        Files.copy( tlsKeyFile().toPath(), originalKeyFile , StandardCopyOption.REPLACE_EXISTING);
        Files.copy( tlsCertFile().toPath(), originalCertFile, StandardCopyOption.REPLACE_EXISTING );
    }

    @Override
    public void afterAll( ExtensionContext extensionContext ) throws Exception
    {
        updateEncryptionKeyAndCert( originalKeyFile.toFile(), originalCertFile.toFile() );
        Files.deleteIfExists( originalKeyFile );
        Files.deleteIfExists( originalCertFile );
    }
}
