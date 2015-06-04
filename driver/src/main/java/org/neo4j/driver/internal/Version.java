package org.neo4j.driver.internal;

import org.neo4j.driver.Session;

public class Version
{
    /**
     * Extracts the driver version from the driver jar MANIFEST.MF file.
     */
    public static String driverVersion()
    {
        // "Session" is arbitrary - the only thing that matters is that the class we use here is in the
        // 'org.neo4j.driver' package, because that is where the jar manifest specifies the version.
        // This is done as part of the build, adding a MANIFEST.MF file to the generated jarfile.
        Package pkg = Session.class.getPackage();
        if(pkg != null && pkg.getSpecificationVersion() != null)
        {
            return pkg.getSpecificationVersion();
        }

        // If there is no version, we're not running from a jar file, but from raw compiled class files.
        // This should only happen during development, so call the version 'dev'.
        return "dev";
    }
}
