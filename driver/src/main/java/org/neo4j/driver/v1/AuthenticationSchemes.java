package org.neo4j.driver.v1;

import org.neo4j.driver.v1.auth.BasicAuthenticationScheme;

/**
 * This class provides access to all authentication schemes supported by
 * this driver implementation
 */
public final class AuthenticationSchemes
{
    private AuthenticationSchemes()
    {
        throw new UnsupportedOperationException();
    }

    public static final BasicAuthenticationScheme BASIC = null;
}
