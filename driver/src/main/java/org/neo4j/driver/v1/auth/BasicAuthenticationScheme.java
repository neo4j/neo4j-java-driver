package org.neo4j.driver.v1.auth;

/**
 * Basic authentication uses the user name as the principal and transmits
 * the password as its credential as clear text as part of establishing a session
 *
 * Basis authentication should not be used over unencrypted connections
 *
 * @see AuthenticationScheme
 */
public interface BasicAuthenticationScheme extends AuthenticationScheme
{
    /**
     * Obtain a {@link Credential} for authentication using the basic authentication scheme
     *
     * @param password to be used
     * @return a credential for authentication using the basic authentication scheme
     */
    Credential credential( String password );

    /**
     * Obtain a {@link Token} for authentication using the basic authentication scheme
     *
     * @param userName of the user to be authenticated
     * @param password of the user to be authenticated
     * @return token for authentication using the basic authentication scheme
     */
    Token token( String userName, String password );

    interface Credential extends AuthenticationScheme.Credential
    {
        @Override
        BasicAuthenticationScheme scheme();
    }

    interface Token extends AuthenticationScheme.Token, Credential
    {
        /**
         * @return user name of the user to be authenticated using this token
         */
        @Override
        String principal();
    }
}
