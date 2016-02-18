package org.neo4j.driver.v1.auth;

/**
 * An authentication scheme is a method of performing authentication against a Neo4j graph database
 */
public interface AuthenticationScheme
{
    /**
     * @return the (human readable) name of the authentication scheme
     */
    String name();

    /**
     * A credential encapsulates a critical piece of information (like a password or a certificate)
     * for verifying the identify of a subject (a user)
     *
     * @see AuthenticationScheme
     */
    interface Credential
    {
        /**
         * @return the authentication scheme in which this credential may be used
         */
        AuthenticationScheme scheme();
    }

    /**
     * A token is the combination of a credential and an associated principal. A principal is a piece
     * of information (like a user name) that uniquely describes the subject (user) whose identity
     * is verified by the credential.
     *
     * @see Credential
     * @see AuthenticationScheme
     */
    interface Token extends Credential
    {
        /**
         * @return the principal of the encapsulated credential
         */
        Object principal();
    }
}
