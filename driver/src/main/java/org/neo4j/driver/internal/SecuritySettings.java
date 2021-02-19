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
package org.neo4j.driver.internal;

import java.io.IOException;
import java.security.GeneralSecurityException;

import org.neo4j.driver.Config;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.SecurityPlanImpl;

import static org.neo4j.driver.internal.Scheme.isHighTrustScheme;
import static org.neo4j.driver.internal.Scheme.isSecurityScheme;
import static org.neo4j.driver.internal.security.SecurityPlanImpl.insecure;

public class SecuritySettings
{
    private static final boolean DEFAULT_ENCRYPTED = false;
    private static final Config.TrustStrategy DEFAULT_TRUST_STRATEGY = Config.TrustStrategy.trustSystemCertificates();
    private static final SecuritySettings DEFAULT = new SecuritySettings( DEFAULT_ENCRYPTED, DEFAULT_TRUST_STRATEGY );
    private final boolean encrypted;
    private final Config.TrustStrategy trustStrategy;

    public SecuritySettings( boolean encrypted, Config.TrustStrategy trustStrategy )
    {
        this.encrypted = encrypted;
        this.trustStrategy = trustStrategy == null ? DEFAULT_TRUST_STRATEGY : trustStrategy;
    }

    public boolean encrypted()
    {
        return encrypted;
    }

    public Config.TrustStrategy trustStrategy()
    {
        return trustStrategy;
    }

    private boolean isCustomized()
    {
        return this != DEFAULT;
    }

    public SecurityPlan createSecurityPlan( String uriScheme )
    {
        Scheme.validateScheme( uriScheme );
        try
        {
            if ( isSecurityScheme( uriScheme ) )
            {
                assertSecuritySettingsNotUserConfigured( uriScheme );
                return createSecurityPlanFromScheme( uriScheme );
            }
            else
            {
                return createSecurityPlanImpl( encrypted, trustStrategy );
            }
        }
        catch ( GeneralSecurityException | IOException ex )
        {
            throw new ClientException( "Unable to establish SSL parameters", ex );
        }
    }

    private void assertSecuritySettingsNotUserConfigured( String uriScheme )
    {
        if ( isCustomized() )
        {
            throw new ClientException( String.format( "Scheme %s is not configurable with manual encryption and trust settings", uriScheme ) );
        }
    }

    private SecurityPlan createSecurityPlanFromScheme( String scheme ) throws GeneralSecurityException, IOException
    {
        if ( isHighTrustScheme(scheme) )
        {
            return SecurityPlanImpl.forSystemCASignedCertificates( true, RevocationStrategy.NO_CHECKS );
        }
        else
        {
            return SecurityPlanImpl.forAllCertificates( false, RevocationStrategy.NO_CHECKS );
        }
    }

    /*
     * Establish a complete SecurityPlan based on the details provided for
     * driver construction.
     */
    private static SecurityPlan createSecurityPlanImpl( boolean encrypted, Config.TrustStrategy trustStrategy )
            throws GeneralSecurityException, IOException
    {
        if ( encrypted )
        {
            boolean hostnameVerificationEnabled = trustStrategy.isHostnameVerificationEnabled();
            RevocationStrategy revocationStrategy = trustStrategy.revocationStrategy();
            switch ( trustStrategy.strategy() )
            {
            case TRUST_CUSTOM_CA_SIGNED_CERTIFICATES:
                return SecurityPlanImpl.forCustomCASignedCertificates( trustStrategy.certFile(), hostnameVerificationEnabled, revocationStrategy );
            case TRUST_SYSTEM_CA_SIGNED_CERTIFICATES:
                return SecurityPlanImpl.forSystemCASignedCertificates( hostnameVerificationEnabled, revocationStrategy );
            case TRUST_ALL_CERTIFICATES:
                return SecurityPlanImpl.forAllCertificates( hostnameVerificationEnabled, revocationStrategy );
            default:
                throw new ClientException(
                        "Unknown TLS authentication strategy: " + trustStrategy.strategy().name() );
            }
        }
        else
        {
            return insecure();
        }
    }

    public static class SecuritySettingsBuilder
    {
        private boolean isCustomized = false;
        private boolean encrypted;
        private Config.TrustStrategy trustStrategy;

        public SecuritySettingsBuilder withEncryption()
        {
            encrypted = true;
            isCustomized = true;
            return this;
        }

        public SecuritySettingsBuilder withoutEncryption()
        {
            encrypted = false;
            isCustomized = true;
            return this;
        }

        public SecuritySettingsBuilder withTrustStrategy( Config.TrustStrategy strategy )
        {
            trustStrategy = strategy;
            isCustomized = true;
            return this;
        }

        public SecuritySettings build()
        {
            return isCustomized ? new SecuritySettings( encrypted, trustStrategy ) : SecuritySettings.DEFAULT;
        }
    }
}
