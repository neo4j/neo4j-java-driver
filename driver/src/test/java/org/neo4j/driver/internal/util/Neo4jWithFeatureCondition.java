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
package org.neo4j.driver.internal.util;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.util.Neo4jRunner;
import org.neo4j.driver.util.Neo4jSettings;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

public class Neo4jWithFeatureCondition implements ExecutionCondition
{
    private static final ConditionEvaluationResult ENABLED_NOT_ANNOTATED = enabled( "Neither @EnabledOnNeo4jWith nor @DisabledOnNeo4jWith is present" );
    private static final ConditionEvaluationResult ENABLED_UNKNOWN_DB_VERSION = enabled( "Shared neo4j is not running, unable to check version" );

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition( ExtensionContext context )
    {
        Optional<AnnotatedElement> elementOptional = context.getElement();
        if ( elementOptional.isPresent() )
        {
            AnnotatedElement element = elementOptional.get();

            EnabledOnNeo4jWith enabledAnnotation = element.getAnnotation( EnabledOnNeo4jWith.class );
            if ( enabledAnnotation != null )
            {
                ConditionEvaluationResult result = checkFeatureAvailability( enabledAnnotation.value(), false );
                if ( enabledAnnotation.edition() != Neo4jEdition.UNDEFINED )
                {
                    result = checkEditionAvailability( result, enabledAnnotation.edition() );
                } return result;
            }

            DisabledOnNeo4jWith disabledAnnotation = element.getAnnotation( DisabledOnNeo4jWith.class );
            if ( disabledAnnotation != null )
            {
                return checkFeatureAvailability( disabledAnnotation.value(), true );
            }
        }
        return ENABLED_NOT_ANNOTATED;
    }

    private static ConditionEvaluationResult checkFeatureAvailability( Neo4jFeature feature, boolean negated )
    {
        Driver driver = getSharedNeo4jDriver();
        if ( driver != null )
        {
            ServerVersion version = ServerVersion.version( driver );
            return createResult( version, feature, negated );
        }
        return ENABLED_UNKNOWN_DB_VERSION;
    }

    private static ConditionEvaluationResult checkEditionAvailability( ConditionEvaluationResult previousResult, Neo4jEdition edition )
    {
        if(previousResult.isDisabled()) {
            return previousResult;
        }
        Driver driver = getSharedNeo4jDriver();
        if ( driver != null )
        {
            try ( Session session = driver.session() )
            {
                String value = session.run( "CALL dbms.components() YIELD edition" ).single().get( "edition" ).asString();
                boolean editionMatches = edition.matches( value );
                return editionMatches
                       ? enabled( previousResult.getReason().map( v -> v + " and enabled" ).orElse( "Enabled" ) + " on " + value + "-edition" )
                       : disabled( previousResult.getReason().map( v -> v + " but disabled" ).orElse( "Disabled" ) + " on " + value + "-edition" );
            }
        }
        return ENABLED_UNKNOWN_DB_VERSION;
    }

    private static ConditionEvaluationResult createResult( ServerVersion version, Neo4jFeature feature, boolean negated )
    {
        if ( feature.availableIn( version ) )
        {
            return negated
                   ? disabled( "Disabled on neo4j " + version + " because it supports " + feature )
                   : enabled( "Enabled on neo4j " + version + " because it supports " + feature );
        }
        else
        {
            return negated
                   ? enabled( "Enabled on neo4j " + version + " because it does not support " + feature )
                   : disabled( "Disabled on neo4j " + version + " because it does not support " + feature );
        }
    }

    private static Driver getSharedNeo4jDriver()
    {
        try
        {
            Neo4jRunner runner = Neo4jRunner.getOrCreateGlobalRunner();
            // ensure database is running with default credentials
            runner.ensureRunning( Neo4jSettings.TEST_SETTINGS );
            return runner.driver();
        }
        catch ( Throwable t )
        {
            System.err.println( "Failed to check database version in the test execution condition" );
            t.printStackTrace();
            return null;
        }
    }
}
