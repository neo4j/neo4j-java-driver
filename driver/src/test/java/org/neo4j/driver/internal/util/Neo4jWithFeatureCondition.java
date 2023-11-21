/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

import java.util.regex.Pattern;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.driver.testutil.DatabaseExtension;

public class Neo4jWithFeatureCondition implements ExecutionCondition {
    private static final ConditionEvaluationResult ENABLED_NOT_ANNOTATED =
            enabled("Neither @EnabledOnNeo4jWith nor @DisabledOnNeo4jWith is present");
    private static final ConditionEvaluationResult ENABLED_UNKNOWN_DB_VERSION =
            enabled("Shared neo4j is not running, unable to check version");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        var elementOptional = context.getElement();
        if (elementOptional.isPresent()) {
            var element = elementOptional.get();

            var enabledAnnotation = element.getAnnotation(EnabledOnNeo4jWith.class);
            if (enabledAnnotation != null) {
                var result = checkFeatureAvailability(enabledAnnotation.value(), false);
                if (enabledAnnotation.edition() != Neo4jEdition.UNDEFINED) {
                    result = checkEditionAvailability(result, enabledAnnotation.edition());
                }
                return result;
            }

            var disabledAnnotation = element.getAnnotation(DisabledOnNeo4jWith.class);
            if (disabledAnnotation != null) {
                return checkFeatureAvailability(disabledAnnotation.value(), true);
            }
        }
        return ENABLED_NOT_ANNOTATED;
    }

    @SuppressWarnings("resource")
    private static ConditionEvaluationResult checkFeatureAvailability(Neo4jFeature feature, boolean negated) {
        var driver = DatabaseExtension.getInstance().driver();
        if (driver != null) {
            try (var session = driver.session()) {
                var agent = session.executeRead(
                        tx -> tx.run("RETURN 1").consume().server().agent());
                var pattern = Pattern.compile("^Neo4j/(\\d+)\\.(\\d+)\\.(\\d+)(-dev)?$");
                var matcher = pattern.matcher(agent);
                if (!matcher.matches()) {
                    throw new IllegalStateException(String.format("Unexpected server agent value %s", agent));
                }
                var version = new Neo4jFeature.Version(
                        Integer.parseInt(matcher.group(1)),
                        Integer.parseInt(matcher.group(2)),
                        Integer.parseInt(matcher.group(3)));
                return createResult(version, feature, negated);
            }
        }
        return ENABLED_UNKNOWN_DB_VERSION;
    }

    @SuppressWarnings("resource")
    private static ConditionEvaluationResult checkEditionAvailability(
            ConditionEvaluationResult previousResult, Neo4jEdition edition) {
        if (previousResult.isDisabled()) {
            return previousResult;
        }
        var driver = DatabaseExtension.getInstance().driver();
        if (driver != null) {
            try (var session = driver.session()) {
                var value = session.run("CALL dbms.components() YIELD edition")
                        .single()
                        .get("edition")
                        .asString();
                var editionMatches = edition.matches(value);
                return editionMatches
                        ? enabled(previousResult
                                        .getReason()
                                        .map(v -> v + " and enabled")
                                        .orElse("Enabled") + " on " + value + "-edition")
                        : disabled(previousResult
                                        .getReason()
                                        .map(v -> v + " but disabled")
                                        .orElse("Disabled") + " on " + value + "-edition");
            }
        }
        return ENABLED_UNKNOWN_DB_VERSION;
    }

    private static ConditionEvaluationResult createResult(
            Neo4jFeature.Version version, Neo4jFeature feature, boolean negated) {
        if (feature.availableIn(version)) {
            return negated
                    ? disabled("Disabled on neo4j " + version + " because it supports " + feature)
                    : enabled("Enabled on neo4j " + version + " because it supports " + feature);
        } else {
            return negated
                    ? enabled("Enabled on neo4j " + version + " because it does not support " + feature)
                    : disabled("Disabled on neo4j " + version + " because it does not support " + feature);
        }
    }
}
