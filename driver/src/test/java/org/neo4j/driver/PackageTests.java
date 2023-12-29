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
package org.neo4j.driver;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PackageTests {
    private final DescribedPredicate<JavaClass> jdbcModuleClasses = resideInAPackage("..jdbc");

    private JavaClasses allClasses;

    @BeforeAll
    void importAllClasses() {
        this.allClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("org.neo4j.driver..");
    }

    @SuppressWarnings("deprecation")
    @ParameterizedTest
    @ValueSource(
            classes = {
                AuthToken.class,
                Bookmark.class,
                AccessMode.class,
                TransactionConfig.class,
                AuthTokenAndExpiration.class,
                AuthTokenManager.class,
                AuthTokenManagers.class,
                AuthTokens.class,
                BaseSession.class,
                BookmarkManager.class,
                BookmarkManagerConfig.class,
                BookmarkManagers.class,
                Config.class,
                ConnectionPoolMetrics.class,
                Driver.class,
                EagerResult.class,
                ExecutableQuery.class,
                GraphDatabase.class,
                //                Logger.class,
                //                Logging.class,
                Metrics.class,
                MetricsAdapter.class,
                NotificationCategory.class,
                NotificationConfig.class,
                NotificationSeverity.class,
                Query.class,
                QueryConfig.class,
                QueryRunner.class,
                Records.class,
                Result.class,
                RevocationCheckingStrategy.class,
                RoutingControl.class,
                Session.class,
                SessionConfig.class,
                SimpleQueryRunner.class,
                Transaction.class,
                TransactionCallback.class,
                TransactionConfig.class,
                TransactionContext.class,
                TransactionWork.class,
                ServerAddress.class,
                ServerAddressResolver.class
            })
    void shouldNotUseClassInBolt(Class<?> cls) {
        var rule = noClasses()
                .that()
                .resideInAPackage("..internal.bolt..")
                .should()
                .dependOnClassesThat()
                .belongToAnyOf(cls);
        rule.check(this.allClasses);
    }
}
