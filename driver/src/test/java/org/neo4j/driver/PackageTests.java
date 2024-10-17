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

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.library.Architectures;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.bolt.basicimpl.NettyBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.BootstrapFactory;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.ChannelAttributes;
import org.neo4j.driver.internal.bolt.basicimpl.async.connection.EventLoopGroupFactory;
import org.neo4j.driver.internal.bolt.basicimpl.logging.ChannelActivityLogger;
import org.neo4j.driver.internal.bolt.pooledimpl.PooledBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.routedimpl.RoutedBoltConnectionProvider;
import org.neo4j.driver.internal.bolt.routedimpl.cluster.Rediscovery;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.util.ErrorUtil;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PackageTests {
    private JavaClasses allClasses;

    @BeforeAll
    void importAllClasses() {
        this.allClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("org.neo4j.driver..");
    }

    @Test
    void nettyShouldOnlyBeAccessedByBasicBoltImpl() {
        var rule = classes()
                .that()
                .resideInAPackage("io.netty..")
                .should()
                .onlyBeAccessed()
                .byClassesThat(resideInAPackage("org.neo4j.driver.internal.bolt.basicimpl..")
                        .or(resideInAPackage("io.netty.."))
                        .or(assignableTo(DriverFactory.class))
                        .or(assignableTo(ExponentialBackoffRetryLogic.class)));
        rule.check(new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("org.neo4j.driver..", "io.netty.."));
    }

    @Test
    void boltLayerShouldBeSelfContained() {
        // temporarily whitelisted classes
        var whitelistedClasses = Stream.of(
                        // values
                        Value.class,
                        Value[].class,
                        Values.class,
                        Type.class,
                        TypeConstructor.class,
                        TypeSystem.class,
                        Node.class,
                        Record.class,
                        Point.class,
                        MapAccessor.class,
                        InternalNode.class,
                        InternalRelationship.class,
                        InternalPath.class,
                        InternalPath.SelfContainedSegment.class,
                        IsoDuration.class,
                        InternalTypeSystem.class,
                        // exceptions
                        Neo4jException.class,
                        ErrorUtil.class,
                        UntrustedServerException.class)
                .map(JavaClass.Predicates::assignableTo)
                .reduce((one, two) -> DescribedPredicate.or(one, two))
                .get();

        Architectures.layeredArchitecture()
                .consideringOnlyDependenciesInAnyPackage("org.neo4j.driver..")
                .layer("Bolt")
                .definedBy("..internal.bolt..")
                .layer("Whitelisted")
                .definedBy(whitelistedClasses)
                .whereLayer("Bolt")
                .mayOnlyAccessLayers("Whitelisted")
                .check(allClasses);
    }

    @Test
    void boltBasicImplLayerShouldNotBeAccessedDirectly() {
        Architectures.layeredArchitecture()
                .consideringOnlyDependenciesInAnyPackage("org.neo4j.driver..")
                .layer("Bolt basic impl")
                .definedBy("..internal.bolt.basicimpl..")
                .whereLayer("Bolt basic impl")
                .mayNotBeAccessedByAnyLayer()
                .ignoreDependency(DriverFactory.class, BootstrapFactory.class)
                .ignoreDependency(DriverFactory.class, NettyBoltConnectionProvider.class)
                .ignoreDependency(ChannelActivityLogger.class, ChannelAttributes.class)
                .ignoreDependency(Futures.class, EventLoopGroupFactory.class)
                .check(allClasses);
    }

    @Test
    void boltPooledImplLayerShouldNotBeAccessedDirectly() {
        Architectures.layeredArchitecture()
                .consideringOnlyDependenciesInAnyPackage("org.neo4j.driver..")
                .layer("Bolt pooled impl")
                .definedBy("..internal.bolt.pooledimpl..")
                .whereLayer("Bolt pooled impl")
                .mayNotBeAccessedByAnyLayer()
                .ignoreDependency(DriverFactory.class, PooledBoltConnectionProvider.class)
                .check(allClasses);
    }

    @Test
    void boltRoutedImplLayerShouldNotBeAccessedDirectly() {
        Architectures.layeredArchitecture()
                .consideringOnlyDependenciesInAnyPackage("org.neo4j.driver..")
                .layer("Bolt routed impl")
                .definedBy("..internal.bolt.routedimpl..")
                .whereLayer("Bolt routed impl")
                .mayNotBeAccessedByAnyLayer()
                .ignoreDependency(DriverFactory.class, RoutedBoltConnectionProvider.class)
                .ignoreDependency(DriverFactory.class, Rediscovery.class)
                .check(allClasses);
    }
}
