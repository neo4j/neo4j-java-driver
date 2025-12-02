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
/**
 * The Neo4j Java Driver module.
 */
@SuppressWarnings({"requires-automatic", "requires-transitive-automatic"})
module org.neo4j.driver {
    exports org.neo4j.driver;
    exports org.neo4j.driver.async;
    exports org.neo4j.driver.reactive;
    exports org.neo4j.driver.reactivestreams;
    exports org.neo4j.driver.types;
    exports org.neo4j.driver.summary;
    exports org.neo4j.driver.net;
    exports org.neo4j.driver.util;
    exports org.neo4j.driver.exceptions;
    exports org.neo4j.driver.exceptions.value;
    exports org.neo4j.driver.mapping;
    exports org.neo4j.driver.observation;
    exports org.neo4j.driver.internal.observation to
            org.neo4j.driver.observation.metrics,
            org.neo4j.driver.observation.micrometer;
    exports org.neo4j.driver.property_encryption;
    exports org.neo4j.driver.property_encryption.async;
    exports org.neo4j.driver.property_encryption.reactive;
    exports org.neo4j.driver.property_encryption.reactivestreams;

    requires org.neo4j.bolt.connection;
    requires org.neo4j.bolt.connection.pooled;
    requires org.neo4j.bolt.connection.routed;
    requires org.neo4j.bolt.connection.codec;
    requires reactor.core;
    requires transitive java.logging;
    requires transitive org.reactivestreams;
    requires static org.graalvm.nativeimage;
    requires static org.slf4j;
    requires static java.management;
    requires static reactor.blockhound;

    uses org.neo4j.bolt.connection.BoltConnectionProviderFactory;
    uses org.neo4j.bolt.connection.codec.packstream.PackStreamEncoderFactory;
    uses org.neo4j.bolt.connection.codec.packstream.PackStreamDecoderFactory;
    uses org.neo4j.bolt.connection.codec.value_encoding.ValueEncoderFactory;
    uses org.neo4j.bolt.connection.codec.value_encoding.ValueDecoderFactory;
}
