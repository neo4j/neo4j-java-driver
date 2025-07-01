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
package org.neo4j.driver.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.driver.util.Preview;

/**
 * Marks the annotated array as Neo4j Vector.
 * <p>
 * Example:
 * <pre>
 * {@code
 * // assuming the following Java record
 * public record Entity(String name, @Vector double[] vector) {}
 * // the vector will be stored as Neo4j Vector in the database
 * }
 * </pre>
 *
 * @since 6.0.0
 */
@Target({ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
@Preview(name = "Neo4j Vector")
public @interface Vector {}
