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

import java.util.List;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.mapping.Property;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.MapAccessorWithDefaultValue;
import org.neo4j.driver.util.Immutable;
import org.neo4j.driver.util.Pair;
import org.neo4j.driver.util.Preview;

/**
 * Container for Cypher result values.
 * <p>
 * Streams of records are returned from Cypher query execution, contained
 * within a {@link Result}.
 * <p>
 * A record is a form of ordered map and, as such, contained values can be
 * accessed by either positional {@link #get(int) index} or textual
 * {@link #get(String) key}.
 *
 * @since 1.0
 */
@Immutable
public interface Record extends MapAccessorWithDefaultValue {
    /**
     * Retrieve the keys of the underlying map
     *
     * @return all field keys in order
     */
    @Override
    List<String> keys();

    /**
     * Retrieve the values of the underlying map
     *
     * @return all field keys in order
     */
    @Override
    List<Value> values();

    /**
     * Retrieve the index of the field with the given key
     *
     * @throws java.util.NoSuchElementException if the given key is not from {@link #keys()}
     * @param key the give key
     * @return the index of the field as used by {@link #get(int)}
     */
    int index(String key);

    /**
     * Retrieve the value at the given field index
     *
     * @param index the index of the value
     * @return the value or a {@link org.neo4j.driver.internal.value.NullValue} if the index is out of bounds
     * @throws ClientException if record has not been initialized
     */
    Value get(int index);

    /**
     * Retrieve all record fields
     *
     * @return all fields in key order
     * @throws NoSuchRecordException if the associated underlying record is not available
     */
    List<Pair<String, Value>> fields();

    /**
     * Maps values of this record to properties of the given type providing that is it supported.
     * <p>
     * Example (using the <a href=https://github.com/neo4j-graph-examples/movies>Neo4j Movies Database</a>):
     * <pre>
     * {@code
     * // assuming the following Java record
     * public record MovieInfo(String title, String director, List<String> actors) {}
     * // the record values may be mapped to MovieInfo instances
     * var movies = driver.executableQuery("MATCH (actor:Person)-[:ACTED_IN]->(movie:Movie)<-[:DIRECTED]-(director:Person) RETURN movie.title as title, director.name AS director, collect(actor.name) AS actors")
     *         .execute()
     *         .records()
     *         .stream()
     *         .map(record -> record.as(MovieInfo.class))
     *         .toList();
     * }
     * </pre>
     * <p>
     * Note that Object Mapping is an alternative to accessing the user-defined values in a {@link MapAccessor}.
     * If Object Graph Mapping (OGM) is needed, please use a higher level solution built on top of the driver, like
     * <a href="https://neo4j.com/docs/getting-started/languages-guides/java/spring-data-neo4j/">Spring Data Neo4j</a>.
     * <p>
     * The mapping is done by matching user-defined property names to target type constructor parameters. Therefore, the
     * constructor parameters must either have {@link Property} annotation or have a matching name that is available
     * at runtime (note that the constructor parameter names are typically changed by the compiler unless either the
     * compiler {@code -parameters} option is used or they belong to the cannonical constructor of
     * {@link java.lang.Record java.lang.Record}). The name matching is case-sensitive.
     * <p>
     * Additionally, the {@link Property} annotation may be used when mapping a property with a different name to
     * {@link java.lang.Record java.lang.Record} cannonical constructor parameter.
     * <p>
     * The constructor selection criteria is the following (top priority first):
     * <ol>
     *     <li>Maximum matching properties.</li>
     *     <li>Minimum mismatching properties.</li>
     * </ol>
     * The constructor search is done in the order defined by the {@link Class#getDeclaredConstructors} and is
     * finished either when a full match is found with no mismatches or once all constructors have been visited.
     * <p>
     * At least 1 property match must be present for mapping to work.
     * <p>
     * A {@code null} value is used for arguments that don't have a matching property. If the argument does not accept
     * {@code null} value (this includes primitive types), an alternative constructor that excludes it must be
     * available.
     * <p>
     * The mapping only works for types with directly accessible constructors, not interfaces or abstract types.
     * <p>
     * Types with generic parameters defined at the class level are not supported. However, constructor arguments with
     * specific types are permitted, see the {@code actors} parameter in the example above.
     * <p>
     * On the contrary, the following record would not be mapped because the type information is insufficient:
     * <pre>
     * {@code
     * public record MovieInfo(String title, String director, List<T> actors) {}
     * }
     * </pre>
     * Wildcard type value is not supported.
     *
     * @param targetClass the target class to map to
     * @param <T>         the target type to map to
     * @return the mapped value
     * @throws Uncoercible when mapping to the target type is not possible
     * @throws LossyCoercion when mapping cannot be achieved without losing precision
     * @see Property
     * @see Value#as(Class)
     * @since 5.28.5
     */
    @Preview(name = "Object mapping")
    default <T> T as(Class<T> targetClass) {
        // for backwards compatibility only
        throw new UnsupportedOperationException("not supported");
    }
}
