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
package org.neo4j.driver.types;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.Value;
import java.util.function.Function;

/**
 * Provides methods to access the value of an underlying unordered map by key.
 * When calling the methods, a user need to provides a default value, which will be given back if no match found by
 * the key provided.
 * The default value also servers the purpose of specifying the return type of the value found in map by key.
 * If the type of the value found A differs from the type of the default value B, a cast from A to B would happen
 * automatically. Note: Error might arise if the cast from A to B is not possible.
 */
public interface MapAccessorWithDefaultValue
{
    /**
     * Retrieve the value with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the value
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the value found by the key or the default value if no such key exists
     */
    Value get( String key, Value defaultValue );

    /**
     * Retrieve the object with the given key.
     * If no object found by the key, then the default object provided would be returned.
     * @param key the key of the object
     * @param defaultValue the default object that would be returned if no object found by the key in the map
     * @return the object found by the key or the default object if no such key exists
     */
    Object get( String key, Object defaultValue );

    /**
     * Retrieve the number with the given key.
     * If no number found by the key, then the default number provided would be returned.
     * @param key the key of the number
     * @param defaultValue the default number that would be returned if no number found by the key in the map
     * @return the number found by the key or the default number if no such key exists
     */
    Number get( String key, Number defaultValue );

    /**
     * Retrieve the entity with the given key.
     * If no entity found by the key, then the default entity provided would be returned.
     * @param key the key of the entity
     * @param defaultValue the default entity that would be returned if no entity found by the key in the map
     * @return the entity found by the key or the default entity if no such key exists
     */
    Entity get( String key, Entity defaultValue );

    /**
     * Retrieve the node with the given key.
     * If no node found by the key, then the default node provided would be returned.
     * @param key the key of the node
     * @param defaultValue the default node that would be returned if no node found by the key in the map
     * @return the node found by the key or the default node if no such key exists
     */
    Node get( String key, Node defaultValue );

    /**
     * Retrieve the path with the given key.
     * If no path found by the key, then the default path provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default path that would be returned if no path found by the key in the map
     * @return the path found by the key or the default path if no such key exists
     */
    Path get( String key, Path defaultValue );

    /**
     * Retrieve the value with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the value found by the key or the default value if no such key exists
     */
    Relationship get( String key, Relationship defaultValue );

    /**
     * Retrieve the list of objects with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the value
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the list of objects found by the key or the default value if no such key exists
     */
    List<Object> get( String key, List<Object> defaultValue );

    /**
     * Retrieve the list with the given key.
     * If no value found by the key, then the default list provided would be returned.
     * @param key the key of the value
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @param mapFunc the map function that defines how to map each element of the list from {@link Value} to T
     * @param <T> the type of the elements in the returned list
     * @return the converted list found by the key or the default list if no such key exists
     */
    <T> List<T> get( String key, List<T> defaultValue, Function<Value,T> mapFunc );

    /**
     * Retrieve the map with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the map found by the key or the default value if no such key exists
     */
    Map<String, Object> get( String key, Map<String,Object> defaultValue );

    /**
     * Retrieve the map with the given key.
     * If no value found by the key, then the default map provided would be returned.
     * @param key the key of the value
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @param mapFunc the map function that defines how to map each value in map from {@link Value} to T
     * @param <T> the type of the values in the returned map
     * @return the converted map found by the key or the default map if no such key exists.
     */
    <T> Map<String, T> get( String key, Map<String,T> defaultValue, Function<Value,T> mapFunc );

    /**
     * Retrieve the java integer with the given key.
     * If no integer found by the key, then the default integer provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default integer that would be returned if no integer found by the key in the map
     * @return the integer found by the key or the default integer if no such key exists
     */
    int get( String key, int defaultValue );

    /**
     * Retrieve the java long number with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the java long number found by the key or the default value if no such key exists
     */
    long get( String key, long defaultValue );

    /**
     * Retrieve the java boolean with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the java boolean found by the key or the default value if no such key exists
     */
    boolean get( String key, boolean defaultValue );

    /**
     * Retrieve the java string with the given key.
     * If no string found by the key, then the default string provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default string that would be returned if no string found by the key in the map
     * @return the string found by the key or the default string if no such key exists
     */
    String get( String key, String defaultValue );

    /**
     * Retrieve the java float number with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the java float number found by the key or the default value if no such key exists
     */
    float get( String key, float defaultValue );

    /**
     * Retrieve the java double number with the given key.
     * If no value found by the key, then the default value provided would be returned.
     * @param key the key of the property
     * @param defaultValue the default value that would be returned if no value found by the key in the map
     * @return the java double number found by the key or the default value if no such key exists
     */
    double get( String key, double defaultValue );
}
