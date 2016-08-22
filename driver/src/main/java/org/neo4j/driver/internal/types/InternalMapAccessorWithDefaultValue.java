/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.internal.types;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.MapAccessorWithDefaultValue;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Function;

public abstract class InternalMapAccessorWithDefaultValue implements MapAccessorWithDefaultValue
{
    public abstract Value get( String key );

    @Override
    public Value get( String key, Value defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Value get( Value value, Value defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return ((AsValue) value).asValue();
        }
    }

    @Override
    public Object get( String key, Object defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Object get(Value value, Object defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asObject();
        }
    }

    @Override
    public Number get( String key, Number defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Number get( Value value, Number defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asNumber();
        }
    }

    @Override
    public Entity get( String key, Entity defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Entity get( Value value, Entity defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asEntity();
        }
    }

    @Override
    public Node get( String key, Node defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Node get( Value value, Node defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asNode();
        }
    }

    @Override
    public Path get( String key, Path defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Path get( Value value, Path defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asPath();
        }
    }

    @Override
    public Relationship get( String key, Relationship defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Relationship get( Value value, Relationship defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asRelationship();
        }
    }

    @Override
    public List<Object> get( String key, List<Object> defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private List<Object> get( Value value, List<Object> defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return  value.asList();
        }
    }

    @Override
    public  <T> List<T> get( String key, List<T> defaultValue, Function<Value,T> mapFunc )
    {
        return get( get( key ), defaultValue, mapFunc );
    }

    private <T> List<T> get( Value value, List<T> defaultValue, Function<Value, T> mapFunc )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asList( mapFunc );
        }
    }

    @Override
    public Map<String, Object> get( String key, Map<String,Object> defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private Map<String, Object> get( Value value, Map<String, Object> defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asMap();
        }
    }

    @Override
    public <T> Map<String, T> get( String key, Map<String,T> defaultValue, Function<Value,T> mapFunc )
    {
        return get( get( key ), defaultValue, mapFunc );
    }

    private <T>Map<String, T> get( Value value, Map<String, T> defaultValue, Function<Value, T> mapFunc )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asMap( mapFunc );
        }
    }

    @Override
    public int get( String key, int defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private int get( Value value, int defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asInt();
        }
    }

    @Override
    public long get( String key, long defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private long get( Value value, long defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asLong();
        }
    }

    @Override
    public boolean get( String key, boolean defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private boolean get( Value value, boolean defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asBoolean();
        }
    }

    @Override
    public String get( String key, String defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private String get( Value value, String defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asString();
        }
    }

    @Override
    public float get( String key, float defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private float get( Value value, float defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asFloat();
        }
    }

    @Override
    public double get( String key, double defaultValue )
    {
        return get( get( key ), defaultValue );
    }

    private double get( Value value, double defaultValue )
    {
        if( value.equals( Values.NULL ) )
        {
            return defaultValue;
        }
        else
        {
            return value.asDouble();
        }
    }

}
