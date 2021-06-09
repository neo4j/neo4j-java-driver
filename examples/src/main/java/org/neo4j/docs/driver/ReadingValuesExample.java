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
package org.neo4j.docs.driver;

import java.util.function.Function;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;

import static org.neo4j.driver.Values.parameters;

public class ReadingValuesExample extends BaseApplication
{
    private static final String nullFieldName = "fieldName";
    private static final String intFieldName = "fieldName";

    public ReadingValuesExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    public Boolean nullIsNull()
    {
        return echo( null, record ->
        {
            // tag::java-driver-reading-values-null[]
            Value nullValue = record.get( nullFieldName );

            // Checking if its null
            Boolean trueBoolean = nullValue.isNull(); // true

            // end::java-driver-reading-values-null[]
            return trueBoolean;
        } );
    }

    public String nullAsString()
    {
        return echo( null, record ->
        {
            Value nullValue = record.get( nullFieldName );
            // tag::java-driver-reading-values-null[]
            // Getting the null value as string
            String stringWithNullContent = nullValue.asString(); // "null"

            // end::java-driver-reading-values-null[]
            return stringWithNullContent;
        } );
    }

    public Object nullAsObject()
    {
        return echo( null, record ->
        {
            Value nullValue = record.get( nullFieldName );
            // tag::java-driver-reading-values-null[]
            // Getting `null` as object
            Object nullObject = nullValue.asObject(); // null

            // end::java-driver-reading-values-null[]
            return nullObject;
        } );
    }

    public float nullAsObjectFloatDefaultValue()
    {
        return echo( null, record ->
        {
            Value nullValue = record.get( nullFieldName );
            // tag::java-driver-reading-values-null[]
            // Coercing value with a default value set
            float floatValue = nullValue.asFloat( 1.0f ); // 1.0f

            // end::java-driver-reading-values-null[]
            return floatValue;
        } );
    }

    public void nullAsObjectFloat()
    {
        echo( null, record ->
        {
            Value nullValue = record.get( nullFieldName );
            // tag::java-driver-reading-values-null[]
            // Could not cast null to float
            float floatValue = nullValue.asFloat(); // throws org.neo4j.driver.exceptions.value.Uncoercible
            // end::java-driver-reading-values-null[]
            return floatValue;
        } );
    }

    public Boolean integerFieldIsNull()
    {
        return echo( 4, record ->
        {
            // tag::java-driver-reading-values-non-null[]
            Value value = record.get( intFieldName );
            // Checking if the value is null
            Boolean falseBoolean = value.isNull(); // false

            // end::java-driver-reading-values-non-null[]
            return falseBoolean;
        } );
    }

    public int integerAsInteger()
    {
        return echo( 4, record ->
        {
            Value value = record.get( intFieldName );

            // tag::java-driver-reading-values-non-null[]
            // Getting as int
            int intValue = value.asInt(); // the int

            // end::java-driver-reading-values-non-null[]
            return intValue;
        } );
    }

    public long integerAsLong()
    {
        return echo( 4, record ->
        {
            Value value = record.get( intFieldName );

            // tag::java-driver-reading-values-non-null[]
            // Getting value asLong is also possible for int values
            long longValue = value.asLong(); // the int casted to long

            // end::java-driver-reading-values-non-null[]
            return longValue;
        } );
    }

    public void integerAsString()
    {
        echo( 4, record ->
        {
            Value value = record.get( intFieldName );

            // tag::java-driver-reading-values-non-null[]
            // But it's not possible to get the int value as string
            String stringValue = value.asString(); // throws org.neo4j.driver.exceptions.value.Uncoercible
            // end::java-driver-reading-values-non-null[]
            return stringValue;
        } );
    }

    private <T, O> O echo( T value, Function<Record,O> transformation )
    {
        try ( Session session = driver.session() )
        {
            return session.readTransaction( tx ->
                                            {
                                                Result result = tx.run( "RETURN $in AS fieldName", parameters( "in", value ) );
                                                Record record = result.next();
                                                return transformation.apply( record );
                                            } );
        }
    }
}
