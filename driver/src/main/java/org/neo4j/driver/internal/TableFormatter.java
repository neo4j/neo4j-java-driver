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
package org.neo4j.driver.internal;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;
import org.neo4j.driver.v1.util.Function;

import java.io.PrintWriter;
import java.util.*;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

public class TableFormatter implements ResultFormatter
{

    private static final ResultFormatter INSTANCE = new TableFormatter();

    public static ResultFormatter getInstance()
    {
        return INSTANCE;
    }

    private Function<Value,Object> serializeFunction = new Function<Value,Object>()
    {
        @Override
        public Object apply( Value value )
        {
            return serialize( value );
        }
    };

    @Override
    public void format( PrintWriter writer, StatementResult result )
    {
        List<String> columns = result.keys();
        if ( !columns.isEmpty() )
        {
            List<Record> data = result.list();
            Map<String,Integer> columnSizes = calculateColumnSizes( columns, data );
            String headerLine = createString( columns, columnSizes );
            int lineWidth = headerLine.length() - 2;
            String dashes = "+" + repeat( '-', lineWidth ) + "+";

            String row = (data.size() > 1) ? "rows" : "row";
            String footer = String.format( "%d %s", data.size(), row );

            writer.println( dashes );
            writer.println( headerLine );
            writer.println( dashes );

            for ( Record record : data )
            {
                writer.println( createString( columns, columnSizes, record ) );
            }
            writer.println( dashes );
            writer.println( footer );

            ResultSummary stats = result.consume();
            if ( stats.counters().containsUpdates() )
            {
                writer.print( formatStats( stats.counters() ) );
            }
        }
        else
        {
            ResultSummary stats = result.consume();
            if ( stats.counters().containsUpdates() )
            {
                writer.println( "+-------------------+" );
                writer.println( "| No data returned. |" );
                writer.println( "+-------------------+" );
                writer.print( formatStats( stats.counters() ) );
            }
            else
            {
                writer.println( "+--------------------------------------------+" );
                writer.println( "| No data returned, and nothing was changed. |" );
                writer.println( "+--------------------------------------------+" );
            }
        }
    }

    private String formatStats( SummaryCounters counters )
    {
        if ( !counters.containsUpdates() )
        {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        includeIfNonZero( builder, "Nodes created: ", counters.nodesCreated() );
        includeIfNonZero( builder, "Relationships created: ", counters.relationshipsCreated() );
        includeIfNonZero( builder, "Properties set: ", counters.propertiesSet() );
        includeIfNonZero( builder, "Nodes deleted: ", counters.nodesDeleted() );
        includeIfNonZero( builder, "Relationships deleted: ", counters.relationshipsDeleted() );
        includeIfNonZero( builder, "Labels added: ", counters.labelsAdded() );
        includeIfNonZero( builder, "Labels removed: ", counters.labelsRemoved() );
        includeIfNonZero( builder, "Indexes added: ", counters.indexesAdded() );
        includeIfNonZero( builder, "Indexes removed: ", counters.indexesRemoved() );
        includeIfNonZero( builder, "Constraints added: ", counters.constraintsAdded() );
        includeIfNonZero( builder, "Constraints removed: ", counters.constraintsAdded() );
        return (builder.length() == 0) ? "<Nothing happened>" : builder.toString();
    }

    private void includeIfNonZero( StringBuilder builder, String message, int count )
    {
        if ( count > 0 )
        {
            builder.append( message ).append( count ).append( "\n" );
        }
    }


    private String repeat( char c, int width )
    {
        char[] chars = new char[width];
        Arrays.fill( chars, c );
        return String.valueOf( chars );
    }

    private String createString( List<String> columns, Map<String,Integer> columnSizes, Record m )
    {
        StringBuilder sb = new StringBuilder( "|" );
        for ( String column : columns )
        {
            sb.append( " " );
            Integer length = columnSizes.get( column );
            String txt = serialize( m.get( column ) );
            String value = makeSize( txt, length );
            sb.append( value );
            sb.append( " |" );
        }
        return sb.toString();
    }

    private String createString( List<String> columns, Map<String,Integer> columnSizes )
    {
        StringBuilder sb = new StringBuilder( "|" );
        for ( String column : columns )
        {
            sb.append( " " );
            sb.append( makeSize( column, columnSizes.get( column ) ) );
            sb.append( " |" );
        }
        return sb.toString();
    }

    private Map<String,Integer> calculateColumnSizes( List<String> columns, List<Record> data )
    {
        Map<String,Integer> columnSizes = new LinkedHashMap<>();
        for ( String column : columns )
        {
            columnSizes.put( column, column.length() );
        }
        for ( Record record : data )
        {
            for ( String column : columns )
            {
                int len = serialize( record.get( column ) ).length();
                int existing = columnSizes.get( column );
                if ( existing < len )
                {
                    columnSizes.put( column, len );
                }
            }
        }
        return columnSizes;
    }

    private String makeSize( String txt, int wantedSize )
    {
        int actualSize = txt.length();
        if ( actualSize > wantedSize )
        {
            return txt.substring( 0, wantedSize );
        }
        else if ( actualSize < wantedSize )
        {
            return txt + repeat( ' ', wantedSize - actualSize );
        }
        else
        {
            return txt;
        }
    }

    private String serializeMap( Value value )
    {
        Map<String,Object> map = value.asMap( serializeFunction );
        return serializeMap( map );
    }

    private String serializeMap( Map<String,Object> map )
    {
        StringBuilder sb = new StringBuilder( map.size() * 50 );
        sb.append( "{" );
        for ( Map.Entry<String,Object> entry : map.entrySet() )
        {
            sb.append( entry.getKey() ).append( ": " ).append( entry.getValue() );
        }
        sb.append( "}" );
        return sb.toString();
    }

    private String serialize( Value a )
    {
        if ( a == null || a.isNull() )
        {
            return "<null>";
        }
        if ( a.hasType( TYPE_SYSTEM.STRING() ) )
        {
            return "\"" + a.asString() + "\"";
        }
        if ( a.hasType( TYPE_SYSTEM.NUMBER() ) )
        {
            return String.valueOf( a.asNumber() );
        }
        if ( a.hasType( TYPE_SYSTEM.BOOLEAN() ) )
        {
            return String.valueOf( a.asBoolean() );
        }
        if ( a.hasType( TYPE_SYSTEM.PATH() ) )
        {
            Path p = a.asPath();
            Iterator<Path.Segment> it = p.iterator();
            StringBuilder sb = new StringBuilder( p.length() * 50 );
            while ( it.hasNext() )
            {
                Path.Segment segment = it.next();
                sb.append( serializeNode( segment.start() ) );
                sb.append( serializeRelationship( segment.relationship() ) );
                sb.append( serializeNode( segment.end() ) );
            }
            return sb.toString();
        }
        if ( a.hasType( TYPE_SYSTEM.NODE() ) )
        {
            return serializeNode( a.asNode() );
        }
        if ( a.hasType( TYPE_SYSTEM.RELATIONSHIP() ) )
        {
            Relationship r = a.asRelationship();
            return serializeRelationship( r );
        }
        if ( a.hasType( TYPE_SYSTEM.MAP() ) )
        {
            return serializeMap( a );
        }
        if ( a.hasType( TYPE_SYSTEM.LIST() ) )
        {
            return a.asList( serializeFunction ).toString();
        }
        return a.asString();
    }

    private String serializeRelationship( Relationship r )
    {
        return String.format( "(%d)-[%d:%s%s]->(%d)", r.startNodeId(), r.id(), r.type(), serializeMap( r.asMap() ),
                r.endNodeId() );
    }

    private String serializeNode( Node n )
    {
        return String.format( "(%d:%s%s)", n.id(), n.labels(), serializeMap( n.asMap() ) );
    }
}
