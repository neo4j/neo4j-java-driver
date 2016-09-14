package org.neo4j.driver.v1.types;

import java.util.Map;

import static java.lang.String.format;

public class Representations
{
    private static final String SAFE_FIRST_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";
    private static final String SAFE_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";

    public static String repr( Object value )
    {
        if ( value == null )
        {
            return "";
        }
        else if ( value instanceof String )
        {
            return repr( (String) value );
        }
        else if ( value instanceof Map )
        {
            return repr( (Map) value );
        }
        else if ( value instanceof Node )
        {
            return repr( (Node) value );
        }
        else if ( value instanceof Relationship )
        {
            return repr( (Relationship) value );
        }
        else if ( value instanceof Path )
        {
            return repr( (Path) value );
        }
        else
        {
            return value.toString();
        }
    }

    public static String repr( String value )
    {
        return "'" + value.replace( "\\", "\\\\" ).replace( "'", "\\'" ) + "'";
    }

    public static String repr( Map value )
    {
        StringBuilder s = new StringBuilder();
        s.append( '{' );
        for ( Object key : value.keySet() )
        {
            s.append( escape( key.toString() ) );
            s.append( ':' );
            s.append( repr( value.get( key ) ) );
        }
        s.append( '}' );
        return s.toString();
    }

    public static String repr( Node value )
    {
        StringBuilder s = new StringBuilder();
        s.append( "(_" );
        s.append( value.id() );
        for ( String label : value.labels() )
        {
            s.append( ':' );
            s.append( escape( label ) );
        }
        s.append( ' ' );
        s.append( repr( value.asMap() ) );
        s.append( ')' );
        return s.toString();
    }

    public static String repr( Relationship value )
    {
        return format( "%s-%s->%s",
                reprNodeByID( value.startNodeId() ),
                reprRelationshipDetail( value ),
                reprNodeByID( value.endNodeId() ) );
    }

    public static String repr( Path value )
    {
        StringBuilder s = new StringBuilder();
        long last = value.start().id();
        s.append( reprNodeByID( last ) );
        String template;
        for ( Path.Segment segment : value )
        {
            long start = segment.relationship().startNodeId();
            long end = segment.relationship().endNodeId();
            if ( start == last )
            {
                // forward
                last = end;
                template = "-%s->%s";
            }
            else
            {
                // backward
                last = start;
                template = "<-%s-%s";
            }
            s.append( format( template, reprRelationshipDetail( segment.relationship() ), reprNodeByID( last ) ) );
        }
        return s.toString();
    }

    private static String reprNodeByID( long id )
    {
        return format( "(_%d)", id );
    }

    private static String reprRelationshipDetail( Relationship value )
    {
        final Map<String, Object> properties = value.asMap();
        if ( properties.isEmpty() )
        {
            return format( "[_%d:%s]", value.id(), escape( value.type() ) );
        }
        else
        {
            return format( "[_%d:%s %s]", value.id(), escape( value.type() ), repr( properties ) );
        }
    }

    public static String escape( String identifier )
    {
        if ( identifier == null || identifier.length() == 0 )
        {
            throw new IllegalArgumentException( "Invalid identifier" );
        }
        boolean safe = true;
        for ( int i = 0; i < identifier.length(); i++ )
        {
            if ( i == 0 )
            {
                safe = safe && SAFE_FIRST_CHARS.indexOf( identifier.charAt( i ) ) >= 0;
            }
            else
            {
                safe = safe && SAFE_CHARS.indexOf( identifier.charAt( i ) ) >= 0;
            }
        }
        if ( safe )
        {
            return identifier;
        }
        else
        {
            return "`" + identifier.replace( "`", "``" ) + "`";
        }
    }

}
