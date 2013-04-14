package com.noctarius.replikate.spi;

public abstract class Preconditions
{

    public static void notNull( Object value, String name )
    {
        if ( value == null )
        {
            throw new NullPointerException( name + " must not be null" );
        }
    }

    public static void checkType( Object value, Class<?> type, String name )
    {
        if ( !type.isInstance( value ) )
        {
            throw new IllegalArgumentException( name + " is not of type " + type.getName() );
        }
    }

    private Preconditions()
    {
    }

}
