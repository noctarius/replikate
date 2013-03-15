package com.github.noctarius.waljdbc;

import java.util.Arrays;

public class SimpleJournalEntry<V>
    implements JournalEntry<V>
{

    private final V value;

    private final byte type;

    public SimpleJournalEntry( V value, byte type )
    {
        this.value = value;
        this.type = type;
    }

    @Override
    public V getValue()
    {
        return value;
    }

    @Override
    public byte getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "SimpleJournalEntry [value=" + value + ", type=" + type + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + type;
        result = prime * result + ( ( value == null ) ? 0 : value.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        SimpleJournalEntry<?> other = (SimpleJournalEntry<?>) obj;
        if ( type != other.type )
            return false;
        if ( value == null )
        {
            if ( other.value != null )
                return false;
        }
        return nullSafeEquals( value, other.value );
    }

    private boolean nullSafeEquals( Object o1, Object o2 )
    {
        if ( o1 == o2 )
        {
            return true;
        }
        if ( o1 == null || o2 == null )
        {
            return false;
        }
        if ( o1.equals( o2 ) )
        {
            return true;
        }
        if ( o1 instanceof Object[] && o2 instanceof Object[] )
        {
            return Arrays.equals( (Object[]) o1, (Object[]) o2 );
        }
        if ( o1 instanceof boolean[] && o2 instanceof boolean[] )
        {
            return Arrays.equals( (boolean[]) o1, (boolean[]) o2 );
        }
        if ( o1 instanceof byte[] && o2 instanceof byte[] )
        {
            return Arrays.equals( (byte[]) o1, (byte[]) o2 );
        }
        if ( o1 instanceof char[] && o2 instanceof char[] )
        {
            return Arrays.equals( (char[]) o1, (char[]) o2 );
        }
        if ( o1 instanceof double[] && o2 instanceof double[] )
        {
            return Arrays.equals( (double[]) o1, (double[]) o2 );
        }
        if ( o1 instanceof float[] && o2 instanceof float[] )
        {
            return Arrays.equals( (float[]) o1, (float[]) o2 );
        }
        if ( o1 instanceof int[] && o2 instanceof int[] )
        {
            return Arrays.equals( (int[]) o1, (int[]) o2 );
        }
        if ( o1 instanceof long[] && o2 instanceof long[] )
        {
            return Arrays.equals( (long[]) o1, (long[]) o2 );
        }
        if ( o1 instanceof short[] && o2 instanceof short[] )
        {
            return Arrays.equals( (short[]) o1, (short[]) o2 );
        }
        return false;
    }

}
