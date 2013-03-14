package com.github.noctarius.waljdbc;

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
        else if ( !value.equals( other.value ) )
            return false;
        return true;
    }

}
