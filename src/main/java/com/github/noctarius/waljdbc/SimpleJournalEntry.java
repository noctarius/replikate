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

}
