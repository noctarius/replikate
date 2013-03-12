package com.github.noctarius.waljdbc;

import java.util.Arrays;

public class SimpleJournalEntry<V>
    implements JournalEntry<V>
{

    private final V value;

    private final byte type;

    private final byte[] data;

    public SimpleJournalEntry( V value, byte[] data, byte type )
    {
        this.value = value;
        this.type = type;
        this.data = Arrays.copyOf( data, data.length );
    }

    public V getValue()
    {
        return value;
    }

    @Override
    public byte[] getData()
    {
        return Arrays.copyOf( data, data.length );
    }

    @Override
    public int getLength()
    {
        return data.length;
    }

    @Override
    public byte getType()
    {
        return type;
    }

}
