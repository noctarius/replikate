package com.github.noctarius.waljdbc;

public interface JournalEntry<V>
{

    byte[] getData();

    int getLength();

    byte getType();

}
