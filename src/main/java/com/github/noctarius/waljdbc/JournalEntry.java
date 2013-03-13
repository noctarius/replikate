package com.github.noctarius.waljdbc;

public interface JournalEntry<V>
{

    V getValue();

    byte getType();

}
