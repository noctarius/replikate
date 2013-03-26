package com.github.noctarius.replikate;

public interface JournalEntry<V>
{

    V getValue();

    byte getType();

}
