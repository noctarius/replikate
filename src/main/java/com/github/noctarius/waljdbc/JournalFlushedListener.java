package com.github.noctarius.waljdbc;

public interface JournalFlushedListener<V>
{

    void flushed( V entry );

    void failed( V entry, JournalException cause );

}
