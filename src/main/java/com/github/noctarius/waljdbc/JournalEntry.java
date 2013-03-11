package com.github.noctarius.waljdbc;

import java.io.DataInput;
import java.io.DataOutput;

public interface JournalEntry<V>
{

    void read( DataInput in );

    void write( DataOutput out );

}
