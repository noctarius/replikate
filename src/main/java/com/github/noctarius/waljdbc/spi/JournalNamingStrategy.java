package com.github.noctarius.waljdbc.spi;

public interface JournalNamingStrategy
{

    String generate( long logNumber );

    boolean isJournal( String name );

    long extractLogNumber( String name );

}
