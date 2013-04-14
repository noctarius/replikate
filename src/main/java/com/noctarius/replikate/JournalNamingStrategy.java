package com.noctarius.replikate;

public interface JournalNamingStrategy
{

    String generate( long logNumber );

    boolean isJournal( String name );

    long extractLogNumber( String name );

}
