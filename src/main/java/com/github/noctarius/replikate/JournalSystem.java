package com.github.noctarius.replikate;

import java.util.concurrent.ExecutorService;

import com.github.noctarius.replikate.spi.AbstractJournalSystem;
import com.github.noctarius.replikate.spi.JournalFactory;

public abstract class JournalSystem
{

    public abstract <V> Journal<V> getJournal( String name, JournalConfiguration<V> configuration );

    public abstract <V> Journal<V> getJournal( String name, JournalFactory<V> journalFactory,
                                               JournalConfiguration<V> configuration );

    public abstract <V> Journal<V> getJournal( String name, JournalStrategy journalStrategy,
                                               JournalConfiguration<V> configuration );

    public abstract void shutdown();

    public static JournalSystem buildJournalSystem()
    {
        return buildJournalSystem( null );
    }

    public static JournalSystem buildJournalSystem( ExecutorService listenerExecutorService )
    {
        return new AbstractJournalSystem( listenerExecutorService )
        {
        };
    }

}
