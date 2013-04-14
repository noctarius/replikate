/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.noctarius.replikate.spi;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.noctarius.replikate.Journal;
import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.exceptions.JournalException;

public abstract class AbstractJournal<V>
    implements Journal<V>
{

    private final int journalVersion = JOURNAL_VERSION;

    private final AtomicLong logNumber = new AtomicLong( 0 );

    private final JournalRecordIdGenerator recordIdGenerator;

    private final JournalEntryReader<V> reader;

    private final JournalEntryWriter<V> writer;

    private final JournalNamingStrategy namingStrategy;

    private final ExecutorService listenerExecutorService;

    private final String name;

    protected AbstractJournal( String name, JournalRecordIdGenerator recordIdGenerator, JournalEntryReader<V> reader,
                               JournalEntryWriter<V> writer, JournalNamingStrategy namingStrategy,
                               ExecutorService listenerExecutorService )
    {
        this.name = name;
        this.recordIdGenerator = recordIdGenerator;
        this.reader = reader;
        this.writer = writer;
        this.namingStrategy = namingStrategy;
        this.listenerExecutorService = listenerExecutorService;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public long nextLogNumber()
    {
        return logNumber.incrementAndGet();
    }

    @Override
    public JournalRecordIdGenerator getRecordIdGenerator()
    {
        return recordIdGenerator;
    }

    @Override
    public JournalEntryReader<V> getReader()
    {
        return reader;
    }

    @Override
    public JournalEntryWriter<V> getWriter()
    {
        return writer;
    }

    @Override
    public JournalNamingStrategy getNamingStrategy()
    {
        return namingStrategy;
    }

    @Override
    public long getLastRecordId()
    {
        return getRecordIdGenerator().lastGeneratedRecordId();
    }

    @Override
    public void close()
        throws IOException
    {
        listenerExecutorService.shutdown();
    }

    public int getJournalVersion()
    {
        return journalVersion;
    }

    protected void setCurrentLogNumber( long logNumber )
    {
        this.logNumber.set( logNumber );
    }

    protected void onCommit( final JournalListener<V> journalListener, final JournalRecord<V> record )
    {
        listenerExecutorService.execute( new Runnable()
        {

            @Override
            public void run()
            {
                journalListener.onCommit( record );
            }
        } );
    }

    protected void onFailure( final JournalListener<V> journalListener, final JournalEntry<V> entry,
                              final JournalException cause )
    {
        listenerExecutorService.execute( new Runnable()
        {

            @Override
            public void run()
            {
                journalListener.onFailure( entry, cause );
            }
        } );
    }

    protected void onFailure( final JournalListener<V> journalListener, final JournalBatch<V> journalBatch,
                              final JournalException cause )
    {
        listenerExecutorService.execute( new Runnable()
        {

            @Override
            public void run()
            {
                journalListener.onFailure( journalBatch, cause );
            }
        } );
    }

    protected class SimpleAppendOperation
        implements JournalOperation
    {

        protected final JournalEntry<V> entry;

        protected final JournalListener<V> listener;

        public SimpleAppendOperation( JournalEntry<V> entry, JournalListener<V> listener )
        {
            this.entry = entry;
            this.listener = listener;
        }

        public void execute()
        {
            appendEntrySynchronous( entry, listener );
        }
    }

}
