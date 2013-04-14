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
package com.noctarius.replikate.io.amqp;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.noctarius.replikate.JournalBatch;
import com.noctarius.replikate.JournalEntry;
import com.noctarius.replikate.JournalListener;
import com.noctarius.replikate.JournalNamingStrategy;
import com.noctarius.replikate.JournalRecord;
import com.noctarius.replikate.exceptions.JournalException;
import com.noctarius.replikate.exceptions.SynchronousJournalException;
import com.noctarius.replikate.spi.AbstractJournal;
import com.noctarius.replikate.spi.JournalEntryReader;
import com.noctarius.replikate.spi.JournalEntryWriter;
import com.noctarius.replikate.spi.JournalOperation;
import com.noctarius.replikate.spi.JournalRecordIdGenerator;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

class AMQPJournal<V>
    extends AbstractJournal<V>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( AMQPJournal.class );

    private final ConnectionFactory connectionFactory = new ConnectionFactory();

    private final AMQP.BasicProperties sendBasicProperties =
        new AMQP.BasicProperties.Builder().contentType( "application/octet-stream" ).deliveryMode( 2 ).priority( 1 ).build();

    private final BlockingQueue<JournalOperation> journalQueue = new LinkedBlockingQueue<>();

    private final JournalSenderTask journalSenderTask = new JournalSenderTask();

    private final CountDownLatch shutdownLatch = new CountDownLatch( 1 );

    private final AtomicBoolean shutdown = new AtomicBoolean( false );

    private final Thread journalSender;

    private final Connection connection;

    private final Channel channel;

    AMQPJournal( String name, String amqpUrl, JournalRecordIdGenerator recordIdGenerator, JournalEntryReader<V> reader,
                 JournalEntryWriter<V> writer, JournalNamingStrategy namingStrategy,
                 ExecutorService listenerExecutorService )
        throws Exception
    {
        super( name, recordIdGenerator, reader, writer, namingStrategy, listenerExecutorService );

        connectionFactory.setUri( amqpUrl );
        connection = connectionFactory.newConnection( listenerExecutorService );
        channel = connection.createChannel();

        journalSender = new Thread( journalSenderTask, "AMQP-Journal-Sender-" + name );
        journalSender.start();
    }

    @Override
    public void appendEntry( JournalEntry<V> entry )
        throws JournalException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void appendEntry( JournalEntry<V> entry, JournalListener<V> listener )
        throws JournalException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void appendEntrySynchronous( JournalEntry<V> entry )
        throws JournalException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void appendEntrySynchronous( JournalEntry<V> entry, JournalListener<V> listener )
        throws JournalException
    {
        // TODO Auto-generated method stub

    }

    @Override
    public JournalBatch<V> startBatchProcess()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JournalBatch<V> startBatchProcess( JournalListener<V> listener )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close()
        throws IOException
    {
        if ( !shutdown.compareAndSet( false, true ) )
        {
            return;
        }

        try
        {
            journalSenderTask.shutdown();

            // Wait for asynchronous journal writer to finish
            shutdownLatch.await();

            channel.close();
            connection.close();
        }
        catch ( InterruptedException e )
        {

        }
    }

    private class JournalSenderTask
        implements Runnable
    {

        private final AtomicBoolean shutdown = new AtomicBoolean( false );

        @Override
        public void run()
        {
            try
            {
                while ( true )
                {
                    try
                    {
                        // If all work is done, break up
                        if ( shutdown.get() && journalQueue.size() == 0 )
                        {
                            break;
                        }

                        JournalOperation operation = journalQueue.take();
                        if ( operation != null )
                        {
                            operation.execute();
                        }

                        Thread.sleep( 1 );
                    }
                    catch ( InterruptedException e )
                    {
                        if ( !shutdown.get() )
                        {
                            LOGGER.warn( "JournalSenderTask ignores to interrupt, to shutdown "
                                + "it call JournalSenderTask::shutdown()", e );
                        }
                    }
                }
            }
            finally
            {
                shutdownLatch.countDown();
            }
        }

        public void shutdown()
        {
            shutdown.compareAndSet( false, true );
            journalSender.interrupt();
        }
    }

    private class BatchCommitOperation
        implements JournalOperation
    {

        private final List<AMQPJournalEntryFacade<V>> entries;

        private final JournalBatch<V> journalBatch;

        private final JournalListener<V> listener;

        private BatchCommitOperation( List<AMQPJournalEntryFacade<V>> entries, JournalBatch<V> journalBatch,
                                      JournalListener<V> listener )
        {
            this.entries = entries;
            this.journalBatch = journalBatch;
            this.listener = listener;
        }

        public void execute()
        {
            // Storing current recordId for case of rollback
            long markedRecordId = getRecordIdGenerator().lastGeneratedRecordId();

            try
            {
                commit();
            }
            catch ( Exception e )
            {
                if ( listener != null )
                {
                    onFailure( listener, journalBatch,
                               new SynchronousJournalException( "Failed to persist journal batch process", e ) );
                }

                // Rollback the journal file
                try
                {
                    rollback();
                }
                catch ( IOException ioe )
                {
                    LOGGER.error( "Transaction could not be rollbacked", ioe );
                }

                // Rollback the recordId
                getRecordIdGenerator().notifyHighestJournalRecordId( markedRecordId );
            }
        }

        protected void rollback()
            throws IOException
        {
            channel.txRollback();
        }

        protected void commit()
            throws IOException
        {
            channel.txSelect();

            // Send all entries in one transaction over to the AMQP server
            List<AMQPJournalRecord<V>> records = new LinkedList<>();
            for ( AMQPJournalEntryFacade<V> entry : entries )
            {
                long recordId = getRecordIdGenerator().nextRecordId();
                AMQPJournalRecord<V> record = new AMQPJournalRecord<>( entry.wrappedEntry, recordId );
                channel.basicPublish( "", "", sendBasicProperties, entry.cachedData );
                records.add( record );
            }

            channel.txCommit();

            // ... and if non of them failed just announce them as committed
            for ( JournalRecord<V> record : records )
            {
                onCommit( listener, record );
            }
        }
    }

    private class BatchCommitSyncOperation
        extends BatchCommitOperation
    {

        private final CountDownLatch synchronizer;

        private volatile JournalException journalException = null;

        BatchCommitSyncOperation( List<AMQPJournalEntryFacade<V>> entries, JournalBatch<V> journalBatch,
                                  JournalListener<V> listener, CountDownLatch synchronizer )
        {
            super( entries, journalBatch, listener );
            this.synchronizer = synchronizer;
        }

        @Override
        public void execute()
        {
            // Storing current recordId for case of rollback
            long markedRecordId = getRecordIdGenerator().lastGeneratedRecordId();

            try
            {
                commit();
            }
            catch ( Exception e )
            {
                journalException = new JournalException( "Could not rollback journal batch file", e );

                // Rollback the journal file
                try
                {
                    rollback();
                }
                catch ( IOException ioe )
                {
                    LOGGER.error( "Transaction could not be rollbacked", ioe );
                }

                // Rollback the recordId
                getRecordIdGenerator().notifyHighestJournalRecordId( markedRecordId );
            }
            finally
            {
                synchronizer.countDown();
            }
        }

        public JournalException getCause()
        {
            return journalException;
        }
    }

}
