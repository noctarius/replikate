package com.noctarius.replikate.spi;

public interface JournalRecordIdGenerator
{

    long nextRecordId();

    long lastGeneratedRecordId();

    void notifyHighestJournalRecordId( long recordId );

}
