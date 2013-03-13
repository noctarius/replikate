package com.github.noctarius.waljdbc.spi;

public interface JournalRecordIdGenerator
{

    long nextRecordId();

    long lastGeneratedRecordId();

}
