package com.github.noctarius.waljdbc.spi;

import com.github.noctarius.waljdbc.Journal;
import com.github.noctarius.waljdbc.JournalEntry;
import com.github.noctarius.waljdbc.exceptions.JournalException;

public interface JournalFlushedListener<V>
{

    void flushed( JournalEntry<V> entry );

    void failed( JournalEntry<V> entry, JournalException cause );

    /**
     * This callback method is called when during journal replaying a missing recordId or other suspicious events arise.<br>
     * The further behavior of the journal replay can be influenced using the callback's result.<br>
     * <ul>
     * <li>{@link ReplayNotificationResult#Continue} means that execution will be move on with the next record or finish
     * initialization of journal when replay is finished</li><br>
     * <li>{@link ReplayNotificationResult#Terminate} will silently finish the record reading and will only announce the
     * already read entries</li><br>
     * <li>Finally {@link ReplayNotificationResult#Except} will throw a {@link JournalException} and none of the already
     * read entries will be announced (same can be achieved by throwing an {@link RuntimeException} inside the callback)
     * </li>
     * </ul>
     * 
     * @param journal The journal that is read
     * @param lastEntry The previously read journal entry
     * @param currentEntry The currently read journal entry
     * @return Further execution behavior
     */
    ReplayNotificationResult replayNotifySuspiciousRecordId( Journal<V> journal, JournalEntry<V> lastEntry,
                                                             JournalEntry<V> currentEntry );

    /**
     * This method is called when during startup of the Journal implementation a previous journal was found and records
     * replayed. For every record in the existing journal the callback will be executed.<br>
     * The further behavior of the journal replay can be influenced using the callback's result.<br>
     * <ul>
     * <li>{@link ReplayNotificationResult#Continue} means that execution will be move on with the next record or finish
     * initialization of journal when replay is finished</li><br>
     * <li>{@link ReplayNotificationResult#Terminate} will silently finish the record reading</li><br>
     * <li>{@link ReplayNotificationResult#Except} will throw a {@link JournalException} (same can be achieved by
     * throwing an {@link RuntimeException} inside the callback)</li>
     * 
     * @param journal The journal that is read
     * @param entry The currently read journal entry
     * @return Further execution behavior
     */
    ReplayNotificationResult replayRecordId( Journal<V> journal, JournalEntry<V> entry );

}
