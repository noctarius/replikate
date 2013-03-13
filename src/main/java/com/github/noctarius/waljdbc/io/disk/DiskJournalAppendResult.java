package com.github.noctarius.waljdbc.io.disk;

public enum DiskJournalAppendResult
{

    /** Journal entry was successfully appended */
    APPEND_SUCCESSFUL,

    /** Journal entry is too large for a standard journal, building an overflow-journal */
    JOURNAL_FULL_OVERFLOW,

    /** Journal entry is too large, building a new journal file */
    JOURNAL_OVERFLOW

}
