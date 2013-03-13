package com.github.noctarius.waljdbc.io.disk;

class JournalFileHeader
{

    static final byte[] MAGIC_NUMBER = { (byte) 0xFE, (byte) 0xEE, (byte) 0xEE, (byte) 0xEF };

    private final int version;

    private final int maxLogFileSize;

    private final long logFileNumber;

    private final byte type;

    private final int firstDataOffset;

    JournalFileHeader( int version, int maxLogFileSize, long logFileNumber, byte type )
    {
        this( version, maxLogFileSize, logFileNumber, type, DiskJournal.JOURNAL_FILE_HEADER_SIZE );
    }

    JournalFileHeader( int version, int maxLogFileSize, long logFileNumber, byte type, int firstDataOffset )
    {
        this.version = version;
        this.maxLogFileSize = maxLogFileSize;
        this.logFileNumber = logFileNumber;
        this.type = type;
        this.firstDataOffset = firstDataOffset;
    }

    public int getVersion()
    {
        return version;
    }

    public int getMaxLogFileSize()
    {
        return maxLogFileSize;
    }

    public long getLogNumber()
    {
        return logFileNumber;
    }

    public byte getType()
    {
        return type;
    }

    public int getFirstDataOffset()
    {
        return firstDataOffset;
    }

}
