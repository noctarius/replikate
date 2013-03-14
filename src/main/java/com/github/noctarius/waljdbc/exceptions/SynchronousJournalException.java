package com.github.noctarius.waljdbc.exceptions;

public class SynchronousJournalException
    extends JournalException
{

    private static final long serialVersionUID = 2431547060197865753L;

    public SynchronousJournalException()
    {
        super();
    }

    public SynchronousJournalException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public SynchronousJournalException( String message )
    {
        super( message );
    }

    public SynchronousJournalException( Throwable cause )
    {
        super( cause );
    }

}
