package com.github.noctarius.waljdbc;

public class JournalException
    extends RuntimeException
{

    private static final long serialVersionUID = 2480300208807598045L;

    JournalException()
    {
        super();
    }

    JournalException( String message, Throwable cause )
    {
        super( message, cause );
    }

    JournalException( String message )
    {
        super( message );
    }

    JournalException( Throwable cause )
    {
        super( cause );
    }

}
