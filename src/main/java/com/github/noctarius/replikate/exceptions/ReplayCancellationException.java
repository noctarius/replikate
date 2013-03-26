package com.github.noctarius.replikate.exceptions;

public class ReplayCancellationException
    extends JournalException
{

    private static final long serialVersionUID = 7361321634534376619L;

    public ReplayCancellationException()
    {
        super();
    }

    public ReplayCancellationException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public ReplayCancellationException( String message )
    {
        super( message );
    }

    public ReplayCancellationException( Throwable cause )
    {
        super( cause );
    }

}
