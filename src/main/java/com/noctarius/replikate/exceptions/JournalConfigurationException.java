package com.noctarius.replikate.exceptions;

public class JournalConfigurationException
    extends JournalException
{

    private static final long serialVersionUID = 3841407745654517147L;

    public JournalConfigurationException()
    {
        super();
    }

    public JournalConfigurationException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public JournalConfigurationException( String message )
    {
        super( message );
    }

    public JournalConfigurationException( Throwable cause )
    {
        super( cause );
    }

}
