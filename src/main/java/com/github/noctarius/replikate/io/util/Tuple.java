package com.github.noctarius.replikate.io.util;

public class Tuple<FV, SV>
{

    private final FV value1;

    private final SV value2;

    public Tuple( FV value1, SV value2 )
    {
        this.value1 = value1;
        this.value2 = value2;
    }

    public FV getValue1()
    {
        return value1;
    }

    public SV getValue2()
    {
        return value2;
    }

}
