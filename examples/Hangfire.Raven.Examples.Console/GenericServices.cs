﻿using System;

namespace Hangfire.Raven.Examples.Console
{
    public class GenericServices<TType>
    {
        public void Method<TMethod>(TType arg1, TMethod arg2)
        {
            System.Console.WriteLine("Arg1: {0}, Arg2: {1}", arg1, arg2);
        }
    }
}