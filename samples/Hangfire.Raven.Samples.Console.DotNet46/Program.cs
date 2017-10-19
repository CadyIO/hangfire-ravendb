using System;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.Samples.Console.DotNet46
{
    class Program
    {
        public static int x = 0;

        public static void Main()
        {
            try
            {
                // you can use Raven Storage and specify the connection string name
                GlobalConfiguration.Configuration
                    .UseColouredConsoleLogProvider()
                    .UseRavenStorage("RavenDebug");

                // you can use Raven Storage and specify the connection string and database name
                //GlobalConfiguration.Configuration
                //    .UseColouredConsoleLogProvider()
                //    .UseRavenStorage("http://localhost:9090", "HangfireConsole");

                // you can use Raven Embedded Storage which runs in memory!
                //GlobalConfiguration.Configuration
                //    .UseColouredConsoleLogProvider()
                //    .UseEmbeddedRavenStorage();

                //you have to create an instance of background job server at least once for background jobs to run
                var client = new BackgroundJobServer();

                // Run once
                BackgroundJob.Enqueue(() => System.Console.WriteLine("Background Job: Hello, world!"));

                BackgroundJob.Enqueue(() => Test());

                // Run every minute
                RecurringJob.AddOrUpdate(() => Test(), Cron.Minutely);

                System.Console.WriteLine("Press Enter to exit...");
                System.Console.ReadLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void Test()
        {
            System.Console.WriteLine($"{x++} Cron Job: Hello, world!");
            //throw new ArgumentException("fail");
        }
    }
}
