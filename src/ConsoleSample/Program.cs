using System;
using Hangfire;
using HangFire.Raven.Storage;

namespace ConsoleSample
{
    public static class Program
    {
        public static int x = 0;

        public static void Main()
        {
            // you can use Raven Storage and specify the connection string and database name
            GlobalConfiguration.Configuration
                .UseColouredConsoleLogProvider()
                .UseRavenStorage("http://localhost:8080", "hangfire", "apikeytest");

            // you can use Raven Embedded Storage which runs in memory!
            //GlobalConfiguration.Configuration
            //    .UseColouredConsoleLogProvider()
            //    .UseEmbeddedRavenStorage();

            //you have to create an instance of background job server at least once for background jobs to run
            var client = new BackgroundJobServer();

            //BackgroundJob.Enqueue(() => Console.WriteLine("Background Job: Hello, world!"));
            BackgroundJob.Enqueue(() => test());
            //RecurringJob.AddOrUpdate(() => test(), Cron.Minutely);

            Console.WriteLine("Press Enter to exit...");
            Console.ReadLine();
        }

        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void test()
        {
            Console.WriteLine($"{x++} Cron Job: Hello, world!");
            throw new ArgumentException("fail");
        }
    }
}