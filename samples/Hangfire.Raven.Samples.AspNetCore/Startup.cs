using System.Diagnostics;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raven.Samples.AspNetCore
{
    public class Startup
    {
        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // Add framework services.
            services.AddMvc();

            // Add Hangfire to services using RavenDB Storage
            // BUG: https://github.com/cady-io/hangfire-ravendb/issues/15
            //services.AddHangfire(t => t.UseRavenStorage(Configuration["ConnectionStrings:RavenDebug"]));

            services.AddHangfire(t => t.UseRavenStorage(Configuration["ConnectionStrings:RavenDebugUrl"], Configuration["ConnectionStrings:RavenDebugDatabase"]));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseBrowserLink();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();

            // Add Hangfire Server and Dashboard support
            app.UseHangfireServer();
            app.UseHangfireDashboard();

            // Run once
            BackgroundJob.Enqueue(() => System.Console.WriteLine("Background Job: Hello, world!"));

            BackgroundJob.Enqueue(() => Test());

            // Run every minute
            RecurringJob.AddOrUpdate(() => Test(), Cron.Minutely);

            app.UseMvc(routes =>
            {
                routes.MapRoute(
                    name: "default",
                    template: "{controller=Home}/{action=Index}/{id?}");
            });
        }

        public static int x = 0;

        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void Test()
        {
            Debug.WriteLine($"{x++} Cron Job: Hello, world!");
            //throw new ArgumentException("fail");
        }
    }
}
