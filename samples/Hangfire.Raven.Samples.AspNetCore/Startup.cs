using System.Diagnostics;
using System.Threading.Tasks;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raven.Samples.AspNetCore {
    public class Startup {
        public Startup(IHostingEnvironment env) {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services) {
            // Add framework services.
            services.AddMvc();

            // Add Hangfire to services using RavenDB Storage
            // BUG: https://github.com/cady-io/hangfire-ravendb/issues/15
            //services.AddHangfire(t => t.UseRavenStorage(Configuration["ConnectionStrings:RavenDebug"]));

            services.AddHangfire(t => t.UseRavenStorage(Configuration["ConnectionStrings:RavenDebugUrl"], Configuration["ConnectionStrings:RavenDebugDatabase"]));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory) {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            if (env.IsDevelopment()) {
                app.UseDeveloperExceptionPage();
                app.UseBrowserLink();
            } else {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();

            // Add Hangfire Server and Dashboard support
            app.UseHangfireServer(new BackgroundJobServerOptions() { Queues = new[] { "Default", "Testing" } });
            app.UseHangfireDashboard();

            // Run once
            BackgroundJob.Enqueue(() => System.Console.WriteLine("Background Job: Hello, world!"));

            BackgroundJob.Enqueue(() => Test());

            BackgroundJob.Schedule(() => System.Console.WriteLine("Scheduled Job: Hello, I am delayed world!"), new System.TimeSpan(0, 1, 0));

            // Run every minute
            RecurringJob.AddOrUpdate(() => Test(), Cron.Minutely);

            Task.Run(() => {
                for (int i = 0; i < 100; i++)
                    BackgroundJob.Enqueue(() => System.Console.WriteLine("Background Job: Hello stressed world!"));

            app.UseMvc(routes => {
                routes.MapRoute(
                    name: "default",
                    template: "{controller=Home}/{action=Index}/{id?}");
            });
        }

        public static int x = 0;

        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void Test() {
            Debug.WriteLine($"{x++} Cron Job: Hello, world!");
            //throw new ArgumentException("fail");
        }
    }
}
