using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;

namespace Capsulemon.Scheduler.DailyCrm
{
    public class Program
    {
        static IConfiguration Configuration;

        private static IServiceCollection GetServices(string env)
        {
            string appName = typeof(Program).Namespace.Split(".").Last();
            Configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile($"{appName}/appsettings.json")
                    .AddJsonFile($"{appName}/appsettings.{env}.json", optional: true)
                    .Build();

            IServiceCollection services = new ServiceCollection();
            ConfigureServices(services);
            return services;
        }

        public static void Run(string env, bool isVacuum = true, List<DateTime> dates = null, List<string> strCommands = null)
        {
            IServiceCollection services = GetServices(env);
            var serviceProvider = services.BuildServiceProvider();
            serviceProvider.GetService<App>()
                .Init(isVacuum, dates, strCommands)
                .Run();
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            services.AddScoped<Func<string, IDbConnection>>(
                provider => db => new NpgsqlConnection(Configuration.GetConnectionString(db))
            );
                        
            services.AddSingleton(Configuration);
            services.AddTransient<App>();
        }
    }
}
