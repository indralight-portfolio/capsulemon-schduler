using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.IO;
using System.Linq;

namespace Capsulemon.Scheduler.ClanWar
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

        public static void Run(string env)
        {
            IServiceCollection services = GetServices(env);
            var serviceProvider = services.BuildServiceProvider();
            serviceProvider.GetService<App>().Run();
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton(Configuration);
            services.AddTransient<App>();
        }
    }
}
