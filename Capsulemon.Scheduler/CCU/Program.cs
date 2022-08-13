using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Implementations;
using StackExchange.Redis.Extensions.Newtonsoft;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Capsulemon.Scheduler.CCU
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
            var redisStatusConfiguration = Configuration.GetSection("RedisStatus").Get<RedisConfiguration>();
            redisStatusConfiguration.ConfigurationOptions.CertificateValidation += CheckServerCertificate;
            services.AddSingleton<ISerializer, NewtonsoftSerializer>();
            services.AddScoped<IStatusCache, StatusCache>(
                provider => new StatusCache(new RedisCacheConnectionPoolManager(redisStatusConfiguration), provider.GetService<ISerializer>(), redisStatusConfiguration)
            );
            services.AddScoped<Func<string, IDbConnection>>(
                provider => db => new NpgsqlConnection(Configuration.GetConnectionString(db))
            );

            services.AddSingleton(Configuration);
            services.AddTransient<App>();
        }

        private static bool CheckServerCertificate(object sender, X509Certificate certificate,
           X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            // the lazy version here is:
            return true;

            // better version - check that the CA thumbprint is in the chain
            //if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
            //{
            //    // check that the untrusted ca is in the chain
            //    var ca = new X509Certificate2("ca.pem");
            //    var caFound = chain.ChainElements
            //        .Cast<X509ChainElement>()
            //        .Any(x => x.Certificate.Thumbprint == ca.Thumbprint);

            //    // note you could also hard-code the expected CA thumbprint,
            //    // but pretty easy to load it from the pem file that aiven provide

            //    return caFound;
            //}
            //return false;
        }
    }
}
