using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Extensions.NETCore.Setup;
using Amazon.Runtime;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Implementations;
using StackExchange.Redis.Extensions.Newtonsoft;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Capsulemon.Scheduler.ReplayPick
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

        public static void Run(string env, int interval = 60, List<string> datehours = null)
        {
            IServiceCollection services = GetServices(env);
            var serviceProvider = services.BuildServiceProvider();
            serviceProvider.GetService<App>()
                .Init(interval, datehours)
                .Run();
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            var awsConfiguration = Configuration.GetSection("AWS").Get<AWSConfig>();
            BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsConfiguration.AccessKey, awsConfiguration.SecretKey);
            var awsOptions = new AWSOptions
            {
                Credentials = awsCreds,
                Region = RegionEndpoint.GetBySystemName(awsConfiguration.Region),
            };
            services.AddDefaultAWSOptions(awsOptions);
            services.AddAWSService<IAmazonDynamoDB>();

            services.AddSingleton(Configuration);
            services.AddTransient<App>();
        }
    }
}
