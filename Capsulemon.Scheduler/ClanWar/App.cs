

using Amazon.Lambda.Core;
using Microsoft.Extensions.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;

namespace Capsulemon.Scheduler.ClanWar
{
    public class App
    {
        IConfiguration configuration;
        string env { get; set; }

        public App(IConfiguration configuration_)
        {
            configuration = configuration_;
            env = configuration.GetValue<string>("Env");
        }
        public void Run()
        {
            using (HttpClient httpClient = new HttpClient())
            {
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var url = $"{getLobbyUrl(env)}/clanWar/checkScheduler";
                var httpResponse = httpClient.PostAsJsonAsync<object>(url, null).GetAwaiter().GetResult();
                var result = httpResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                LambdaLogger.Log($"[ClanWar] url: {url}, response: {httpResponse.StatusCode}, result: {result}");

                url = $"{getLobbyUrl(env)}/groupLeague/checkScheduler";
                httpResponse = httpClient.PostAsJsonAsync<object>(url, null).GetAwaiter().GetResult();
                result = httpResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult();

                LambdaLogger.Log($"[GroupLeague] url: {url}, response: {httpResponse.StatusCode}, result: {result}");
            }
        }

        private string getLobbyUrl(string env)
        {
            switch (env)
            {
                case "qa":
                case "review":
                case "live":
                    return $"http://{env}-api.capsulemon.com";
                default:
                    return $"http://{env}.capsulemon.com:8082";
            }
        }
    }
}