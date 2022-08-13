using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Capsulemon.Scheduler
{
    public class Function
    {
        public string FunctionHandler(JObject input, ILambdaContext context)
        {
            LambdaLogger.Log(input.ToString());

            string function = Environment.GetEnvironmentVariable("Function");
            if (function == "CCU")
            {
                string env = input["Env"].ToString();

                CCU.Program.Run(env);
            }
            else if (function == "ClanWar")
            {
                string env = input["Env"].ToString();

                ClanWar.Program.Run(env);
            }
            else if (function == "DailyCrm")
            {
                string env = input["Env"].ToString();
                bool isVacuum = (input["isVacuum"] != null && input["isVacuum"].ToString() == "true");

                DailyCrm.Program.Run(env, isVacuum);
            }
            else if (function == "ReplayPick")
            {
                const int interval_default = 60;
                string env = input["Env"].ToString();
                int interval;
                int.TryParse((input["Interval"] ?? "0").ToString(), out interval);
                interval = interval == 0 ? interval_default : interval;

                ReplayPick.Program.Run(env, interval);
            }

            return function;
        }
    }
}
