using System;
using System.Collections.Generic;
using System.Globalization;

namespace Capsulemon.Scheduler.Test
{
    class Program
    {
        const string pattern = "yyyyMMddHHmm";
        const int interval_default = 60;

        static void Main(string[] args)
        {
            Console.Write("Enter Env (default=dev) :  ");
            string env = Console.ReadLine();

            env = string.IsNullOrEmpty(env) ? "dev" : env;

            int interval = interval_default;
            Console.Write($"Enter interval (default={interval_default}): ");
            int.TryParse(Console.ReadLine(), out interval);
            interval = interval == 0 ? interval_default : interval;

            Console.Write($"Enter Start ({pattern}): ");
            string start_ = Console.ReadLine();
            DateTime start, end;
            List<string> datehours = null;
            if (DateTime.TryParseExact(start_, pattern, null, DateTimeStyles.None, out start))
            {
                Console.Write($"Enter End ({pattern}): ");
                string end_ = Console.ReadLine();
                DateTime.TryParseExact(end_, pattern, null, DateTimeStyles.None, out end);
                do
                {
                    datehours = datehours ?? new List<string>();
                    datehours.Add(start.ToString(pattern));
                    start = start.AddMinutes(interval);
                } while (start <= end);
            }

            ReplayPick.Program.Run(env, interval, datehours);

            Console.WriteLine("done");
            Console.ReadLine();
        }
    }
}
