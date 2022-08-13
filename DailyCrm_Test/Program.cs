using System;
using System.Collections.Generic;
using System.Linq;

namespace Capsulemon.Scheduler.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enter Env (default=live-local) :  ");
            string env = Console.ReadLine();
            env = string.IsNullOrEmpty(env) ? "live-local" : env;
            List<DateTime> dates = null;
            string[] strCommands = null;

            Console.Write($"Enter Start (yyyy-MM-dd): ");
            string start_ = Console.ReadLine();
            DateTime start, end;
            if (DateTime.TryParse(start_, out start))
            {
                Console.Write($"Enter End (yyyy-MM-dd): ");
                string end_ = Console.ReadLine();
                DateTime.TryParse(end_, out end);
                if (end_ == ".") { end = DateTime.Today.AddDays(-1); }
                do
                {
                    dates = dates ?? new List<DateTime>();
                    dates.Add(start);
                    start = start.AddDays(1);
                    //start = start.AddDays(7);
                } while (start <= end);
            }

            strCommands = new string[]
            {
                //"AccountDaily",
                //"PaidAccountDaily",
                //"ShopProductDaily",
                //"ShopProductDaily_Country",
                //"Dau",
                //"BasicDaily",
                //"BasicDaily_Country",
                //"BasicWeekly",
                //"BasicMonthly",
                //"BasicWeekly_Country",
                //"BasicMonthly_Country",
                //"Retention",
                //"Retention_NU",
                //"Retention_NU_P",
                //"Retention_Country",
                //"Retention_Country_P",
                //"MatchEloStatsDaily",
                //"MonsterStatsDaily",
                //"PlayTurnStatsDaily",
                //"CubeMakeStatsDaily",
                //"JewelStatsDaily",
                //"CardWeightStatsDaily",
                //"HardMatchStatsDaily",
                //"MatchEloBandStatsDaily"
            };

            DailyCrm.Program.Run(env, false, dates, strCommands.ToList());

            Console.WriteLine("done");
            Console.ReadLine();
        }
    }
}
