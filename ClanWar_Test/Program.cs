using System;

namespace Capsulemon.Scheduler.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enter Env (default=dev) :  ");
            string env_ = Console.ReadLine();
            string env = string.IsNullOrEmpty(env_) ? "dev" : env_;

            ClanWar.Program.Run(env);

            Console.WriteLine("done");
            Console.ReadLine();
        }
    }
}
