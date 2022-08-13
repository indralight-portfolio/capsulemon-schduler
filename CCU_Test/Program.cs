using System;

namespace Capsulemon.Scheduler.Test
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enter Env (default=live-local) :  ");
            string env_ = Console.ReadLine();
            string env = string.IsNullOrEmpty(env_) ? "live-local" : env_;

            CCU.Program.Run(env);

            Console.WriteLine("done");
            Console.ReadLine();
        }
    }
}
