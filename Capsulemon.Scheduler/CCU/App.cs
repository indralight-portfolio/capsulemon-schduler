using Amazon.Lambda.Core;
using Capsulemon.Scheduler.Models;
using Npgsql;
using StackExchange.Redis;
using StackExchange.Redis.Extensions;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Data;

namespace Capsulemon.Scheduler.CCU
{
    public class App
    {
        IRedisDatabase statusCache;
        IDatabase statusCacheDB;
        NpgsqlConnection crmDbConnection;

        public App(IStatusCache statusCache_, Func<string, IDbConnection> dbConnection_)
        {
            if (statusCache_ != null)
            {
                statusCache = statusCache_.GetDbFromConfiguration();
                statusCacheDB = statusCache.Database;
            }
            crmDbConnection = (NpgsqlConnection)dbConnection_("CrmDb");
        }
        public void Run()
        {
            int ccu = 0;
            DateTime dt = DateTime.UtcNow;
            {
                string pattern = "ServerDesc:*";
                IEnumerable<string> servers = statusCache.SearchKeysAsync(pattern).GetAwaiter().GetResult();
                foreach (string hashkey in servers)
                {
                    var entity = statusCacheDB.HashGetAll(hashkey).ToModel<GameServerDesc>();
                    if ((entity.dtmUpdated - dt).TotalSeconds <= 60)
                        ccu += entity.numCCU;
                }
            }

            LambdaLogger.Log("[CCU] get ccu from Redis. ccu = " + ccu);

            using (var cmd = new NpgsqlCommand())
            {
                crmDbConnection.Open();
                cmd.Connection = crmDbConnection;

                dt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0);

                cmd.CommandText =
"   delete from public.crm_ccu where time=@time;" +
"   insert into public.crm_ccu" +
"   select @time, @ccu;";
                cmd.Parameters.AddWithValue("time", dt);
                cmd.Parameters.AddWithValue("ccu", ccu);
                cmd.ExecuteNonQuery();

                crmDbConnection.Close();
            }

            LambdaLogger.Log("[CCU] insert ccu complete. dt = " + dt.ToString("yyyy-MM-dd HH:mm"));
        }
    }
}