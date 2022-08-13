using Amazon.Lambda.Core;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Text;

namespace Capsulemon.Scheduler.DailyCrm
{
    public class App
    {
        NpgsqlConnection crmDbConnection;

        delegate void Command(NpgsqlCommand cmd, DateTime date);
        List<Command> commands = new List<Command>();
        List<DateTime> dates = new List<DateTime>();
        List<string> strCommands = new List<string>();
        bool isVacuum = false;

        public App(Func<string, IDbConnection> dbConnection_)
        {
            crmDbConnection = (NpgsqlConnection)dbConnection_("CrmDb");
        }

        public App Init(bool isVacuum_ = false, List<DateTime> dates_ = null, List<string> strCommands_ = null)
        {
            dates = dates_ ?? new List<DateTime> { DateTime.UtcNow.AddDays(-1).Date };
            isVacuum = isVacuum_;

            strCommands = strCommands_ ?? new List<string>();

            if (dates.Count < 1)
            {
                dates.Add(DateTime.UtcNow.AddDays(-1).Date);
            }
            if (strCommands.Count > 0)
            {
                foreach (string strCommand in strCommands)
                {
                    MethodInfo method = typeof(App).GetMethod(strCommand, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
                    Command command = (Command)Delegate.CreateDelegate(typeof(Command), this, method);
                    commands.Add(command);
                }
            }
            else
            {
                // 코딩된 순서대로 실행된다.
                foreach (MethodInfo method in this.GetType().GetMethods(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                {
                    try
                    {
                        Command command = (Command)Delegate.CreateDelegate(typeof(Command), this, method);
                        commands.Add(command);
                    }
                    catch
                    {
                        continue;
                    }
                }
            }
            return this;
        }

        public void Run()
        {
            crmDbConnection.Open();
            if (isVacuum)
            {
                using (var cmd = new NpgsqlCommand())
                {
                    // timeout 신경써야 한다.
                    cmd.Connection = crmDbConnection;
                    cmd.CommandTimeout = 1200;
                    cmd.CommandText =
"   delete from public.gamelogs where time < dateadd(mm,-3,getdate());" +
"   vacuum reindex public.gamelogs;" +
"   analyze public.gamelogs;";
                    cmd.ExecuteNonQueryAsync();
                    LambdaLogger.Log("[DailyCrm] Finish");
                    return;
                }
            }
            else
            {
                foreach (DateTime date in dates)
                {
                    LambdaLogger.Log("[DailyCrm] " + date.ToString("yyyy-MM-dd"));
                    using (var cmd = new NpgsqlCommand())
                    {
                        cmd.Connection = crmDbConnection;
                        cmd.CommandTimeout = 60;
                        foreach (var command in commands)
                        {
                            LambdaLogger.Log("[DailyCrm] " + command.GetMethodInfo().Name);
                            command(cmd, date);
                            cmd.Parameters.Clear();
                        }
                        LambdaLogger.Log("[DailyCrm] Finish");
                    }
                }
            }
            crmDbConnection.Close();
        }

        private void AccountDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_account_daily where date = @date1;" +
"   insert into public.crm_account_daily" +
"   select @date1 as date," +
"       a.oidaccount," +
"       max(json_extract_path_text(a.payload,'mysticorblevel')::int2) as arenalevel," +
"       trunc(b.created)=@date1 as isnew," +
"       c.country" +
"   from public.gamelogs a" +
"   inner join rds_footprint b" +
"   on a.oidaccount=b.oidaccount" +
"   inner join rds_staticaccount c" +
"   on a.oidaccount=c.oidaccount" +
"   where a.time between @date1 and @date2 and a.command = 'Login'" +
//"   and json_extract_path_text(payload,'mysticorblevel')::int2 > 0" +
"   group by a.oidaccount, trunc(b.created), c.country;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();

            cmd.CommandText =
"   delete public.crm_account_daily where date = @date1 and isnew and oidaccount in (" +
"   select oidaccount" +
"   from public.gamelogs" +
"   where time between @date1 and @date2 and command = 'Transfer'" +
"   );";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void PaidAccountDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_paid_account_daily where date = @date1;" +
"   insert into public.crm_paid_account_daily" +
            //"   select @date1 as date," +
            //"       a.oidAccount," +
            //"       count(*) as cnt," +
            //"       sum(b.price) as amount" +
            //"   from public.gamelogs a" +
            //"   left outer join public.static_product b" +
            //"   on json_extract_path_text(a.payload,'productid')=b.productid" +
            //"   and b.type = 'Cash'" +
            //"   where a.time between @date1 and @date2 and a.command = 'Shop:Buy'" +
            //"   and json_extract_path_text(a.payload,'paytype') = 'Cash'" +
            //"   group by a.oidAccount;";
"   select @date1 as date," +
"       a.oidAccount," +
"       count(*) as cnt," +
"       sum(json_extract_path_text(a.payload,'price')::integer) as amount" +
"   from public.gamelogs a" +
"   where a.time between @date1 and @date2 and a.command = 'Shop:Buy'" +
"   and json_extract_path_text(a.payload,'paytype') = 'Cash'" +
"   group by a.oidAccount;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void ShopProductDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_shop_product_daily where date = @date1;" +
"   insert into public.crm_shop_product_daily" +
//"   select @date1 as date," +
//"       json_extract_path_text(a.payload,'paytype') as paytype," +
//"       json_extract_path_text(a.payload,'productid')::integer as productid," +
//"       count(*) as cnt," +
//"       sum(b.price) as amount" +
//"   from public.gamelogs a" +
//"   left outer join public.static_product b" +
//"   on json_extract_path_text(a.payload,'productid')=b.productid" +
//"   where a.time between @date1 and @date2 and a.command = 'Shop:Buy'" +
//"   group by paytype,json_extract_path_text(a.payload,'productid');";
"   select @date1 as date," +
"       json_extract_path_text(a.payload,'paytype') as paytype," +
"       json_extract_path_text(a.payload,'productid')::integer as productid," +
"       count(*) as cnt," +
"       sum(json_extract_path_text(a.payload,'price')::integer) as amount" +
"   from public.gamelogs a" +
"   where a.time between @date1 and @date2 and a.command = 'Shop:Buy'" +
"   group by paytype,productid;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void ShopProductDaily_Country(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_shop_product_daily_country where date = @date1;" +
"   insert into public.crm_shop_product_daily_country" +
"   select @date1 as date," +
"       b.country," +
"       json_extract_path_text(a.payload,'paytype') as paytype," +
"       json_extract_path_text(a.payload,'productid')::integer as productid," +
"       count(*) as cnt," +
"       sum(json_extract_path_text(a.payload,'price')::integer) as amount" +
"   from public.gamelogs a" +
"   left outer join public.rds_staticaccount b on a.oidaccount=b.oidaccount" +
"   where a.time between @date1 and @date2 and a.command = 'Shop:Buy'" +
"   group by date, b.country, paytype, productid;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void RevenueDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_revenue_daily where date = @date1;" +
"   insert into public.crm_revenue_daily" +
"   select @date1 as date," +
"       b.country," +
"       count(*) as pu," +
"       isnull(sum(amount),0) as revenue" +
"   from public.crm_paid_account_daily a" +
"   left outer join public.rds_staticaccount b on a.oidaccount=b.oidaccount" +
"   where a.date = @date1" +
"   group by b.country";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.ExecuteNonQuery();
        }

        private void Dau(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_dau where date = @date;" +
"   insert into public.crm_dau" +
"   select @date as date," +
"       arenalevel," +
"       count(*) as dau" +
"   from public.crm_account_daily" +
"   where date = @date" +
"   group by arenalevel;";
            cmd.Parameters.AddWithValue("date", date);
            cmd.ExecuteNonQuery();
        }

        private void BasicDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_basic_daily where date = @date1;" +
"   insert into public.crm_basic_daily" +
"   select @date1 as date," +
"       a.ru," +
"       a.nru," +
"       b.dau," +
"       b.dau_p," +
"       c.pu as dpu," +
"       c.revenue," +
"       d.mcu" +
"   from" +
"   (" +
"       select" +
"           count(*) as ru," +
"           count(case when created between @date1 and @date2 then oidaccount end) as nru" +
"       from public.rds_footprint" +
"       where created < @date2" +
"   ) a," +
"   (" +
"       select" +
"           isnull(sum(dau),0) as dau," +
"           isnull(sum(case when arenalevel>0 then dau end),0) as dau_p" +
"       from public.crm_dau where date = @date1" +
"    ) b," +
"    (" +
"       select" +
"           isnull(sum(pu),0) as pu," +
"           isnull(sum(revenue),0) as revenue" +
"       from public.crm_revenue_daily" +
"       where date = @date1" +
"   ) c," +
"   (" +
"       select" +
"           max(ccu) as mcu" +
"           from public.crm_ccu" +
"       where time between @date1 and @date2" +
"   ) d";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void BasicDaily_Country(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_basic_daily_country where date = @date1;" +
"   insert into crm_basic_daily_country" +
"   select @date1 as date," +
"       isnull(aa.country,bb.country) as country," +
"       isnull(bb.nru, 0) as nru," +
"       isnull(aa.dau, 0) as dau," +
"       isnull(aa.dau_p, 0) as dau_p," +
"       isnull(cc.pu, 0) as dpu," +
"       isnull(cc.revenue, 0) as revenue" +
"   from (" +
"   select" +
"       b.country," +
"       count(*) as dau," +
"       count(case when arenalevel > 0 then 1 end) as dau_p" +
"   from public.crm_account_daily a" +
"   left outer join public.rds_staticaccount b on a.oidaccount=b.oidaccount" +
"   where a.date = @date1" +
"   group by b.country" +
"   ) aa" +
"   full outer join(" +
"   select" +
"       b.country," +
"       count(*) as nru" +
"   from public.rds_footprint a" +
"   left outer join public.rds_staticaccount b on a.oidaccount=b.oidaccount" +
"   where a.created between @date1 and @date2" +
"   group by b.country" +
"   ) bb on aa.country=bb.country" +
"   left outer join(" +
"   select" +
"       country," +
"       pu," +
"       revenue" +
"   from    public.crm_revenue_daily" +
"   where date = @date1" +
"   ) cc on aa.country=cc.country";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void BasicWeekly(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_basic_weekly where date = @date1;" +
"   insert into public.crm_basic_weekly" +
"   select @date1 as date," +
"       a.nru," +
"       b.au," +
"       c.pu," +
"       c.revenue" +
"   from" +
"   (" +
"       select" +
"           isnull(sum(nru),0) as nru" +
"       from public.crm_basic_daily" +
"       where date between @date1 and @date2" +
"   ) a," +
"   (" +
"       select" +
"           count(distinct oidaccount) as au" +
"       from public.crm_account_daily" +
"       where date between @date1 and @date2" +
"    ) b," +
"    (" +
"       select" +
"           isnull(sum(pu),0) as pu," +
"           isnull(sum(revenue),0) as revenue" +
"       from public.crm_revenue_daily" +
"       where date between @date1 and @date2" +
"   ) c";
            cmd.Parameters.AddWithValue("date1", date.FirstDayOfWeek(DayOfWeek.Sunday));
            cmd.Parameters.AddWithValue("date2", date.LastDayOfWeek(DayOfWeek.Sunday));
            cmd.ExecuteNonQuery();
        }

        private void BasicMonthly(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_basic_monthly where date = @date1;" +
"   insert into public.crm_basic_monthly" +
"   select @date1 as date," +
"       a.nru," +
"       b.au," +
"       c.pu," +
"       c.revenue" +
"   from" +
"   (" +
"       select" +
"           isnull(sum(nru),0) as nru" +
"       from public.crm_basic_daily" +
"       where date between @date1 and @date2" +
"   ) a," +
"   (" +
"       select" +
"           count(distinct oidaccount) as au" +
"       from public.crm_account_daily" +
"       where date between @date1 and @date2" +
"    ) b," +
"    (" +
"       select" +
"           isnull(sum(pu),0) as pu," +
"           isnull(sum(revenue),0) as revenue" +
"       from public.crm_revenue_daily" +
"       where date between @date1 and @date2" +
"   ) c";
            cmd.Parameters.AddWithValue("date1", date.FirstDayOfMonth());
            cmd.Parameters.AddWithValue("date2", date.LastDayOfMonth());
            cmd.ExecuteNonQuery();
        }

        private void BasicWeekly_Country(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_basic_weekly_country where date = @date1;" +
"   insert into public.crm_basic_weekly_country" +
"   select @date1 as date," +
"       a.country," +
"       a.nru," +
"       isnull(b.au, 0) as au," +
"       isnull(c.pu, 0) as pu," +
"       isnull(c.revenue, 0) as revenue" +
"   from" +
"   (" +
"       select" +
"           country," +
"           isnull(sum(nru), 0) as nru" +
"       from public.crm_basic_daily_country" +
"       where date between @date1 and @date2" +
"       group by country" +
"   ) a" +
"   left outer join(" +
"       select" +
"           country," +
"           count(distinct oidaccount) as au" +
"       from public.crm_account_daily" +
"       where date between @date1 and @date2" +
"       group by country" +
"    ) b on a.country=b.country" +
"    left outer join(" +
"       select" +
"           country," +
"           sum(pu) as pu," +
"           sum(revenue) as revenue" +
"       from public.crm_revenue_daily" +
"       where date between @date1 and @date2" +
"       group by country" +
"   ) c on a.country=c.country";
            cmd.Parameters.AddWithValue("date1", date.FirstDayOfWeek(DayOfWeek.Sunday));
            cmd.Parameters.AddWithValue("date2", date.LastDayOfWeek(DayOfWeek.Sunday));
            cmd.ExecuteNonQuery();
        }

        private void BasicMonthly_Country(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_basic_monthly_country where date = @date1;" +
"   insert into public.crm_basic_monthly_country" +
"   select @date1 as date," +
"       a.country," +
"       a.nru," +
"       isnull(b.au, 0) as au," +
"       isnull(c.pu, 0) as pu," +
"       isnull(c.revenue, 0) as revenue" +
"   from" +
"   (" +
"       select" +
"           country," +
"           isnull(sum(nru), 0) as nru" +
"       from public.crm_basic_daily_country" +
"       where date between @date1 and @date2" +
"       group by country" +
"   ) a" +
"   left outer join(" +
"       select" +
"           country," +
"           count(distinct oidaccount) as au" +
"       from public.crm_account_daily" +
"       where date between @date1 and @date2" +
"       group by country" +
"    ) b on a.country=b.country" +
"    left outer join(" +
"       select" +
"           country," +
"           sum(pu) as pu," +
"           sum(revenue) as revenue" +
"       from public.crm_revenue_daily" +
"       where date between @date1 and @date2" +
"       group by country" +
"   ) c on a.country=c.country";
            cmd.Parameters.AddWithValue("date1", date.FirstDayOfMonth());
            cmd.Parameters.AddWithValue("date2", date.LastDayOfMonth());
            cmd.ExecuteNonQuery();
        }

        private void Retention(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_retention" +
"   where (date >= dateadd(d, -7, @date) or date = dateadd(d, -15, @date) or date = dateadd(d, -30, @date)) and date < @date" +
"       and day = datediff(d, date, @date);" +
"   insert into public.crm_retention" +
"   select" +
"       a.date as date," +
"       datediff(d, a.date, @date) as day," +
"       a.arenalevel," +
"       count(*) as cnt_o," +
"       count(b.oidAccount) as cnt," +
"       cnt::float4 / cnt_o as rate" +
"   from public.crm_account_daily a" +
"   left outer join public.crm_account_daily b" +
"   on a.oidaccount = b.oidAccount" +
"   and b.date = @date" +
"   where (a.date >= dateadd(d, -7, @date) or a.date = dateadd(d, -15, @date) or a.date = dateadd(d, -30, @date))" +
"   and a.date < @date" +
"   group by a.date, a.arenalevel;";
            cmd.Parameters.AddWithValue("date", date);
            cmd.ExecuteNonQuery();
        }

        private void Retention_NU(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_retention_nu" +
"   where (date >= dateadd(d, -7, @date) or date = dateadd(d, -15, @date) or date = dateadd(d, -30, @date)) and date < @date" +
"       and day = datediff(d, date, @date);" +
"   insert into public.crm_retention_nu" +
"   select" +
"       a.date as date," +
"       datediff(d, a.date, @date) as day," +
"       count(*) as cnt_o," +
"       count(b.oidAccount) as cnt," +
"       cnt::float4 / cnt_o as rate" +
"   from public.crm_account_daily a" +
"   left outer join public.crm_account_daily b" +
"   on a.oidaccount = b.oidAccount" +
"   and b.date = @date" +
"   where (a.date >= dateadd(d, -7, @date) or a.date = dateadd(d, -15, @date) or a.date = dateadd(d, -30, @date))" +
"   and a.date < @date and a.isnew" +
"   group by a.date;";
            cmd.Parameters.AddWithValue("date", date);
            cmd.ExecuteNonQuery();
        }

        private void Retention_NU_P(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_retention_nu_p" +
"   where (date >= dateadd(d, -7, @date) or date = dateadd(d, -15, @date) or date = dateadd(d, -30, @date)) and date < @date" +
"       and day = datediff(d, date, @date);" +
"   insert into public.crm_retention_nu_p" +
"   select" +
"       a.date as date," +
"       datediff(d, a.date, @date) as day," +
"       count(*) as cnt_o," +
"       count(b.oidAccount) as cnt," +
"       cnt::float4 / cnt_o as rate" +
"   from public.crm_account_daily a" +
"   left outer join public.crm_account_daily b" +
"   on a.oidaccount = b.oidAccount" +
"   and b.date = @date" +
"   where (a.date >= dateadd(d, -7, @date) or a.date = dateadd(d, -15, @date) or a.date = dateadd(d, -30, @date))" +
"   and a.date < @date and a.isnew and a.arenalevel>0" +
"   group by a.date;";
            cmd.Parameters.AddWithValue("date", date);
            cmd.ExecuteNonQuery();
        }

        private void Retention_Country(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_retention_country" +
"   where (date >= dateadd(d, -7, @date) or date = dateadd(d, -15, @date) or date = dateadd(d, -30, @date)) and date < @date" +
"       and day = datediff(d, date, @date);" +
"   insert into public.crm_retention_country" +
"   select" +
"       a.date as date," +
"       datediff(d, a.date, @date) as day," +
"       a.country," +
"       count(*) as cnt_o," +
"       count(b.oidAccount) as cnt," +
"       cnt::float4 / cnt_o as rate" +
"   from public.crm_account_daily a" +
"   left outer join public.crm_account_daily b" +
"   on a.oidaccount = b.oidAccount" +
"   and b.date = @date" +
"   where (a.date >= dateadd(d, -7, @date) or a.date = dateadd(d, -15, @date) or a.date = dateadd(d, -30, @date))" +
"   and a.date < @date and a.isnew" +
"   group by a.date, a.country;";
            cmd.Parameters.AddWithValue("date", date);
            cmd.ExecuteNonQuery();
        }

        private void Retention_Country_P(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_retention_country_p" +
"   where (date >= dateadd(d, -7, @date) or date = dateadd(d, -15, @date) or date = dateadd(d, -30, @date)) and date < @date" +
"       and day = datediff(d, date, @date);" +
"   insert into public.crm_retention_country_p" +
"   select" +
"       a.date as date," +
"       datediff(d, a.date, @date) as day," +
"       a.country," +
"       count(*) as cnt_o," +
"       count(b.oidAccount) as cnt," +
"       cnt::float4 / cnt_o as rate" +
"   from public.crm_account_daily a" +
"   left outer join public.crm_account_daily b" +
"   on a.oidaccount = b.oidAccount" +
"   and b.date = @date" +
"   where (a.date >= dateadd(d, -7, @date) or a.date = dateadd(d, -15, @date) or a.date = dateadd(d, -30, @date))" +
"   and a.date < @date and a.isnew and a.arenalevel>0" +
"   group by a.date, a.country;";
            cmd.Parameters.AddWithValue("date", date);
            cmd.ExecuteNonQuery();
        }

        private void MatchEloStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_match_elo_stats_daily where date = @date1;" +
"   insert into public.crm_match_elo_stats_daily" +
"   select @date1 as date," +
"       json_extract_path_text(payload, 'isaiarena') = 'true' as isai," +
"       json_extract_path_text(json_extract_path_text(payload, 'myinfo'), 'arenalevel')::int2 as arenalevel," +
"       count(*) as cnt," +
"       count(case when json_extract_path_text(payload, 'iswin') = 'true' then 1 end) as cnt_win" +
"   from public.gamelogs" +
"   where oidaccount>0 and time between @date1 and @date2 and command = 'ArenaEnd'" +
"   and json_extract_path_text(payload, 'matchtype')='ELO'" +
"   group by isai,arenalevel;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void MonsterStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_monster_stats_daily where date = @date1;" +
"   insert into public.crm_monster_stats_daily" +
"   select @date1 as date," +
"       json_extract_path_text(payload,'monsterid')::integer as monsterid," +
"       json_extract_path_text(payload, 'arenalevel')::int2 as arenalevel," +
"       case json_extract_path_text(payload,'isaiarena') when 'false' then false else true end as isai," +
"       'monster' as type," +
"       count(*) cnt," +
"       count(case when json_extract_path_text(payload, 'iswin') = 'true' then 1 end) cnt_win" +
"   from public.gamelogs" +
"   where oidaccount>0 and time between @date1 and @date2 and command = 'MonsterRating'" +
"   group by monsterid,arenalevel,isai" +
"   union select @date1 as date," +
"       json_extract_path_text(payload,'skillid')::integer as monsterid," +
"       json_extract_path_text(payload, 'arenalevel')::int2 as arenalevel," +
"       case json_extract_path_text(payload,'isaiarena') when 'false' then false else true end as isai," +
"       'skill' as type," +
"       count(*) cnt," +
"       count(case when json_extract_path_text(payload, 'iswin') = 'true' then 1 end) cnt_win" +
"   from public.gamelogs" +
"   where oidaccount>0 and time between @date1 and @date2 and command = 'SkillRating'" +
"   group by monsterid,arenalevel,isai;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void PlayTurnStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_playturn_stats_daily where date = @date1;" +
"   insert into public.crm_playturn_stats_daily" +
"   select @date1 as date," +
"       json_extract_path_text(json_extract_path_text(payload, 'myinfo'), 'arenalevel')::int2 as arenalevel," +
"       json_extract_path_text(payload,'isaiarena')='true' as isai," +
"       json_extract_path_text(payload,'matchtype') as matchtype," +
"       count(*) as cnt," +
"       sum(json_extract_path_text(payload, 'turncount')::int2) as sum_turn," +
"       sum(json_extract_path_text(payload, 'totalplaytime')::integer) as sum_playtime," +
"       max(json_extract_path_text(payload, 'turncount')::int2) as max_turn," +
"       max(json_extract_path_text(payload, 'totalplaytime')::integer) as max_playtime," +
"       min(json_extract_path_text(payload, 'turncount')::int2) as min_turn," +
"       min(json_extract_path_text(payload, 'totalplaytime')::integer) as min_playtime" +
"   from public.gamelogs" +
"   where oidAccount>0 and time between @date1 and @date2 and command='ArenaEnd'" +
"       and json_extract_path_text(payload,'totalplaytime')::integer>60" +
"   group by arenalevel, isai, matchtype;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void CubeMakeStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_cube_make_stats_daily where date = @date1;" +
"   insert into public.crm_cube_make_stats_daily" +
"   with a as (" +
"   select" +
"       json_extract_path_text(payload, 'cubegrade') as grade," +
"       json_extract_path_text(payload, 'useJewel') = 'true' as jewel" +
"   from public.gamelogs" +
"   where time between @date1 and @date2 and command = 'Cube:Make'" +
"   )" +
"   select @date1 as date," +
"       grade," +
"       count(*) as cnt," +
"       count(case when jewel then 1 end) as cnt_jewel" +
"   from a" +
"   group by grade;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void JewelStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_jewel_stats_daily where date = @date1;" +
"   insert into public.crm_jewel_stats_daily" +
"   select @date1 as date," +
"       command," +
"       json_extract_path_text(payload,'reason') as reason," +
"       count(*) as cnt," +
"       sum(json_extract_path_text(payload,'delta')::integer) as amount" +
"   from public.gamelogs" +
"   where time between @date1 and @date2 and command in ('Jewel:Gain', 'Jewel:Spend')" +
"   group by command, reason;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void CardWeightStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_cardweight_winningrate_daily where date = @date1;" +
"   insert into public.crm_cardweight_winningrate_daily" +
"   select @date1 as date," +
"       cardweight_my-cardweight_op as diff," +
"       count(*) as cnt," +
"       count(case when iswin='true' then 1 end) as cnt_win" +
"   from (" +
"   select" +
"       oidaccount," +
"       json_extract_path_text(payload, 'iswin') as iswin," +
"       json_extract_path_text(json_extract_path_text(payload, 'myinfo'), 'cardweight')::int2 as cardweight_my," +
"       json_extract_path_text(json_extract_path_text(payload, 'opinfo'), 'cardweight')::int2 as cardweight_op" +
"   from gamelogs" +
"   where time between @date1 and @date2 and command='ArenaEnd'" +
"       and json_extract_path_text(payload, 'matchtype') in ('ELO')" +
"       and json_extract_path_text(payload, 'isaiarena')='false'" +
"   )" +
"   group by diff;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();

            cmd.CommandText =
"   delete public.crm_cardweight_distribution_daily where date = @date1;" +
"   insert into public.crm_cardweight_distribution_daily" +
"   select @date1 as date," +
"       arenalevel," +
"       cardweight," +
"       count(*) as cnt" +
"   from (" +
"   select" +
"       oidaccount," +
"       json_extract_path_text(payload, 'matchedtier')::int2 as arenalevel," +
"       json_extract_path_text(json_extract_path_text(payload, 'myinfo'), 'cardweight')::int2 as cardweight" +
"   from gamelogs" +
"   where time between @date1 and @date2 and command='ArenaEnd'" +
"       and json_extract_path_text(payload, 'matchtype') in ('ELO')" +
"       and json_extract_path_text(payload, 'isaiarena')='false'" +
"   )" +
"   group by arenalevel, cardweight;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();

            cmd.CommandText =
"   delete public.crm_cardweight_stats_daily where date = @date1;" +
"   insert into public.crm_cardweight_stats_daily" +
"   select @date1 as date," +
"       arenalevel," +
"       count(*) as cnt," +
"       avg(cardweight::float) as avg," +
"       median(cardweight::float) as median," +
"       stddev_pop(cardweight::float) as stddev," +
"       min(cardweight) as min," +
"       max(cardweight) as max" +
"   from (" +
"   select" +
"       oidaccount," +
"       json_extract_path_text(payload, 'matchedtier')::int2 as arenalevel," +
"       json_extract_path_text(json_extract_path_text(payload, 'myinfo'), 'cardweight')::int2 as cardweight" +
"   from gamelogs" +
"   where time between @date1 and @date2 and command='ArenaEnd'" +
"       and json_extract_path_text(payload, 'matchtype') in ('ELO')" +
"       and json_extract_path_text(payload, 'isaiarena')='false'" +
"   )" +
"   group by arenalevel;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void HardMatchStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_hardmatch_stats_daily where date = @date1;" +
"   insert into public.crm_hardmatch_stats_daily" +
"   select @date1 as date," +
"       json_extract_path_text(payload, 'arenalevel')::int2 as arenalevel," +
"       json_extract_path_text(payload, 'difficulttype') as difficulttype," +
"       count(*) as cnt_match," +
"       sum(json_extract_path_text(payload, 'hitbonuscount')::int2) as total_bonus" +
"   from public.gamelogs" +
"   where oidaccount>0 and time between @date1 and @date2 and command = 'Arena:HardMatch'" +
"   group by arenalevel, difficulttype;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }

        private void MatchEloBandStatsDaily(NpgsqlCommand cmd, DateTime date)
        {
            cmd.CommandText =
"   delete public.crm_match_eloband_stats_daily where date = @date1;" +
"   insert into public.crm_match_eloband_stats_daily" +
"   select" +
"   @date1 as date," +
"	isai," +
"	case when elo_my <= 2000 then 2000 else elo_my / 100 * 100 end as eloband_my," +
"	case when elo_op <= 2000 then 2000 else elo_op / 100 * 100 end as eloband_op," +
"	count(*) as cnt" +
"   from (" +
"   select" +
"       json_extract_path_text(payload, 'isaiarena') = 'true' as isai," +
"       json_extract_path_text(json_extract_path_text(payload, 'myinfo'), 'arenapoint')::int2 as elo_my," +
"	    json_extract_path_text(json_extract_path_text(payload, 'opinfo'), 'arenapoint')::int2 as elo_op" +
"   from public.gamelogs" +
"   where oidaccount>0 and time between @date1 and @date2 and command = 'ArenaEnd'" +
"   )" +
"   group by date, isai, eloband_my, eloband_op;";
            cmd.Parameters.AddWithValue("date1", date);
            cmd.Parameters.AddWithValue("date2", date.AddDays(1));
            cmd.ExecuteNonQuery();
        }
    }
}
