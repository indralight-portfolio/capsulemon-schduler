using System;
using System.Collections.Generic;
using System.Text;

namespace Capsulemon.Scheduler
{
    public static class DateTimeExtensions
    {
        public static DateTime FirstDayOfWeek(this DateTime datetime, DayOfWeek startOfWeek = DayOfWeek.Monday)
        {
            // https://ko.wikipedia.org/wiki/ISO_8601#Week_Dates
            // 주의 시작은 월요일

            int diff = (7 + (datetime.DayOfWeek - startOfWeek)) % 7;
            return datetime.AddDays(-1 * diff).Date;
        }

        public static DateTime LastDayOfWeek(this DateTime datetime, DayOfWeek startOfWeek = DayOfWeek.Monday)
        {
            int diff = (7 + (datetime.DayOfWeek - startOfWeek)) % 7;
            return datetime.AddDays(6 - diff).Date;
        }
        public static DateTime FirstDayOfMonth(this DateTime datetime)
        {
            return new DateTime(datetime.Year, datetime.Month, 1).Date;
        }

        public static DateTime LastDayOfMonth(this DateTime datetime)
        {
            return new DateTime(datetime.Year, datetime.Month, 1).AddMonths(1).AddDays(-1).Date;
        }
    }
}
