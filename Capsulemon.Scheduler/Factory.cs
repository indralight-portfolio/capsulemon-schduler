using StackExchange.Redis.Extensions.Core.Abstractions;
using StackExchange.Redis.Extensions.Core.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace StackExchange.Redis.Extensions.Core
{
    public interface IStatusCache : IRedisCacheClient
    { }

    public class StatusCache : RedisCacheClient, IStatusCache
    {
        public StatusCache(IRedisCacheConnectionPoolManager connectionPoolManager, ISerializer serializer, RedisConfiguration configuration) : base(connectionPoolManager, serializer, configuration) { }
    }
}

namespace StackExchange.Redis.Extensions
{
    public static class ExtensionMethods
    {
        public static T ToModel<T>(this HashEntry[] entries)
        {
            try
            {
                if (entries.Length <= 0)
                {
                    return default(T);
                }

                Type type = typeof(T);
                var result = Activator.CreateInstance(type);
                for (int i = 0; i < entries.Length; ++i)
                {
                    var entry = entries[i];
                    var property = type.GetProperty(name: entry.Name);
                    if (null == property)
                    {
                        Console.WriteLine("Error: can't found. " + entry.Name);
                        continue;
                    }

                    var typeCode = Type.GetTypeCode(property.PropertyType);
                    switch (typeCode)
                    {
                        case TypeCode.Boolean:
                            type.GetProperty(entry.Name).SetValue(result, Convert.ToBoolean(entry.Value));
                            break;

                        case TypeCode.Byte:
                        case TypeCode.SByte:
                        case TypeCode.UInt16:
                        case TypeCode.Int16:
                        case TypeCode.Int32:
                            type.GetProperty(entry.Name).SetValue(result, Convert.ToInt32(entry.Value));
                            break;

                        case TypeCode.UInt32:
                            type.GetProperty(entry.Name).SetValue(result, Convert.ToUInt32(entry.Value));
                            break;

                        case TypeCode.Int64:
                            type.GetProperty(entry.Name).SetValue(result, Convert.ToInt64(entry.Value));
                            break;

                        case TypeCode.UInt64:
                            type.GetProperty(entry.Name).SetValue(result, Convert.ToUInt64(entry.Value));
                            break;

                        case TypeCode.Decimal:
                        case TypeCode.Double:
                        case TypeCode.Single:
                            throw new NotImplementedException();

                        case TypeCode.DateTime:
                            // DateTime.Parse는 무조건 LocalTime으로 전환한다. 그런데.. UTC가 System시간인 서버에서는 어떻게 동작하지?
                            type.GetProperty(entry.Name).SetValue(result, DateTime.Parse(entry.Value).ToUniversalTime());
                            break;

                        default:
                            type.GetProperty(entry.Name).SetValue(result, entry.Value.ToString());
                            break;
                    }
                }

                return (T)result;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            return default(T);
        }
    }
}