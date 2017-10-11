using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SAFE.SystemUtils
{
    public static class Serializer
    {
        public static JsonSerializerSettings SerializerSettings;
        static Serializer()
        {
            SerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                Culture = new System.Globalization.CultureInfo(string.Empty)
                {
                    NumberFormat = new System.Globalization.NumberFormatInfo
                    {
                        CurrencyDecimalDigits = 31
                    }
                },
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                ContractResolver = new PrivateMemberContractResolver()
            };
            SerializerSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
            //SerializerSettings.Converters.Add(new ClaimConverter());
            JsonConvert.DefaultSettings = () => SerializerSettings;
        }

        public static string GetJson(this byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }

        public static T Parse<T>(this byte[] data)
        {
            var result = default(T);
            TryParse<T>(data, out result);
            return result;
        }

        public static object Parse(this byte[] data, string assemblyQualifiedName)
        {
            object result = null;
            TryParse(data, assemblyQualifiedName, out result);
            return result;
        }

        public static bool TryParse<T>(byte[] data, out T result)
        {
            try
            {
                result = (T)ParseFromJson(Encoding.UTF8.GetString(data), typeof(T).AssemblyQualifiedName);
                return true;
            }
            catch (Exception ex)
            {
                // _logger.Error(ex, "Error in {0} when handling msg.", ..);
                result = default(T);
                return false;
            }
        }

        public static object TryParse(byte[] data, string assemblyQualifiedName, out object result)
        {
            try
            {
                result = ParseFromJson(Encoding.UTF8.GetString(data), assemblyQualifiedName);
                return true;
            }
            catch (Exception ex)
            {
                // _logger.Error(ex, "Error in {0} when handling msg.", ..);
                result = null;
                return false;
            }
        }

        public static object ParseFromJson(string json, string assemblyQualifiedName)
        {
            var type = Type.GetType(assemblyQualifiedName);
            var obj = Deserialize(json, type);
            
            return obj;
        }

        private static object Deserialize(string json, Type type)
        {
            JsonConvert.DefaultSettings = () => SerializerSettings;
            var obj = JsonConvert.DeserializeObject(json, type);
            //OnDeserialize(env);
            return obj;
        }

        public static string Json(this object some)
        {
            try
            {
                var data = JsonConvert.SerializeObject(some, SerializerSettings);

                return data;
            }
            catch
            { return null; } // _logger.Error(ex, "Error in {0} when handling msg.", _subscriptionId);
        }

        public static byte[] AsBytes(this object some)
        {
            try
            {
                return Encoding.UTF8.GetBytes(some.Json());
            }
            catch (Exception ex)
            { return null; } // _logger.Error(ex, "Error in {0} when handling msg.", _subscriptionId);
        }

        //public static object GetBody(object Body, string assemblyQualifiedName)
        //{
        //    var type = Type.GetType(assemblyQualifiedName);
        //    var toParse = Body as JObject;
        //    Body = toParse.ToObject(type);

        //    ParseLists(type, toParse, Body);

        //    return Body;
        //}

        //private static void ParseLists(Type type, JObject toParse, object body)
        //{
        //    var props = type.GetProperties();
        //    foreach (var prop in props)
        //    {
        //        if (prop.PropertyType.Name.Contains("List"))
        //        {
        //            var t = toParse.GetValue(prop.Name) as JArray;
        //            if (t == null) return;
        //            var obj = t.ToObject(prop.PropertyType);
        //            if (obj == null) return;
        //            prop.SetValue(body, obj);
        //        }
        //    }
        //}

        //public static object Cast(this Type Type, object data)
        //{
        //    var DataParam = Expression.Parameter(typeof(object), "data");
        //    var Body = Expression.Block(Expression.Convert(Expression.Convert(DataParam, data.GetType()), Type));

        //    var Run = Expression.Lambda(Body, DataParam).Compile();
        //    var ret = Run.DynamicInvoke(data);
        //    return ret;
        //}

        //public static bool IsNullOrEmpty(this JToken token)
        //{
        //    return (token == null) ||
        //           (token.Type == JTokenType.Array && !token.HasValues) ||
        //           (token.Type == JTokenType.Object && !token.HasValues) ||
        //           (token.Type == JTokenType.String && token.ToString() == String.Empty) ||
        //           (token.Type == JTokenType.Null);
        //}
        
    }

    // http://stackoverflow.com/questions/4066947/private-setters-in-json-net
    // Without this, collections will be null, since all events have private setters.
    public class PrivateMemberContractResolver : DefaultContractResolver
    {
        protected override JsonProperty CreateProperty(
            MemberInfo member,
            MemberSerialization memberSerialization)
        {
            //TODO: Maybe cache
            var prop = base.CreateProperty(member, memberSerialization);

            if (!prop.Writable)
            {
                var property = member as PropertyInfo;
                if (property != null)
                {
                    var hasPrivateSetter = property.GetSetMethod(true) != null;
                    prop.Writable = hasPrivateSetter;
                }
            }

            return prop;
        }
    }
}