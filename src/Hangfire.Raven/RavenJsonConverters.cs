using System;
using System.Linq;
using System.Reflection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Hangfire.Raven
{
    /// <summary>
    /// Base class for Raven Json Converters
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class RavenJsonConverterBase<T>
        : JsonConverter
    {
        public RavenJsonConverterBase()
        {
        }

        /// <summary>
        /// Read Json and convert to T
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value">Object to convert</param>
        /// <param name="serializer">JsonSerializer</param>
        /// <returns>Object</returns>
        protected abstract T Read(Type type, object value, JsonSerializer serializer);

        /// <summary>
        /// Write Json and convert T to text
        /// </summary>
        /// <param name="value">Input</param>
        /// <returns>Input as text</returns>
        protected abstract object Write(T value, JsonSerializer serializer);

        /// <summary>
        /// Overriding the CanConvert of JsonConvert
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public override bool CanConvert(Type type) => typeof(T).IsAssignableFrom(type);

        /// <summary>
        /// Overriding the ReadJson of JsonConvert
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="type"></param>
        /// <param name="existingValue"></param>
        /// <param name="serializer"></param>
        /// <returns></returns>
        public override object ReadJson(JsonReader reader, Type type, object existingValue, JsonSerializer serializer)
        {
            T toReturn;

            switch (reader.TokenType) {
                case JsonToken.StartArray:
                    toReturn = Read(type, JArray.Load(reader), serializer);
                    break;
                case JsonToken.StartObject:
                    toReturn = Read(type, JObject.Load(reader), serializer);
                    break;
                default:
                    toReturn = Read(type, reader.Value, serializer);
                    break;
            }

            return toReturn;
        }

        /// <summary>
        /// Overriding the WriteJson of JsonConvert
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="serializer"></param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            serializer.Serialize(writer, Write((T)value, serializer));
        }
    }

    public class RavenJsonMethodConverter
        : RavenJsonConverterBase<MethodInfo>
    {
        protected override MethodInfo Read(Type type, object value, JsonSerializer serializer)
        {
            if (value == null) {
                return null;
            }

            var splitted = value.ToString().Split(';');

            if (splitted.Count() == 3) {
                var assembly = Assemblies.Get().Where(a => a.GetName().Name == splitted[0]).First();
                var assemblyClass = Assemblies.GetClass(assembly, splitted[1]);

                if (assemblyClass != null) {
                    return assemblyClass.GetMethod(splitted[2]);
                }
            }

            return null;
        }

        protected override object Write(MethodInfo value, JsonSerializer serializer) => $"{value.DeclaringType.GetTypeInfo().Assembly.GetName().Name};{value.DeclaringType.FullName};{value.Name}";
    }

    public class RavenJsonPropertyConverter
        : RavenJsonConverterBase<PropertyInfo>
    {
        protected override PropertyInfo Read(Type type, object value, JsonSerializer serializer)
        {
            if (value == null) {
                return null;
            }

            var splitted = value.ToString().Split(';');

            if (splitted.Count() == 3) {
                var assembly = Assemblies.Get().Where(a => a.GetName().Name == splitted[0]).First();
                var assemblyType = Assemblies.GetClass(assembly, splitted[1]);

                if (assemblyType != null) {
                    return assemblyType.GetProperty(splitted[2]);
                }
            }

            return null;
        }

        protected override object Write(PropertyInfo value, JsonSerializer serializer) => $"{value.DeclaringType.GetTypeInfo().Assembly.GetName().Name};{value.DeclaringType.FullName};{value.Name}";
    }
}
