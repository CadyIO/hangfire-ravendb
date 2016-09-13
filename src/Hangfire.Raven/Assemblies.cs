using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
#if !NET45
using Microsoft.Extensions.DependencyModel;
#endif

namespace Hangfire.Raven
{
    /// <summary>
    /// Helper class for getting assemblies
    /// </summary>
    public static class Assemblies
    {
        /// <summary>
        /// Get all loaded assemblies
        /// </summary>
        /// <returns></returns>
        public static IEnumerable<Assembly> Get()
        {
#if NET45
            return AppDomain.CurrentDomain.GetAssemblies().Where(a => a != null);
#else
            return (from compilationLibrary in DependencyContext.Default.CompileLibraries
                    where compilationLibrary.Name.Contains(typeof(Assemblies).Namespace)
                    select Assembly.Load(new AssemblyName(compilationLibrary.Name)))
                    .ToList();
#endif
        }

        /// <summary>
        /// Get class by name within an assembly
        /// </summary>
        /// <param name="assembly">Assembly</param>
        /// <param name="type">Name of type</param>
        /// <returns></returns>
        public static Type GetClass(Assembly assembly, string type)
        {
            return (from t in assembly.GetTypes()
                    where t.FullName == type //&& !t.IsAbstract && t.IsClass
                    select t).FirstOrDefault();
        }

        public static string GetAssemblyName(MemberInfo value)
        {
#if NET45
            return value.DeclaringType.Assembly.GetName().Name;
#else
            return value.DeclaringType.GetTypeInfo().Assembly.GetName().Name;
#endif
        }
    }
}
