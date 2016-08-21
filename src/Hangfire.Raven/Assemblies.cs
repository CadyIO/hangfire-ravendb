using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

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
            return AppDomain.CurrentDomain.GetAssemblies().Where(a => a != null);
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
    }
}
