using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Connector.Base;
using Unnoti.Core.IContracts;

namespace Unnoti.Core
{
    public static class ConnectorRegistry
    {
        private static Dictionary<string, Type> _cache;

        /// <summary>
        /// Discovers all available connectors across loaded assemblies.
        /// Result is cached for the lifetime of the process.
        /// </summary>
        public static IReadOnlyDictionary<string, Type> Discover()
        {
            if (_cache != null)
                return _cache;

            var connectorInterface = typeof(IConnector);
            var connectorBaseType = typeof(ConnectorBase);

            var connectors = new Dictionary<string, Type>(StringComparer.OrdinalIgnoreCase);

            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                // Skip system assemblies (performance + safety)
                if (assembly.FullName.StartsWith("System")
                    || assembly.FullName.StartsWith("Microsoft"))
                    continue;

                foreach (var type in assembly.GetTypes())
                {
                    if (type.IsAbstract || type.IsInterface)
                        continue;

                    if (!connectorInterface.IsAssignableFrom(type))
                        continue;

                    if (!connectorBaseType.IsAssignableFrom(type))
                        continue;

                    // Create a lightweight instance ONLY to read metadata
                    var instance = (ConnectorBase)Activator.CreateInstance(type);
                    var connectorType = instance.ConnectorType;

                    if (string.IsNullOrWhiteSpace(connectorType))
                        throw new InvalidOperationException(
                            $"ConnectorType missing on {type.FullName}");

                    if (connectors.ContainsKey(connectorType))
                        throw new InvalidOperationException(
                            $"Duplicate ConnectorType '{connectorType}' found in {type.FullName}");

                    connectors.Add(connectorType, type);
                }
            }

            _cache = connectors;
            return _cache;
        }

        /// <summary>
        /// Creates a connector instance by ConnectorType
        /// </summary>
        public static IConnector Create(string connectorType)
        {
            var connectors = Discover();

            if (!connectors.TryGetValue(connectorType, out var type))
                throw new KeyNotFoundException(
                    $"Connector not found for ConnectorType '{connectorType}'");

            return (IConnector)Activator.CreateInstance(type);
        }
    }
}
