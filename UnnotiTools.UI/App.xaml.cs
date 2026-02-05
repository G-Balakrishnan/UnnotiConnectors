using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Unnoti.Connector.Base;
using Unnoti.Core.IContracts;

namespace UnnotiTools.UI
{
    public partial class App : Application
    {
        protected override async void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);
            //System.Diagnostics.Debugger.Break();
            try
            {
                var args = e.Args ?? Array.Empty<string>();

                var isHeadless = args.Any(a =>
                    a.Equals("--headless=true", StringComparison.OrdinalIgnoreCase));

                if (isHeadless)
                {
                    await RunHeadless(args);
                    Shutdown(0);
                    return;
                }

                // UI MODE (ONLY HERE UI IS SHOWN)
                //var mainWindow = new MainWindow();
                //mainWindow.Show();
            }
            catch (Exception ex)
            {
                // ABSOLUTE MUST for Scheduler debugging
                File.WriteAllText(
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "startup-error.log"),
                    ex.ToString());

                Shutdown(1);
            }
        }

        private async Task RunHeadless(string[] args)
        {
            var dict = args
                .Select(a => a.Split('='))
                .Where(a => a.Length == 2)
                .ToDictionary(
                    a => a[0].TrimStart('-'),
                    a => a[1],
                    StringComparer.OrdinalIgnoreCase);

            if (!dict.TryGetValue("connectorKey", out var connectorKey) ||
                !dict.TryGetValue("configPath", out var configPath))
            {
                throw new Exception(
                    "Required arguments: --headless=true --connectorKey --configPath");
            }

            if (!File.Exists(configPath))
                throw new FileNotFoundException("Config file not found", configPath);

            var connector = ConnectorFactory.Get(connectorKey);

            await connector.ExecuteAsync(configPath, CancellationToken.None);
        }

        static class ConnectorFactory
        {
            private static readonly System.Collections.Generic.Dictionary<string, Type> _map =
                AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(a =>
                    {
                        try { return a.GetTypes(); }
                        catch { return Array.Empty<Type>(); }
                    })
                    .Where(t => typeof(IConnector).IsAssignableFrom(t)
                                && !t.IsAbstract
                                && !t.IsInterface)
                    .Select(t => (IConnector)Activator.CreateInstance(t))
                    .ToDictionary(c => c.ConnectorKey, c => c.GetType(),
                        StringComparer.OrdinalIgnoreCase);

            public static IConnector Get(string key)
            {
                if (!_map.TryGetValue(key, out var type))
                    throw new Exception($"Connector not registered: {key}");

                return (IConnector)Activator.CreateInstance(type);
            }
        }
    }
}
