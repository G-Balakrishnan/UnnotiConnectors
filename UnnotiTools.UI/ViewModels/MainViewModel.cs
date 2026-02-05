using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using Unnoti.Connector.Base;
using Unnoti.Core.Config;
using Unnoti.Core;
using UnnotiTools.UI.Services;
using UnnotiTools.UI.Infrastructure;
using Unnoti.Core.IContracts;

namespace UnnotiTools.UI.ViewModels
{
    public class MainViewModel : ViewModelBase
    {
        public ObservableCollection<ConnectorItemViewModel> Connectors { get; }
        public ObservableCollection<KeyValueItem> ConfigItems { get; }

        public ConnectorItemViewModel SelectedConnector
        {
            get => _selectedConnector;
            set
            {
                _selectedConnector = value;
                ConfigItems.Clear();

                foreach (var field in value.Schema)
                {
                    ConfigItems.Add(new KeyValueItem
                    {
                        Key = field.Label,
                        Value = field.DefaultValue
                    });
                }

                OnPropertyChanged();
            }
        }
        public string Logs
        {
            get => _logs;
            set { _logs = value; OnPropertyChanged(); }
        }

        public ICommand RunCommand { get; }

        private ConnectorItemViewModel _selectedConnector;
        private string _logs = "";

        public MainViewModel()
        {
            Connectors = new ObservableCollection<ConnectorItemViewModel>();
            ConfigItems = new ObservableCollection<KeyValueItem>();

            LoadConnectors();

            RunCommand = new RelayCommand(Run);
        }

        private void LoadConnectors()
        {
            foreach (var kv in ConnectorRegistry.Discover())
            {
                var instance = (IConnector)Activator.CreateInstance(kv.Value);

                if (instance is IConfigurableConnector configurable)
                {
                    Connectors.Add(
                        new ConnectorItemViewModel(
                            ((ConnectorBase)instance).Name,
                            ((ConnectorBase)instance).ConnectorType,
                            configurable.GetConfigSchema()));
                }
            }
        }

        private void Run()
        {
            try
            {
                var job = new JobConfig
                {
                   // JobName = "UI Job",
                    ConnectorType = SelectedConnector.ConnectorType,
                    //BaseApiUrl = "https://api-base-url",
                    //LogFilePath = "ui.log",
                    ConnectorConfig = ConfigItems.ToDictionary(k => k.Key, v => v.Value).First().Value
                };

                JobRunner.Run(job, AppendLog);

            }
            catch (Exception ex)
            {
                AppendLog("Error : "+ex.Message);
            }
            
        }

        private void AppendLog(string msg)
        {
            Logs += msg + Environment.NewLine;
        }
    }
}
