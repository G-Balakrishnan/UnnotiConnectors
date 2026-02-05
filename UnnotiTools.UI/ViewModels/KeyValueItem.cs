using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnnotiTools.UI.ViewModels
{
    public class KeyValueItem : ViewModelBase
    {
        public string Key { get; set; }

        private string _value;
        public string Value
        {
            get => _value;
            set { _value = value; OnPropertyChanged(); }
        }
    }
}
