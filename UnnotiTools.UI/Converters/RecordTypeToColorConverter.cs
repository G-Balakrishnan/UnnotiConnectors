using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Data;
using System.Windows.Media;

namespace UnnotiTools.UI.Converters
{
    public class RecordTypeToColorConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null) return Brushes.White;

            var text = value.ToString().ToLower();

            if (text.ToLower().Contains("scheme"))
                return new SolidColorBrush((Color)ColorConverter.ConvertFromString("red")); // Yellow

            if (text.ToLower().Contains("golden"))
                return new SolidColorBrush((Color)ColorConverter.ConvertFromString("#FFD700")); // Gold

            return Brushes.White;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
            => throw new NotImplementedException();
    }
}
