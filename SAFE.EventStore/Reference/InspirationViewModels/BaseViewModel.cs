using Utils;
using SAFE.DotNET.Services;


namespace SAFE.DotNET.ViewModels
{
    public class BaseViewModel
    {
        //private bool _isUiEnabled;

        public bool IsUiEnabled { get; set; } //{ get => _isUiEnabled; set => SetProperty(ref _isUiEnabled, value); }

        public EmailAppService SafeApp => DependencyService.Get<EmailAppService>();
    }
}