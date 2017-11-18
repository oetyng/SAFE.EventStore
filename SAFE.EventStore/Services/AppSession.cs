using SAFE.DotNET;
using SAFE.DotNET.Helpers;
using SAFE.DotNET.Native;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utils;

namespace SAFE.EventStore.Services
{

    public class State
    {
        static string _appId;

        public static string AppId
        {
            get
            {
                if (_appId == string.Empty)
                    throw new ArgumentNullException();
                return _appId;
            }
            set => _appId = value;
        }
    }


    public class AppSession : IDisposable, IAppSession
    {
        const string AuthDeniedMessage = "Failed to receive Authentication.";
        const string AuthInProgressMessage = "Connecting to SAFE Network...";
        //private const string AuthReconnectPropKey = nameof(AuthReconnect);
        

        //private bool _isLogInitialised;
        //private CredentialCacheService CredentialCache { get; }

        public bool IsLogInitialised { get; set; } //{ get => _isLogInitialised; set => SetProperty(ref _isLogInitialised, value); }

        public bool IsAuthenticated { get; private set; }

        public static string AppId => State.AppId;

        public AppSession()
        {
            //_isLogInitialised = false;
            //CredentialCache = new CredentialCacheService();
            InitLoggingAsync();
        }

        public void Dispose()
        {
            FreeState();
            GC.SuppressFinalize(this);
        }

        ~AppSession()
        {
            FreeState();
        }

        void FreeState()
        {
            State.AppId = null;
            Session.FreeApp();
            IsAuthenticated = false;
        }

        async Task<bool> InstallUriAsync()
        {
            State.AppId = "oetyng.apps.safe.eventstore";

            var appInfo = new AppInfo
            {
                Id = AppId,
                Name = "SAFE EventStore",
                Vendor = "Oetyng",
                Icon = "some icon",
                Exec = @"C:\Users\oetyng\SAFEEventStore\SAFE.EventStore.AppAuth.exe", // tested various ways: // @"localhost://p:52794/api/auth?EncodedUrl=test", ,//@"dotnet C:\Users\oetyng\SAFEEventStore\SAFE.EvenStore.AppAuth.dll",
                Schemes = "safe-b2v0ew5nlmfwchmuc2fmzs5ldmvudhn0b3jl"
            };

            var installed = await Session.InstallUriAsync(appInfo);
            
            Debug.WriteLine($"Installed: {installed}");
            return installed;
        }

        public async Task<string> GenerateAppRequestAsync()
        {
            if (!await InstallUriAsync())
                throw new InvalidOperationException();

            var authReq = new AuthReq
            {
                AppContainer = true,
                AppExchangeInfo = new AppExchangeInfo { Id = AppId, Scope = "", Name = "SAFE EventStore", Vendor = "Oetyng" },
                Containers = new List<ContainerPermissions> { new ContainerPermissions { ContainerName = "_publicNames", Access = { Insert = true } } }
            };

            var encodedReq = await Session.EncodeAuthReqAsync(authReq);
            var formattedReq = UrlFormat.Convert(encodedReq, false);
            Debug.WriteLine($"Encoded Req: {formattedReq}");
            return formattedReq;
        }

        public async Task HandleUrlActivationAsync(string encodedUrl)
        {
            try
            {
                var formattedUrl = UrlFormat.Convert(encodedUrl, true);
                var decodeResult = await Session.DecodeIpcMessageAsync(formattedUrl);
                if (decodeResult.AuthGranted.HasValue)
                {
                    var authGranted = decodeResult.AuthGranted.Value;
                    Debug.WriteLine("Received Auth Granted from Authenticator");
                    // update auth progress message
                    MessagingCenter.Send(this, MessengerConstants.AuthRequestProgress, AuthInProgressMessage);
                    await Session.AppRegisteredAsync(AppId, authGranted);
                    //if (AuthReconnect)
                    //{
                    //    var encodedAuthRsp = JsonConvert.SerializeObject(authGranted);
                    //    CredentialCache.Store(encodedAuthRsp);
                    //}
                    IsAuthenticated = true;
                    MessagingCenter.Send(this, MessengerConstants.AuthRequestProgress, string.Empty);
                }
                else
                    Debug.WriteLine("Decoded Req is not Auth Granted");
            }
            catch (Exception ex)
            {
                //await Application.Current.MainPage.DisplayAlert("Error", $"Description: {ex.Message}", "OK");
                MessagingCenter.Send(this, MessengerConstants.AuthRequestProgress, AuthDeniedMessage);
            }
        }

        async void InitLoggingAsync()
        {
            var started = await Session.InitLoggingAsync();
            if (!started)
            {
                Debug.WriteLine("Unable to Initialise Logging.");
                return;
            }

            Debug.WriteLine("Rust Logging Initialised.");
            IsLogInitialised = true;
        }

        public async Task CheckAndReconnect()
        {
            try
            {
                if (Session.IsDisconnected)
                {
                    //if (!AuthReconnect)
                    //{
                    //    throw new Exception("Reconnect Disabled");
                    //}
                    //using (UserDialogs.Instance.Loading("Reconnecting to Network"))
                    //{
                    //var encodedAuthRsp = CredentialCache.Retrieve();
                    //var authGranted = JsonConvert.DeserializeObject<AuthGranted>(encodedAuthRsp);
                    //await Session.AppRegisteredAsync(AppId, authGranted);
                    //}
                    try
                    {
                        //var cts = new CancellationTokenSource(2000);
                        //await UserDialogs.Instance.AlertAsync("Network connection established.", "Success", "OK", cts.Token);
                    }
                    catch (OperationCanceledException) { }
                }
            }
            catch (Exception ex)
            {
                //await Application.Current.MainPage.DisplayAlert("Error", $"Unable to Reconnect: {ex.Message}", "OK");
                FreeState();
                //MessagingCenter.Send(this, MessengerConstants.ResetAppViews);
            }
        }

        //public bool AuthReconnect
        //{
        //    get
        //    {
        //        if (!Application.Current.Properties.ContainsKey(AuthReconnectPropKey))
        //        {
        //            return false;
        //        }

        //        var val = Application.Current.Properties[AuthReconnectPropKey] as bool?;
        //        return val == true;
        //    }
        //    set
        //    {
        //        if (value == false)
        //        {
        //            CredentialCache.Delete();
        //        }

        //        Application.Current.Properties[AuthReconnectPropKey] = value;
        //    }
        //}
    }
}
