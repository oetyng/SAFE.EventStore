using SAFE.DotNET.Native;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utils;

namespace SAFE.EventStore.Services
{
    public class AppConstants
    {
        public const string Name = "SAFE EventStore";
        public const string Vendor = "oetyng";
        public const string Id = "oetyng.apps.safe.eventstore";
    }
    
    public class AppSession : IDisposable, IAppSession
    {
        const string AuthDeniedMessage = "Failed to receive Authentication.";
        const string AuthInProgressMessage = "Connecting to SAFE Network...";
        //const string AuthReconnectPropKey = nameof(AuthReconnect);
        //CredentialCacheService CredentialCache { get; }

        public bool IsLogInitialised { get; set; }
        public bool IsAuthenticated { get; private set; }

        string _appId;
        public string AppId
        {
            get
            {
                if (_appId == string.Empty)
                    throw new ArgumentNullException();
                return _appId;
            }
            set => _appId = value;
        }

        Session _session;

        public AppSession(Session session)
        {
            //CredentialCache = new CredentialCacheService();

            _session = session;
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
            AppId = null;
            _session.FreeApp();
            IsAuthenticated = false;
        }

        async Task<bool> InstallUriAsync()
        {
            AppId = "oetyng.apps.safe.eventstore";

            var appInfo = new AppInfo
            {
                Id = AppId,
                Name = "SAFE EventStore",
                Vendor = "oetyng",
                Icon = "some icon",
                Exec = "[authExecPath]", //@"C:\Users\oetyng\SAFEEventStore\SAFE.EventStore.AppAuth.exe"
                Schemes = "safe-b2v0ew5nlmfwchmuc2fmzs5ldmvudhn0b3jl"
            };

            var installed = await _session.InstallUriAsync(appInfo);
            
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
                AppExchangeInfo = new AppExchangeInfo { Id = AppId, Scope = "", Name = "SAFE EventStore", Vendor = "oetyng" },
                Containers = new List<ContainerPermissions> { new ContainerPermissions { ContainerName = "_publicNames", Access = { Insert = true } } }
            };

            var encodedReq = await _session.EncodeAuthReqAsync(authReq);
            var formattedReq = UrlFormat.Convert(encodedReq, false);
            Debug.WriteLine($"Encoded Req: {formattedReq}");
            return formattedReq;
        }

        public async Task HandleUrlActivationAsync(string encodedUrl)
        {
            try
            {
                var formattedUrl = UrlFormat.Convert(encodedUrl, true);
                var decodeResult = await _session.DecodeIpcMessageAsync(formattedUrl);
                if (decodeResult.AuthGranted.HasValue)
                {
                    var authGranted = decodeResult.AuthGranted.Value;
                    Debug.WriteLine("Received Auth Granted from Authenticator");
                    // update auth progress message
                    await _session.AppRegisteredAsync(AppId, authGranted);
                    //if (AuthReconnect)
                    //{
                    //    var encodedAuthRsp = JsonConvert.SerializeObject(authGranted);
                    //    CredentialCache.Store(encodedAuthRsp);
                    //}
                    IsAuthenticated = true;
                }
                else
                    Debug.WriteLine("Decoded Req is not Auth Granted");
            }
            catch (Exception ex)
            {
                //await Application.Current.MainPage.DisplayAlert("Error", $"Description: {ex.Message}", "OK");
            }
        }

        async void InitLoggingAsync()
        {
            var started = await _session.InitLoggingAsync();
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
                if (_session.IsDisconnected)
                {
                    //if (!AuthReconnect)
                    //{
                    //    throw new Exception("Reconnect Disabled");
                    //}
                    //using (UserDialogs.Instance.Loading("Reconnecting to Network"))
                    //{
                    //var encodedAuthRsp = CredentialCache.Retrieve();
                    //var authGranted = JsonConvert.DeserializeObject<AuthGranted>(encodedAuthRsp);
                    //await _session.AppRegisteredAsync(AppId, authGranted);
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
