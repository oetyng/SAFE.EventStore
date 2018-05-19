using SAFE.EventSourcing;
using SafeApp;
using SafeApp.Utilities;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

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

        public AppSession()
        {
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
            AppId = null;
            _session.Dispose();
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
                App = new AppExchangeInfo { Id = AppId, Scope = "", Name = "SAFE EventStore", Vendor = "oetyng" },
                Containers = new List<ContainerPermissions> { new ContainerPermissions { ContName = "_publicNames", Access = { Insert = true } } }
            };

            var encodedReq = await Session.EncodeAuthReqAsync(authReq);
            var formattedReq = UrlFormat.Format(AppId, encodedReq.Item2, false);
            Debug.WriteLine($"Encoded Req: {formattedReq}");
            return formattedReq;
        }

        public async Task<IEventStore> HandleUrlActivationAsync(string encodedUrl)
        {
            try
            {
                var encodedRequest = UrlFormat.GetRequestData(encodedUrl);
                var decodeResult = await Session.DecodeIpcMessageAsync(encodedRequest);
                if (decodeResult is AuthIpcMsg ipcMsg)
                {
                    // update auth progress message
                    Debug.WriteLine("Received Auth Granted from Authenticator");
                    IsAuthenticated = true;
                    _session = await Session.AppRegisteredAsync(AppId, ipcMsg.AuthGranted);
                    //if (AuthReconnect)
                    //{
                    //    var encodedAuthRsp = JsonConvert.SerializeObject(ipcMsg.AuthGranted);
                    //    CredentialCache.Store(encodedAuthRsp);
                    //}
                    return new EventStoreImDProtocol(AppId, _session);
                }
                else
                    Debug.WriteLine("Decoded Req is not Auth Granted");

                throw new Exception("Decoded Req is not Auth Granted");
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        async void InitLoggingAsync()
        {
            //var started = await _session.InitLoggingAsync();
            //if (!started)
            //{
            //    Debug.WriteLine("Unable to Initialise Logging.");
            //    return;
            //}

            //Debug.WriteLine("Rust Logging Initialised.");
            //IsLogInitialised = true;
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
