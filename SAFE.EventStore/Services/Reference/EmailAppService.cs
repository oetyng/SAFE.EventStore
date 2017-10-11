using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Utils;
using Newtonsoft.Json;
using SAFE.DotNET.Helpers;
using SAFE.DotNET.Models;
using System.Threading;
using SAFE.DotNET.Native;

//[assembly: Dependency(typeof(AppService))]

namespace SAFE.DotNET.Services
{
    public class EmailAppService : IDisposable
    {
        public const string AuthDeniedMessage = "Failed to receive Authentication.";
        public const string AuthInProgressMessage = "Connecting to SAFE Network...";

        //private const string AuthReconnectPropKey = nameof(AuthReconnect);
        private string _appId;

        //private bool _isLogInitialised;

        private string AppId
        {
            get
            {
                if (_appId == string.Empty)
                {
                    throw new ArgumentNullException();
                }
                return _appId;
            }
            set => _appId = value;
        }

        public UserId Self { get; set; }
        //private CredentialCacheService CredentialCache { get; }

        public bool IsLogInitialised { get; set; }//{ get => _isLogInitialised; set => SetProperty(ref _isLogInitialised, value); }

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

        public EmailAppService()
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

        public async Task AddIdAsync(string userId)
        {
            if (userId.Contains(".") || userId.Contains("@"))
            {
                throw new NotSupportedException("Unsupported characters '.' and '@'.");
            }

            // Check if account exits first and return error
            var dstPubIdDigest = await NativeUtils.Sha3HashAsync(userId.ToUtfBytes());
            using (var dstPubIdMDataInfoH = await MDataInfo.NewPublicAsync(dstPubIdDigest, 15001))
            {
                var accountExists = false;
                try
                {
                    var keysH = await MData.ListKeysAsync(dstPubIdMDataInfoH);
                    keysH.Dispose();
                    accountExists = true;
                }
                catch (Exception)
                {
                    // ignored - acct not found
                }
                if (accountExists)
                {
                    throw new Exception("Id already exists.");
                }
            }

            // Create Self Permissions to Inbox and Archive
            using (var inboxSelfPermSetH = await MDataPermissionSet.NewAsync())
            {
                await Task.WhenAll(
                  MDataPermissionSet.AllowAsync(inboxSelfPermSetH, MDataAction.kInsert),
                  MDataPermissionSet.AllowAsync(inboxSelfPermSetH, MDataAction.kUpdate),
                  MDataPermissionSet.AllowAsync(inboxSelfPermSetH, MDataAction.kDelete),
                  MDataPermissionSet.AllowAsync(inboxSelfPermSetH, MDataAction.kManagePermissions));

                using (var inboxPermH = await MDataPermissions.NewAsync())
                {
                    using (var appSignPkH = await Crypto.AppPubSignKeyAsync())
                    {
                        await MDataPermissions.InsertAsync(inboxPermH, appSignPkH, inboxSelfPermSetH);
                    }

                    // Create Archive MD
                    List<byte> serArchiveMdInfo;
                    using (var archiveMDataInfoH = await MDataInfo.RandomPrivateAsync(15001))
                    {
                        await MData.PutAsync(archiveMDataInfoH, inboxPermH, NativeHandle.Zero);
                        serArchiveMdInfo = await MDataInfo.SerialiseAsync(archiveMDataInfoH);
                    }

                    // Update Inbox permisions to allow anyone to insert
                    using (var inboxAnyonePermSetH = await MDataPermissionSet.NewAsync())
                    {
                        await MDataPermissionSet.AllowAsync(inboxAnyonePermSetH, MDataAction.kInsert);
                        await MDataPermissions.InsertAsync(inboxPermH, NativeHandle.Zero, inboxAnyonePermSetH);

                        // Create Inbox MD
                        List<byte> serInboxMdInfo;
                        var (inboxEncPk, inboxEncSk) = await GenerateRandomKeyPair();
                        var inboxEmailPkEntryKey = "__email_enc_pk".ToUtfBytes();
                        using (var inboxEntriesH = await MDataEntries.NewAsync())
                        {
                            await MDataEntries.InsertAsync(inboxEntriesH, inboxEmailPkEntryKey, inboxEncPk.ToHexString().ToUtfBytes());
                            using (var inboxMDataInfoH = await MDataInfo.RandomPublicAsync(15001))
                            {
                                await MData.PutAsync(inboxMDataInfoH, inboxPermH, inboxEntriesH);
                                serInboxMdInfo = await MDataInfo.SerialiseAsync(inboxMDataInfoH);
                            }
                        }

                        // Create Public Id MD
                        var idDigest = await NativeUtils.Sha3HashAsync(userId.ToUtfBytes());
                        using (var pubIdMDataInfoH = await MDataInfo.NewPublicAsync(idDigest, 15001))
                        {
                            using (var pubIdEntriesH = await MDataEntries.NewAsync())
                            {
                                await MDataEntries.InsertAsync(pubIdEntriesH, "@email".ToUtfBytes(), serInboxMdInfo);
                                await MData.PutAsync(pubIdMDataInfoH, inboxPermH, pubIdEntriesH);
                            }
                        }

                        // Update _publicNames Container
                        using (var publicNamesContH = await AccessContainer.GetMDataInfoAsync("_publicNames"))
                        {
                            var pubNamesUserIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(publicNamesContH, userId.ToUtfBytes());
                            var pubNamesMsgBoxCipherBytes = await MDataInfo.EncryptEntryValueAsync(publicNamesContH, idDigest);
                            using (var pubNamesContEntActH = await MDataEntryActions.NewAsync())
                            {
                                await MDataEntryActions.InsertAsync(pubNamesContEntActH, pubNamesUserIdCipherBytes, pubNamesMsgBoxCipherBytes);
                                await MData.MutateEntriesAsync(publicNamesContH, pubNamesContEntActH);
                            }
                        }

                        // Finally update App Container
                        var msgBox = new MessageBox
                        {
                            EmailId = userId,
                            Inbox = new DataArray { Type = "Buffer", Data = serInboxMdInfo },
                            Archive = new DataArray { Type = "Buffer", Data = serArchiveMdInfo },
                            EmailEncPk = inboxEncPk.ToHexString(),
                            EmailEncSk = inboxEncSk.ToHexString()
                        };

                        var msgBoxSer = JsonConvert.SerializeObject(msgBox);
                        using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppId))
                        {
                            var userIdCipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, userId.ToUtfBytes());
                            var msgBoxCipherBytes = await MDataInfo.EncryptEntryValueAsync(appContH, msgBoxSer.ToUtfBytes());
                            var appContEntActH = await MDataEntryActions.NewAsync();
                            await MDataEntryActions.InsertAsync(appContEntActH, userIdCipherBytes, msgBoxCipherBytes);
                            await MData.MutateEntriesAsync(appContH, appContEntActH);
                        }
                    }
                }
            }
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
                        var cts = new CancellationTokenSource(2000);
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

        ~EmailAppService()
        {
            FreeState();
        }

        public void FreeState()
        {
            Session.FreeApp();
        }

        public async Task<string> GenerateAppRequestAsync()
        {
            AppId = "net.maidsafe.examples.mailtutorial";
            var authReq = new AuthReq
            {
                AppContainer = true,
                AppExchangeInfo = new AppExchangeInfo { Id = AppId, Scope = "", Name = "SAFE Messages", Vendor = "MaidSafe.net" },
                Containers = new List<ContainerPermissions> { new ContainerPermissions { ContainerName = "_publicNames", Access = { Insert = true } } }
            };

            var encodedReq = await Session.EncodeAuthReqAsync(authReq);
            var formattedReq = UrlFormat.Convert(encodedReq, false);
            Debug.WriteLine($"Encoded Req: {formattedReq}");
            return formattedReq;
        }

        private static async Task<(List<byte>, List<byte>)> GenerateRandomKeyPair()
        {
            var randomKeyPairTuple = await Crypto.EncGenerateKeyPairAsync();
            List<byte> inboxEncPk, inboxEncSk;
            using (var inboxEncPkH = randomKeyPairTuple.Item1)
            {
                using (var inboxEncSkH = randomKeyPairTuple.Item2)
                {
                    inboxEncPk = await Crypto.EncPubKeyGetAsync(inboxEncPkH);
                    inboxEncSk = await Crypto.EncSecretKeyGetAsync(inboxEncSkH);
                }
            }
            return (inboxEncPk, inboxEncSk);
        }

        public async Task<List<UserId>> GetIdsAsync()
        {
            var ids = new List<UserId>();
            using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppId))
            {
                List<List<byte>> cipherTextEntKeys;
                using (var appContEntKeysH = await MData.ListKeysAsync(appContH))
                {
                    cipherTextEntKeys = await MDataKeys.ForEachAsync(appContEntKeysH);
                }

                foreach (var cipherTextEntKey in cipherTextEntKeys)
                {
                    try
                    {
                        var plainTextEntKey = await MDataInfo.DecryptAsync(appContH, cipherTextEntKey);
                        ids.Add(new UserId(plainTextEntKey.ToUtfString()));
                    }
                    catch (Exception)
                    {
                        // ignore incompatible entries.
                    }
                }
            }
            return ids;
        }

        public async Task<List<Message>> GetMessagesAsync(string userId)
        {
            var messages = new List<Message>();

            List<byte> plainBytes;
            using (var appContH = await AccessContainer.GetMDataInfoAsync("apps/" + AppId))
            {
                var userIdByteList = userId.ToUtfBytes();
                var cipherBytes = await MDataInfo.EncryptEntryKeyAsync(appContH, userIdByteList);
                var content = await MData.GetValueAsync(appContH, cipherBytes);
                plainBytes = await MDataInfo.DecryptAsync(appContH, content.Item1);
            }

            var msgBox = JsonConvert.DeserializeObject<MessageBox>(plainBytes.ToUtfString());
            List<(List<byte>, List<byte>, ulong)> cipherTextEntries;
            using (var inboxInfoH = await MDataInfo.DeserialiseAsync(msgBox.Inbox.Data))
            {
                using (var inboxEntH = await MData.ListEntriesAsync(inboxInfoH))
                {
                    cipherTextEntries = await MDataEntries.ForEachAsync(inboxEntH);
                }
            }
            using (var inboxPkH = await Crypto.EncPubKeyNewAsync(msgBox.EmailEncPk.ToHexBytes()))
            {
                using (var inboxSkH = await Crypto.EncSecretKeyNewAsync(msgBox.EmailEncSk.ToHexBytes()))
                {
                    foreach (var entry in cipherTextEntries)
                    {
                        try
                        {
                            var entryKey = entry.Item1.ToUtfString();
                            if (entryKey == "__email_enc_pk")
                            {
                                continue;
                            }

                            var iDataNameEncoded = await Crypto.DecryptSealedBoxAsync(entry.Item2, inboxPkH, inboxSkH);
                            var iDataNameBytes = iDataNameEncoded.ToUtfString().Split(',').Select(val => Convert.ToByte(val)).ToList();
                            using (var msgDataReaderH = await IData.FetchSelfEncryptorAsync(iDataNameBytes))
                            {
                                var msgSize = await IData.SizeAsync(msgDataReaderH);
                                var msgCipher = await IData.ReadFromSelfEncryptorAsync(msgDataReaderH, 0, msgSize);
                                var plainTextMsg = await Crypto.DecryptSealedBoxAsync(msgCipher, inboxPkH, inboxSkH);
                                var jsonMsgData = plainTextMsg.ToUtfString();
                                messages.Add(JsonConvert.DeserializeObject<Message>(jsonMsgData));
                            }
                        }
                        catch (Exception e)
                        {
                            Debug.WriteLine("Exception: " + e.Message);
                        }
                    }
                }
            }

            return messages;
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
                    MessagingCenter.Send(this, MessengerConstants.AuthRequestProgress, string.Empty);
                }
                else
                {
                    Debug.WriteLine("Decoded Req is not Auth Granted");
                }
            }
            catch (Exception ex)
            {
                //await Application.Current.MainPage.DisplayAlert("Error", $"Description: {ex.Message}", "OK");
                MessagingCenter.Send(this, MessengerConstants.AuthRequestProgress, AuthDeniedMessage);
            }
        }

        private async void InitLoggingAsync()
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

        public async Task SendMessageAsync(string to, Message msg)
        {
            var dstPubIdDigest = await NativeUtils.Sha3HashAsync(to.ToUtfBytes());
            using (var dstPubIdMDataInfoH = await MDataInfo.NewPublicAsync(dstPubIdDigest, 15001))
            {
                var serDstMsgMDataInfo = await MData.GetValueAsync(dstPubIdMDataInfoH, "@email".ToUtfBytes());
                using (var dstInboxMDataInfoH = await MDataInfo.DeserialiseAsync(serDstMsgMDataInfo.Item1))
                {
                    var dstInboxPkItem = await MData.GetValueAsync(dstInboxMDataInfoH, "__email_enc_pk".ToUtfBytes());
                    using (var dstInboxPkH = await Crypto.EncPubKeyNewAsync(dstInboxPkItem.Item1.ToUtfString().ToHexBytes()))
                    {
                        var plainTxtMsg = JsonConvert.SerializeObject(msg);
                        var cipherTxt = await Crypto.EncryptSealedBoxAsync(plainTxtMsg.ToUtfBytes(), dstInboxPkH);
                        List<byte> msgDataMapNameBytes;
                        using (var msgWriterH = await IData.NewSelfEncryptorAsync())
                        {
                            await IData.WriteToSelfEncryptorAsync(msgWriterH, cipherTxt);
                            using (var msgCipherOptH = await CipherOpt.NewPlaintextAsync())
                            {
                                msgDataMapNameBytes = await IData.CloseSelfEncryptorAsync(msgWriterH, msgCipherOptH);
                            }
                        }

                        var encodedString = string.Join(", ", msgDataMapNameBytes.Select(val => val));
                        var msgDataMapNameCipher = await Crypto.EncryptSealedBoxAsync(encodedString.ToUtfBytes(), dstInboxPkH);
                        using (var dstMsgEntActH = await MDataEntryActions.NewAsync())
                        {
                            await MDataEntryActions.InsertAsync(dstMsgEntActH, Mock.RandomString(15).ToUtfBytes(), msgDataMapNameCipher);
                            await MData.MutateEntriesAsync(dstInboxMDataInfoH, dstMsgEntActH);
                        }
                    }
                }
            }
        }
    }
}