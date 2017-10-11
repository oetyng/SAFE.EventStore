using System;
using System.Linq;
using System.Windows.Input;
using SAFE.DotNET.Helpers;
using SAFE.DotNET.Models;


namespace SAFE.DotNET.ViewModels
{
    //internal class MessagesViewModel : BaseViewModel
    //{
    //    private bool _isRefreshing;
    //    private UserId _userId;

    //    public DataModel AppData => DependencyService.Get<DataModel>();

    //    public bool IsRefreshing { get => _isRefreshing; set => SetProperty(ref _isRefreshing, value); }
    //    public UserId UserId { get => _userId; set => SetProperty(ref _userId, value); }
    //    public ICommand RefreshCommand { get; }
    //    public ICommand SendCommand { get; }
    //    public ICommand MessageSelectedCommand { get; }

    //    public MessagesViewModel(UserId userId)
    //    {
    //        UserId = userId ?? new UserId("Unknown");
    //        IsRefreshing = false;
    //        RefreshCommand = new Command(OnRefreshCommand);
    //        SendCommand = new Command(OnSendCommand);
    //        MessageSelectedCommand = new Command<Message>(OnMessageSelectedCommand);
    //    }

    //    private void OnMessageSelectedCommand(Message message)
    //    {
    //        MessagingCenter.Send(this, MessengerConstants.NavDisplayMessageView, message);
    //    }

    //    private async void OnRefreshCommand()
    //    {
    //        IsRefreshing = true;
    //        try
    //        {
    //            var messages = await SafeApp.GetMessagesAsync(UserId.Name);
    //            AppData.Messages.RemoveRange(AppData.Messages.Except(messages));
    //            AppData.Messages.AddRange(messages.Except(AppData.Messages));
    //            AppData.Messages.Sort(true);
    //        }
    //        catch (Exception ex)
    //        {
    //            //await Application.Current.MainPage.DisplayAlert("Error", $"Fetch Messages Failed: {ex.Message}", "OK");
    //        }
    //        IsRefreshing = false;
    //    }

    //    private void OnSendCommand() {
    //        MessagingCenter.Send(this, MessengerConstants.NavSendMessagePage);
    //    }
    //}
}