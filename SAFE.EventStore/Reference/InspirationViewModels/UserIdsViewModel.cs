using System;
using System.Linq;
using System.Windows.Input;
using SAFE.DotNET.Helpers;
using SAFE.DotNET.Models;


namespace SAFE.DotNET.ViewModels
{
    //internal class UserIdsViewModel : BaseViewModel
    //{
    //    private bool _isRefreshing;

    //    public DataModel AppData => DependencyService.Get<DataModel>();

    //    public bool IsRefreshing { get => _isRefreshing; set => SetProperty(ref _isRefreshing, value); }
    //    public ICommand RefreshAccountsCommand { get; }
    //    public ICommand AddAcountCommand { get; }
    //    public ICommand UserIdSelectedCommand { get; }

    //    public UserIdsViewModel()
    //    {
    //        IsRefreshing = false;
    //        RefreshAccountsCommand = new Command(OnRefreshAccountsCommand);
    //        AddAcountCommand = new Command(OnAddAccountCommand);
    //        UserIdSelectedCommand = new Command<UserId>(OnUserIdSelectedCommand);
    //        //Device.BeginInvokeOnMainThread(OnRefreshAccountsCommand);
    //    }

    //    private void OnAddAccountCommand()
    //    {
    //        MessagingCenter.Send(this, MessengerConstants.NavAddIdPage);
    //    }

    //    private async void OnRefreshAccountsCommand()
    //    {
    //        IsRefreshing = true;
    //        try
    //        {
    //            var accounts = await SafeApp.GetIdsAsync();
    //            AppData.Accounts.AddRange(accounts.Except(AppData.Accounts));
    //            AppData.Accounts.Sort();
    //        }
    //        catch (Exception ex)
    //        {
    //            //await Application.Current.MainPage.DisplayAlert("Error", $"Fetch Ids Failed: {ex.Message}", "OK");
    //        }

    //        IsRefreshing = false;
    //    }

    //    private void OnUserIdSelectedCommand(UserId userId)
    //    {
    //        SafeApp.Self = userId;
    //        MessagingCenter.Send(this, MessengerConstants.NavMessagesPage, userId);
    //    }
    //}
}