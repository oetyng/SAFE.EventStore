
using System;
using System.Threading.Tasks;
using SAFE.DotNET.Models;
using SAFE.DotNET.Services;
using SAFE.DotNET.ViewModels;
using SAFE.EventStore.Services;

namespace SAFE.DotNET
{
    // TODO, fix proper
    public static class MessagingCenter
    {
        internal static void Send(AppSession appService, string messageType, string message) // EmailAppService appService
        {
            // todo
        }

        internal static void Send(EmailAppService appService, string messageType, string message) // EmailAppService appService
        {
            // todo
        }

        internal static void Send(BaseViewModel addIdViewModel, string navUserIdsPage)
        {
            // todo
        }

        internal static void Subscribe<T1, T2>(BaseViewModel authViewModel, T2 authRequestProgress, Func<object, string, Task> p)
        {
            throw new NotImplementedException();
        }

        internal static void Send(BaseViewModel displayMessageViewModel, string navSendMessagePage, string subject)
        {
            throw new NotImplementedException();
        }

        internal static void Send(BaseViewModel messagesViewModel, string navDisplayMessageView, Message message)
        {
            throw new NotImplementedException();
        }
    }
}