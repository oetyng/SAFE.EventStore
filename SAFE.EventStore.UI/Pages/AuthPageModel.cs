using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using SAFE.EventStore.Services;
using System;

namespace SAFE.EventStore.UI.Pages
{
    public class AuthPageModel : PageModel
    {
        protected readonly Func<IEventStore> _serviceFactory;
        protected readonly Func<bool> _authenticated;

        public bool Authenticated { get => _authenticated(); }

        public string Message { get; protected set; }

        public AuthPageModel(Setup.UISession service)
        {
            _authenticated = () => service.Authenticated;
            _serviceFactory = service.EventStoreService;
        }

        protected IActionResult SetErrorMsg(Exception ex)
        {
            var msg = string.Empty;
            var currentEx = ex;
            while (currentEx != null)
            {
                msg += currentEx.Message + currentEx.StackTrace;
                currentEx = currentEx.InnerException;
            }

            Message = msg;

            return Page();
        }
    }
}