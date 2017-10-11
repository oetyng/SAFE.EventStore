using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.RazorPages;
using SAFE.EventStore.Services;
using Microsoft.AspNetCore.Mvc;

namespace SAFE.EventStore.UI.Pages
{
    public class LoginModel : AuthPageModel
    {
        IAppSession _service;

        public LoginModel(Setup.UISession session)
            : base(session)
        {
            _service = session.AppSession();
        }

        public string AuthUri { get; private set; }

        public async Task<IActionResult> OnGetAsync()
        {
            AuthUri = await _service.GenerateAppRequestAsync();
            if (_service.IsAuthenticated)
                return RedirectToPage("/Databases/Index");

            return Page();
        }
    }
}