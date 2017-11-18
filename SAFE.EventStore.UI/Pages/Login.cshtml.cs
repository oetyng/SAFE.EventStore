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

        [BindProperty]
        public string EncodedUrl { get; set; }

        public async Task<IActionResult> OnGetAsync()
        {
            AuthUri = await _service.GenerateAppRequestAsync();
            if (_service.IsAuthenticated)
                return RedirectToPage("/Databases/Index");
            
            return Page();
        }

        public async Task<IActionResult> OnPostAsync()
        {
            await _service.HandleUrlActivationAsync(EncodedUrl);
            if (_service.IsAuthenticated)
                return RedirectToPage("/Databases/Index");

            return Page();
        }
    }
}