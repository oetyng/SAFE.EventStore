using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace SAFE.EventStore.UI.Pages.Databases
{
    public class CreateModel : AuthPageModel
    {
        public CreateModel(Setup.UISession session)
            : base(session)
        { }

        [BindProperty]
        public string DatabaseId { get; set; }

        public async Task<IActionResult> OnPostAsync()
        {
            if (!Authenticated)
                return RedirectToPage("/Login");

            if (!ModelState.IsValid)
                return Page();

            try
            {
                var service = _serviceFactory();

                await service.CreateDbAsync(DatabaseId);
            }
            catch (Exception ex)
            {
                return SetErrorMsg(ex);
            }

            return RedirectToPage("./Index");
        }
    }
}