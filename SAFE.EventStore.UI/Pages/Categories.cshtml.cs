using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace SAFE.EventStore.UI.Pages
{
    public class CategoriesModel : AuthPageModel
    {
        public CategoriesModel(Setup.UISession session)
            : base(session)
        { }

        public string DatabaseId { get; private set; }

        public List<string> Categories { get; private set; }

        public async Task<IActionResult> OnGetAsync(string databaseId)
        {
            if (!Authenticated)
                return RedirectToPage("/Login");

            try
            {
                var service = _serviceFactory();
                Message = $"Displaying categories for database {databaseId}.";
                DatabaseId = databaseId;
                Categories = await service.GetCategoriesAsync(databaseId);
            }
            catch (Exception ex)
            {
                return SetErrorMsg(ex);
            }

            return Page();
        }
    }
}