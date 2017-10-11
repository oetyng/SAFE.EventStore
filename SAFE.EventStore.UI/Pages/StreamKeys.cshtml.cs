using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc;
using System;
using SAFE.EventStore.Services;

namespace SAFE.EventStore.UI.Pages
{
    public class StreamKeysModel : AuthPageModel
    {
        public StreamKeysModel(Setup.UISession session)
            : base(session)
        { }

        public string DatabaseId { get; set; }
        
        public List<string> StreamKeys { get; set; }

        public async Task<IActionResult> OnGetAsync(string databaseId, string category)
        {
            if (!Authenticated)
                return RedirectToPage("/Login");

            try
            {
                var service = _serviceFactory();
                Message = $"Displaying stream keys for category {category}.";
                DatabaseId = databaseId;
                StreamKeys = await service.GetStreamKeysAsync(databaseId, category);
            }
            catch(Exception ex)
            {
                return SetErrorMsg(ex);
            }

            return Page();
        }
    }
}