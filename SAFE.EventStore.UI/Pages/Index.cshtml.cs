using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace SAFE.EventStore.UI.Pages
{
    public class IndexModel : AuthPageModel
    {
        public IndexModel(Setup.UISession session)
            : base(session)
        { }

        public void OnGet()
        {

        }
    }
}
