using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace SAFE.EventStore.UI.Pages.Databases
{
    public class IndexModel : AuthPageModel
    {
        public IndexModel(Setup.UISession session)
            : base(session)
        { }

        public List<Database> Databases { get; set; } = new List<Database>();

        public async Task<IActionResult> OnGetAsync()
        {
            if (!Authenticated)
                return RedirectToPage("/Login");

            var service = _serviceFactory();
            var ids = await service.GetDatabaseIdsAsync();
            foreach (var id in ids)
            {
                var streamTypes = await service.GetCategoriesAsync(id.Name);
                Databases.Add(new Database { DatabaseId = id.Name, StreamTypes = streamTypes.Select(x => new DbStreamType { TypeName = x }).ToList() });
            }

            Message = "Databases loaded.";

            return Page();
        }
    }

    public class Database
    {
        public string DatabaseId { get; set; }
        public List<DbStreamType> StreamTypes { get; set; }
    }

    public class DbStreamType
    {
        public string TypeName { get; set; }
    }
}