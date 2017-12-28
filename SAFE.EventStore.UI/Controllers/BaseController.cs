using Microsoft.AspNetCore.Mvc;
using SAFE.EventStore.Services;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace SAFE.EventStore.UI.Controllers
{
    public abstract class BaseController : Controller
    {
        IAppSession _session;

        protected bool IsAuthenticated => _session.IsAuthenticated;

        public BaseController(Setup.UISession session)
        {
            _session = session.AppSession();
        }
    }
}