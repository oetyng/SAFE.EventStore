
namespace SAFE.EventStore.UI.Pages
{
    public class LogoutModel : AuthPageModel
    {
        Setup.LogoutService _service;

        public LogoutModel(Setup.LogoutService service, Setup.UISession session)
            : base(session)
        {
            _service = service;
        }

        public void OnGet()
        {
            _service.Logout();
        }
    }
}