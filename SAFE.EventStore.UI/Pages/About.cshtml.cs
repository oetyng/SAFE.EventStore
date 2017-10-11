
namespace SAFE.EventStore.UI.Pages
{
    public class AboutModel : AuthPageModel
    {
        public AboutModel(Setup.UISession session)
            : base(session)
        { }

        public void OnGet()
        {
            Message = "";
        }
    }
}