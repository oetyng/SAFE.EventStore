
namespace SAFE.EventStore.UI.Pages
{
    public class ContactModel : AuthPageModel
    {
        public ContactModel(Setup.UISession session)
            : base(session)
        { }

        public void OnGet()
        {
            Message = "";
        }
    }
}
