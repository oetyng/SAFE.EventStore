using SAFE.EventStore.Services;
using System.Threading.Tasks;

namespace SAFE.EventStore.UI.Setup
{
    class MockAppSession : IAppSession
    {
        public bool IsAuthenticated { get; private set; }

        public Task CheckAndReconnect()
        {
            return Task.FromResult(0);
        }

        public void Dispose()
        {
            State.AppId = null;
        }

        public Task<string> GenerateAppRequestAsync()
        {
            State.AppId = "oetyng.apps.safe.eventstore";
            HandleUrlActivationAsync("someurl");
            return Task.FromResult("safe-auth");
            //return Task.FromResult("safe://safe-auth");
        }

        public Task HandleUrlActivationAsync(string encodedUrl)
        {
            IsAuthenticated = true;
            return Task.FromResult(0);
        }
    }
}