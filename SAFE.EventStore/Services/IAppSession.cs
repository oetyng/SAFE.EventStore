using System.Threading.Tasks;

namespace SAFE.EventStore.Services
{
    public interface IAppSession
    {
        Task CheckAndReconnect();
        void Dispose();
        bool IsAuthenticated { get; }
        Task<string> GenerateAppRequestAsync();
        Task HandleUrlActivationAsync(string encodedUrl);
    }
}