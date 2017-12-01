using SAFE.EventStore.Models;
using SAFE.SystemUtils;
using System.Threading.Tasks;

namespace SAFE.EventStore
{
    public interface IEventStreamHandler
    {
        Task<Result<ReadOnlyStream>> GetStreamAsync(string streamKey);
        Task<Result<bool>> StoreBatchAsync(EventBatch batch);
    }
}