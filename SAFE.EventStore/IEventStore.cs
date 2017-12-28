using System.Collections.Generic;
using System.Threading.Tasks;
using SAFE.EventStore.Models;
using SAFE.SystemUtils;

namespace SAFE.EventStore
{
    public interface IEventStore
    {
        Task CreateDbAsync(string databaseId);
        void Dispose();
        Task<List<DatabaseId>> GetDatabaseIdsAsync();
        Task<Result<ReadOnlyStream>> GetStreamAsync(string databaseId, string streamKey);
        Task<List<string>> GetStreamKeysAsync(string databaseId, string streamType);
        Task<List<string>> GetCategoriesAsync(string databaseId);
        Task<Result<bool>> StoreBatchAsync(string databaseId, EventBatch batch);
    }
}