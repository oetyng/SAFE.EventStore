using SAFE.EventStore.Models;
using SAFE.EventStore.Services;
using SAFE.SystemUtils;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace SAFE.CQRS.Stream
{
    public interface IEventStreamWriter
    {
        Task<Result<ReadOnlyStream>> GetStreamAsync(string streamKey);
        Task StoreBatchAsync(EventBatch batch);
    }

    public class EventStorage
    {
        ConcurrentDictionary<string, EventStreamWriter> _cache = new ConcurrentDictionary<string, EventStreamWriter>();

        IEventStoreService _store;
        readonly string _databaseId;

        public EventStorage(IEventStoreService store, string databaseId)
        {
            if (store == null || databaseId == null)
                throw new ArgumentNullException();

            _store = store;
            _databaseId = databaseId;
        }

        public IEventStreamWriter GetStream(string streamKey)
        {
            if (!_cache.TryGetValue(streamKey, out EventStreamWriter stream))
            {
                stream = new EventStreamWriter(_store, _databaseId);
                _cache[streamKey] = stream;
            }
            return stream;
        }

        class EventStreamWriter : IEventStreamWriter
        {
            IEventStoreService _store;
            string _databaseId;

            internal EventStreamWriter(IEventStoreService store, string databaseId)
            {
                _store = store;
                _databaseId = databaseId;
            }

            Task<Result<ReadOnlyStream>> IEventStreamWriter.GetStreamAsync(string streamKey)
            {
                return _store.GetStreamAsync(_databaseId, streamKey);
            }

            Task IEventStreamWriter.StoreBatchAsync(EventBatch batch)
            {
                return _store.StoreBatchAsync(_databaseId, batch);
            }
        }
    }
}