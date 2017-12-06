using SAFE.EventStore;
using SAFE.EventStore.Models;
using SAFE.EventStore.Services;
using SAFE.SystemUtils;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace SAFE.CQRS.Stream
{
    public class EventStreamCache
    {
        ConcurrentDictionary<string, IEventStreamHandler> _cache = new ConcurrentDictionary<string, IEventStreamHandler>();

        IEventStore _store;
        readonly string _databaseId;
        object _accessLock = new object();

        public EventStreamCache(IEventStore store, string databaseId)
        {
            if (store == null || databaseId == null)
                throw new ArgumentNullException();

            _store = store;
            _databaseId = databaseId;
        }

        public IEventStreamHandler GetStreamHandler(string streamKey)
        {
            lock (_accessLock) // strictly not needed, since there is no local state in the EventStreamWriter, so it doesn't matter if various instances are used.
            {
                if (!_cache.TryGetValue(streamKey, out IEventStreamHandler stream))
                {
                    stream = new EventStreamHandler(_store, _databaseId);
                    _cache[streamKey] = stream;
                }
                return stream;
            }
        }

        class EventStreamHandler : IEventStreamHandler
        {
            IEventStore _store;
            string _databaseId;

            internal EventStreamHandler(IEventStore store, string databaseId)
            {
                _store = store;
                _databaseId = databaseId;
            }

            Task<Result<ReadOnlyStream>> IEventStreamHandler.GetStreamAsync(string streamKey)
            {
                return _store.GetStreamAsync(_databaseId, streamKey);
            }

            Task<Result<bool>> IEventStreamHandler.StoreBatchAsync(EventBatch batch)
            {
                return _store.StoreBatchAsync(_databaseId, batch);
            }
        }
    }
}