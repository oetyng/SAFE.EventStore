using SAFE.CQRS.Stream;
using SAFE.EventStore.Models;
using SAFE.SystemUtils;
using SAFE.SystemUtils.Events;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.CQRS
{
    public class Repository
    {
        readonly EventStreamCache _streamCache;
        readonly ConcurrentDictionary<string, Aggregate> _currentStateCache = new ConcurrentDictionary<string, Aggregate>();

        public Repository(EventStreamCache streamCache)
        {
            _streamCache = streamCache;
        }

        /// <summary>
        /// Caches instances.
        /// Checks network for new events, 
        /// and applies them to cached instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="streamKey">[stream name]@[guid id]</param>
        /// <returns></returns>
        public async Task<T> GetAR<T>(string streamKey) where T : Aggregate
        {
            var reader = _streamCache.GetStreamHandler(streamKey);
            var streamResult = await reader.GetStreamAsync(streamKey); // todo: pass in cached state version, and only load newer versions
            // todo, handle no exist
            var stream = streamResult.Value;

            if (!_currentStateCache.TryGetValue(streamKey, out Aggregate cached))
            {
                var ar = Activator.CreateInstance<T>();

                var events = stream.Data
                    .Select(x => x.GetDeserialized((b, t) => (Event)b.Parse(t)));

                foreach (var e in events)
                    ar.BuildFromHistory(e);

                _currentStateCache[streamKey] = ar;

                return ar;
            }
            else
            {
                var newEvents = stream.Data
                    .Where(d => d.SequenceNumber > cached.Version)
                    .Select(x => x.GetDeserialized((b, t) => (Event)b.Parse(t)));

                foreach (var e in newEvents)
                    cached.BuildFromHistory(e);
            }

            return (T)cached;
        }

        internal async Task<bool> Save(EventBatch batch)
        {
            var writer = _streamCache.GetStreamHandler(batch.StreamKey);

            var result = await writer.StoreBatchAsync(batch);

            return result.OK; // OK will be true also on idempotent writes
        }
    }   
}