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
        readonly EventStorage _storage;
        readonly ConcurrentDictionary<string, Aggregate> _cache = new ConcurrentDictionary<string, Aggregate>();

        public Repository(EventStorage storage)
        {
            _storage = storage;
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
            var writer = _storage.GetStream(streamKey);
            var streamResult = await writer.GetStreamAsync(streamKey);
            // todo, handle no exist
            var stream = streamResult.Value;

            if (!_cache.TryGetValue(streamKey, out Aggregate cached))
            {
                var ar = Activator.CreateInstance<T>();

                var events = stream.Data
                    .Select(x => x.GetDeserialized((b, t) => (Event)b.Parse(t)));

                foreach (var e in events)
                    ar.BuildFromHistory(e);

                _cache[streamKey] = ar;

                return ar;
            }

            var newEvents = stream.Data
                .Where(d => d.SequenceNumber > cached.Version)
                .Select(x => x.GetDeserialized((b, t) => (Event)b.Parse(t)));

            foreach (var e in newEvents)
                cached.BuildFromHistory(e);

            return (T)cached;
        }

        internal async Task<bool> Save(EventBatch batch)
        {
            var writer = _storage.GetStream(batch.StreamKey);
            
            await writer.StoreBatchAsync(batch);

            return true;
        }
    }

    public class Context<TCmd> where TCmd : Cmd
    {
        readonly TCmd _cmd;
        readonly Repository _repo;
        Aggregate _instance;

        public Context(TCmd cmd, Repository repo)
        {
            _cmd = cmd;
            _repo = repo;
        }

        public async Task<bool> ExecuteAsync(Func<TCmd, Aggregate, Task<bool>> action)
        {
            if (_instance != null)
                throw new InvalidOperationException("Can only execute once!");
            _instance = await Locate(_cmd);
            return await action(_cmd, _instance);
        }

        public async Task<bool> CommitAsync()
        {
            if (_instance == null)
                throw new InvalidOperationException("Cannot commit before executing!");

            var events = _instance.GetUncommittedEvents();
            if (events.Count == 0)
                throw new InvalidOperationException("Already committed.");

            var data = events.Select(e => new EventData(
                e.AsBytes(), 
                _cmd.CorrelationId, 
                _cmd.Id, 
                e.GetType().AssemblyQualifiedName, 
                e.Id, 
                e.GetType().Name, 
                e.SequenceNumber, 
                e.TimeStamp))
            .ToList();

            var batch = new EventBatch(_instance.StreamKey, _cmd.Id, data);

            if (!await _repo.Save(batch))
                return false;

            _instance.ClearUncommittedEvents();
            return true;
        }

        async Task<Aggregate> Locate(Cmd cmd)
        {
            var streamKey = StreamKeyFromCmd(cmd);
            return await _repo.GetAR<Aggregate>(streamKey);
        }

        string StreamKeyFromCmd(Cmd cmd)
        {
            return "steramKey";
        }
    }

    public abstract class Cmd
    {
        public Guid Id { get; private set; }
        public Guid TargetId { get; private set; }
        public Guid CorrelationId { get; internal set; }

        public Cmd(Guid targetId)
        {
            TargetId = targetId;
            Id = SequentialGuid.NewGuid();
        }
    }
}