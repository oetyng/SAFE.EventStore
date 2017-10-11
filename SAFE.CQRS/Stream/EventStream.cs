using SAFE.SystemUtils;
using System;
using System.Threading.Tasks;

namespace SAFE.CQRS
{
    public interface IStateManager
    {
        Task<Result<bool>> TrySetStateAsync(string key, int expectedVersion, object value);
        Task<Result<bool>> RemoveStateAsync(string key);
        Task<Result<T>> TryGetStateAsync<T>(string key);
    }

    //// Manages 1 strean.
    //// Should in ctor have AppId, DatabaseId
    //class MutableEventStream : IMutableEventStream
    //{

    //    readonly string _streamName;
    //    readonly Guid _streamId;

    //    //IStateManager _stateManager;

    //    Dictionary<int, string> _streamEventKeys { get; set; }

    //    Dictionary<Guid, EventBatch> _stream;
    //    int _streamVersion;

    //    public int Version { get { return _streamEventKeys.Count - 1; } }

    //    public MutableEventStream()
    //    {
    //    }

    //    public async Task Init(Guid streamId, string streamName)
    //    {
    //        StreamId = streamId;
    //        StreamName = streamName;

    //        //_stream = await _stateManager.GetOrAddAsync<IReliableDictionary<Guid, BatchEventRelayMsg>>(tx, StreamKey);
    //        //_streamVersions = await _stateManager.GetOrAddAsync<IReliableDictionary<string, int>>(tx, "StreamVersions");
    //    }

    //    public string StreamKey { get { return $"{StreamName}-{StreamId}"; } }
    //    public string StreamName { get; private set; }
    //    public Guid StreamId { get; private set; }


    //    public Task<IEnumerable<Event>> LoadAll()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public Task<IEnumerable<EventData>> LoadAllRaw()
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public Task<IEnumerable<Event>> LoadTo(int version)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public async Task RevertTo(int version)
    //    {
    //        //var toRemove = StreamEventKeys
    //        //    .Where(c => c.Key > version)
    //        //    .ToList();
    //        //var toKeep = StreamEventKeys
    //        //    .Where(c => version >= c.Key)
    //        //    .ToDictionary(x => x.Key, x => x.Value);

    //        //await _stateManager.TrySetStateAsync(GetNetworkStreamKey(), toKeep); // remove key from reliable state

    //        //foreach (var key in toRemove)
    //        //{
    //        //    await _stateManager.RemoveStateAsync(key.Value); // remove the reliable value by its key.
    //        //    StreamEventKeys.Remove(key.Key); // We must remove key from this volatile state too, as we will access this dict from GetInstance<T>() at last line in this method.
    //        //    // TEST (is the actual removal persisted only after actor method exit?? Seems so..)
    //        //    var supposedlyRemoved = await _stateManager.TryGetStateAsync<EventData>(key.Value); // is the actual removal persisted only after actor method exit?? Seems so..
    //        //    if (supposedlyRemoved.OK)
    //        //        Console.WriteLine("This is strange");
    //        //    // TEST (is the actual removal persisted only after actor method exit?? Seems so..)
    //        //};
    //    }

    //    public Task Save(IEnumerable<EventData> events)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public async Task<Result<bool>> TryAddRange(EventBatch range)
    //    {
    //        if (_stream.ContainsKey(range.CausationId))
    //            return Result.OK(false);
    //        var events = range.GetBody();
    //        var firstEvent = events.First();
    //        //var streamKey = firstEvent.StreamKey;

    //        var expectedVersion = _streamVersion;
            
    //        if (firstEvent.SequenceNumber != expectedVersion)
    //            return Result.Fail<bool>($"{StreamKey} version exception! Expected: {expectedVersion}, but got {firstEvent.SequenceNumber}");
    //        if (events.Count != range.Count)
    //            return Result.Fail<bool>($"{StreamKey} event count corruption! Expected {range.Count} but got {events.Count}");

    //        //int[] streamSequence = new int[events.Count];
    //        //var first = firstEvent.SequenceNumber;
    //        //for (int i = 0; i < events.Count; i++)
    //        //    streamSequence[i] = events[i].SequenceNumber - first;

    //        //bool isInSequence = streamSequence.SequenceEqual(Enumerable.Range(0, streamSequence.Count())); // useful to have as extension method list.InSequenceBy<int>(x => x.SomeIntProperty); and list.InSequenceByDescending<int>(x => x.SomeIntProperty);

    //        //if (!isInSequence)
    //        //    return Result.Fail<bool>("Non continuous sequence provided. Events must be in sequence.");

            
    //        var storageResult = await _stateManager.TrySetStateAsync(_streamKey.Key, expectedVersion, range);

    //        if (storageResult.OK) // store to cache
    //        {
    //            _stream[range.CausationId] = range;

    //            var nextExpectedVersion = expectedVersion + range.Count;
    //            _streamVersion = nextExpectedVersion;

    //            return Result.OK(true);
    //        }
    //        else
    //            return Result.Fail<bool>(storageResult.ErrorMsg);
    //    }

    //    public Task Commit()
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
}