using System;

namespace SAFE.EventStore
{
    //public class ActorStream
    //{
    //    IActorStateManager _stateManager;
    //    readonly string StreamEventKeysId = "StreamEventKeys";
    //    object _instance;

    //    readonly string NonSynchedEventKeysId = "UnSynchedEventKeys";

    //    public ActorStream(IActorStateManager stateManager)
    //    {
    //        _stateManager = stateManager;
    //        Metadata = new Dictionary<string, object>();
    //        StreamEventKeys = new Dictionary<int, string>();
    //    }

    //    public async Task Init(List<EventData> history)
    //    {
    //        if (!IsInSequence(history))
    //            throw new InvalidOperationException("Non continuous sequence provided. Events must be in sequence.");

    //        await Init(history.First().StreamName, history.First().StreamId);

    //        if (StreamEventKeys.Count > 0)
    //            throw new InvalidOperationException("Version exception: Expected empty stream.");
    //        await SaveEvents(history);
    //    }

    //    public async Task Init(string streamName, Guid streamId)
    //    {
    //        StreamName = streamName;
    //        StreamId = streamId;
    //        Metadata = await _stateManager.GetOrAddStateAsync("Metadata", Metadata);
    //        StreamEventKeys = await _stateManager.GetOrAddStateAsync(StreamEventKeysId, StreamEventKeys);

    //        await _stateManager.GetOrAddStateAsync(NonSynchedEventKeysId, new List<string>());
    //    }

    //    public string StreamKey { get { return $"{StreamName}-{StreamId}"; } }

    //    public string StreamName { get; private set; }

    //    public Guid StreamId { get; private set; }

    //    public Dictionary<string, object> Metadata { get; private set; }

    //    public int Version { get { return StreamEventKeys.Count - 1; } }
    //    Dictionary<int, string> StreamEventKeys { get; set; }

    //    // this is more risky than reverting by CausationId..
    //    // Reverting by causation id is 100% safe. But sending in an integer..
    //    // If integer is corrupted, all state could be wiped.
    //    // Now.. it wouldn't be total catastrophy, since any valid events that were removed,
    //    // should be stored in EventRouter too (and Projections, and external backup..).
    //    // I.e. if it comes to that, we will be able to recover by synching with those. Todo: synch logic.
    //    public async Task<T> RevertTo<T>(int version) where T : Aggregate
    //    {
    //        var count = version + 1;
    //        var toRemove = StreamEventKeys
    //            .Skip(count)
    //            .ToList();
    //        var toKeep = StreamEventKeys
    //            .Take(count)
    //            .ToDictionary(x => x.Key, x => x.Value);
    //        await _stateManager.SetStateAsync(StreamEventKeysId, toKeep); // remove key from reliable state

    //        foreach (var key in toRemove)
    //        {
    //            await _stateManager.RemoveStateAsync(key.Value); // remove the reliable value by its key.
    //            StreamEventKeys.Remove(key.Key); // We must remove key from this volatile state too, as we will access this dict from GetInstance<T>() at last line in this method.
    //            // TEST (is the actual removal persisted only after actor method exit?? Seems so..)
    //            var supposedlyRemoved = await _stateManager.TryGetStateAsync<EventData>(key.Value); // is the actual removal persisted only after actor method exit?? Seems so..
    //            if (supposedlyRemoved.HasValue)
    //                Console.WriteLine("This is strange");
    //            // TEST (is the actual removal persisted only after actor method exit?? Seems so..)
    //        };

    //        _instance = null;
    //        return await GetInstance<T>();
    //    }

    //    public async Task<List<EventData>> Save(Aggregate instance, Guid correlationId, Guid causationId)
    //    {
    //        if (instance == null)
    //            throw new InvalidOperationException("No hydrated instance to save");

    //        var events = instance.GetUncommittedEvents()
    //            .Select(ev =>
    //            {
    //                var type = ev.GetType();
    //                return new EventData(
    //                        ev.AsBytes(),
    //                        correlationId,
    //                        causationId,
    //                        type.AssemblyQualifiedName,
    //                        ev.Id,
    //                        type.Name,
    //                        ev.SequenceNumber,
    //                        StreamName,
    //                        StreamId,
    //                        ev.TimeStamp);
    //            }).ToList();

    //        if (events.Count == 0) return new List<EventData>();
    //        bool isInSequence = IsInSequence(events);
    //        if (!isInSequence)
    //            throw new InvalidOperationException("Non continuous sequence provided. Events must be in sequence.");

    //        await SaveEvents(events);

    //        instance.ClearUncommittedEvents();

    //        return events;
    //    }

    //    async Task SaveEvents(List<EventData> events)
    //    {
    //        var nonSynched = await _stateManager.GetStateAsync<List<string>>(NonSynchedEventKeysId);
    //        // var streamEventKeys = await _stateManager.GetStateAsync<List<string>>(StreamEventKeysId); // Consider using this instead of the volatile dict.
    //        foreach (var ev in events)
    //        {
    //            var key = GetStateKey(ev);
    //            StreamEventKeys[ev.SequenceNumber] = key;
    //            await _stateManager.SetStateAsync(key, ev);

    //            nonSynched.Add(key);
    //        }
    //        await _stateManager.SetStateAsync(StreamEventKeysId, StreamEventKeys);
    //        await _stateManager.SetStateAsync(NonSynchedEventKeysId, nonSynched);
    //    }

    //    // Events not yet synched with remote storage.
    //    public async Task<List<EventData>> GetNonSynchedEvents()
    //    {
    //        var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, EventData>>();

    //        var nonSynched = await _stateManager.GetStateAsync<List<string>>(NonSynchedEventKeysId);

    //        var tasks = nonSynched.Select(async eventKey => // consider parallel instead. The state is maybe not gotten from disk (I/O) one by one, but entire dict at once, in which case async/await is not useful.
    //        {
    //            var evData = (await _stateManager.GetStateAsync<EventData>(eventKey));
    //            bag.Add(new KeyValuePair<int, EventData>(evData.SequenceNumber, evData));
    //        });
    //        await Task.WhenAll(tasks);
    //        return bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();
    //    }

    //    public async Task ClearNonSynched()
    //    {
    //        await _stateManager.SetStateAsync(NonSynchedEventKeysId, new List<string>());
    //        await _stateManager.SaveStateAsync();
    //    }

    //    bool IsInSequence(List<EventData> events)
    //    {
    //        if (events.First().SequenceNumber != StreamEventKeys.Count)
    //            throw new InvalidOperationException("Version exception");

    //        int[] streamSequence = new int[events.Count];
    //        var first = events.First().SequenceNumber;
    //        for (int i = 0; i < events.Count; i++)
    //            streamSequence[i] = events[i].SequenceNumber - first;

    //        bool isInSequence = streamSequence.SequenceEqual(Enumerable.Range(0, streamSequence.Count())); // useful to have as extension method list.InSequenceBy<int>(x => x.SomeIntProperty); and list.InSequenceByDescending<int>(x => x.SomeIntProperty);

    //        return isInSequence;
    //    }

    //    // if error on save, sets the local instance to empty instance,
    //    // rebuilds state from historic events, apply cmd again. then throw if still fail.
    //    public async Task<T> GetInstance<T>() where T : Aggregate
    //    {
    //        if (_instance != null)
    //            return (T)_instance;
    //        var instance = Activator.CreateInstance<T>();

    //        // dictionaries do not have a guearanteed order, so we must order by the key, which is a strictly incrementing integer (the event number).
    //        var events = await GetOrderedEvents();

    //        foreach (var evt in events)
    //            instance.BuildFromHistory(evt); // await evt

    //        _instance = instance;
    //        return instance;
    //    }

    //    // dictionaries do not have a guearanteed order, so we must order by the key, which is a strictly incrementing integer (the event number).
    //    async Task<List<Event>> GetOrderedEvents()
    //    {
    //        var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, Event>>();

    //        var tasks = StreamEventKeys.Select(async eventKey => // consider parallel instead. The state is maybe not gotten from disk (I/O) one by one, but entire dict at once, in which case async/await is not useful.
    //        {
    //            var evData = (await _stateManager.GetStateAsync<EventData>(eventKey.Value));
    //            var @event = evData.GetDeserialized((bytes, type) =>
    //                            (Event)bytes.Parse(type));
    //            bag.Add(new KeyValuePair<int, Event>(eventKey.Key, @event));
    //        });
    //        await Task.WhenAll(tasks);
    //        return bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();
    //    }


    //    // Only used with utmost care!
    //    // For now, only possible to edit timestamp.
    //    internal async Task<Transport.RequestResult> Edit(Guid eventId, string propertyName, object value)
    //    {
    //        if (propertyName != "TimeStamp" || value == null || value.GetType() != typeof(DateTime))
    //            return Transport.RequestResult.BadRequest("Can only edit timestamp for now.");

    //        var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, EventData>>();

    //        var tasks = StreamEventKeys.Select(async eventKey =>
    //        {
    //            var ev = (await _stateManager.GetStateAsync<EventData>(eventKey.Value));
    //            bag.Add(new KeyValuePair<int, EventData>(eventKey.Key, ev));
    //        });
    //        await Task.WhenAll(tasks);
    //        var events = bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();

    //        var evData = events.SingleOrDefault(c => c.Id == eventId);
    //        if (evData == null)
    //            return Transport.RequestResult.BadRequest();

    //        // The new timestamp must still be a value between previous and next events' timestamps.
    //        if (StreamEventKeys.Keys.Max() > evData.SequenceNumber && ((DateTime)value) >= bag.Single(c => c.Key == evData.SequenceNumber + 1).Value.TimeStamp)
    //            return Transport.RequestResult.BadRequest("Can not change order of events.");
    //        if (StreamEventKeys.Keys.Min() < evData.SequenceNumber && ((DateTime)value) <= bag.Single(c => c.Key == evData.SequenceNumber - 1).Value.TimeStamp)
    //            return Transport.RequestResult.BadRequest("Can not change order of events.");

    //        var key = GetStateKey(evData);
    //        var evtToEdit = evData.GetDeserialized((b, t) => (Event)b.Parse(t));

    //        // TEMP, non generic as we now only allow timestamp edit!
    //        var prop = typeof(Event).GetProperty("TimeStamp");
    //        prop.SetValue(evtToEdit, value);

    //        var modifiedEvt = new EventData(evtToEdit.AsBytes(),
    //            evData.CorrelationId,
    //            evData.CausationId,
    //            evData.EventClrType,
    //            evData.Id,
    //            evData.Name,
    //            evData.SequenceNumber,
    //            evData.StreamName,
    //            evData.StreamId,
    //            evtToEdit.TimeStamp);

    //        await _stateManager.SetStateAsync(key, modifiedEvt);
    //        await _stateManager.SaveStateAsync();

    //        return Transport.RequestResult.OK();
    //    }


    //    // Only used with utmost care!
    //    // For now, only possible to delete very last event in a stream.
    //    internal async Task<Transport.RequestResult> Delete(Guid eventId)
    //    {
    //        var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, EventData>>();

    //        var tasks = StreamEventKeys.Select(async eventKey =>
    //        {
    //            var evData = (await _stateManager.GetStateAsync<EventData>(eventKey.Value));
    //            bag.Add(new KeyValuePair<int, EventData>(eventKey.Key, evData));
    //        });
    //        await Task.WhenAll(tasks);
    //        var events = bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();

    //        var ev = events.SingleOrDefault(c => c.Id == eventId);
    //        if (ev == null)
    //            return Transport.RequestResult.NotModified();
    //        if (ev.Id != events.Last().Id) // TEMP rule
    //            return Transport.RequestResult.BadRequest("Can only remove the very last event of a stream for now! (Stream version is at ");

    //        var key = GetStateKey(ev);
    //        if (StreamEventKeys.Keys.Max() != ev.SequenceNumber) // TEMP rule
    //            return Transport.RequestResult.BadRequest("Can only remove the very last event of a stream for now! (Stream version is at ");

    //        StreamEventKeys.Remove(ev.SequenceNumber);
    //        if (!await _stateManager.TryRemoveStateAsync(key))
    //        {
    //            StreamEventKeys[ev.SequenceNumber] = key;
    //            return Transport.RequestResult.BadRequest($"Could not remove {eventId}!");
    //        }

    //        await _stateManager.SetStateAsync(StreamEventKeysId, StreamEventKeys);
    //        await _stateManager.SaveStateAsync();

    //        return Transport.RequestResult.OK();
    //    }

    //    string GetStateKey(EventData data)
    //    {
    //        var key = data.SequenceNumber + "@" + data.Name;
    //        return key;
    //    }
    //}
}
