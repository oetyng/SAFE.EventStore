using SAFE.EventStore.Models;
using SAFE.SystemUtils;
using SAFE.SystemUtils.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.CQRS
{
    public class ActorStream : IActorStream
    {
        //readonly string StreamEventKeysId = "StreamEventKeys";

        IMutableEventStream _storage;
        object _instance;

        public ActorStream(IMutableEventStream storage)
        {
            _storage = storage;
            Metadata = new Dictionary<string, object>();
            //StreamEventKeys = new Dictionary<int, string>();
        }

        public async Task Init(string streamName, Guid streamId, List<EventData> history)
        {
            Validate(history);

            await Init(streamName, streamId);

            if (_storage.Version > -1)
                throw new InvalidOperationException("Version exception: Expected empty stream.");
            await SaveEvents(/*causation, */history);
        }

        void Validate(List<EventData> history)
        {
            if (!IsInSequence(history))
                throw new InvalidOperationException("Non continuous sequence provided. Events must be in sequence.");

            //var streamName = history.First().StreamName;
            //var streamId = history.First().StreamId;

            //if (!history.TrueForAll(ev => ev.StreamId == streamId && ev.StreamName == ev.StreamName))
            //    throw new InvalidOperationException("Events must belong to same stream.");
        }

        public async Task Init(string streamName, Guid streamId)
        {
            StreamName = streamName;
            StreamId = streamId;

            //Metadata = await _stateManager.GetOrAddStateAsync("Metadata", Metadata);
            //StreamEventKeys = await _stateManager.GetOrAddStateAsync(StreamEventKeysId, StreamEventKeys);

            //await _stateManager.GetOrAddStateAsync(NonSynchedEventKeysId, new List<string>());
        }

        public string StreamKey { get { return $"{StreamName}@{StreamId}"; } }

        public string StreamName { get; private set; }

        public Guid StreamId { get; private set; }

        public int Version { get { return _storage.Version; } }

        public Dictionary<string, object> Metadata { get; private set; }

        // this is more risky than reverting by CausationId..
        // Reverting by causation id is almost(*) safe. But sending in an integer..
        // If integer is corrupted, all state could be wiped.
        // *(if you send in a cmd that you know will cause exception, even though state has been saved, 
        // and you have set cmd id to an id of previous cmds, then you could possibly delete state by this).
        public async Task<T> RevertTo<T>(int version) where T : Aggregate
        {
            await _storage.RevertTo(version);

            _instance = null;
            return await GetInstance<T>();
        }

        // generic handling in this method
        public async Task<List<EventData>> Save(Aggregate instance, Guid causationId, Guid correlationId)
        {
            if (instance == null)
                throw new InvalidOperationException("No hydrated instance to save");

            var events = instance.GetUncommittedEvents()
                .Select(ev =>
                {
                    var type = ev.GetType();
                    return new EventData(
                            ev.AsBytes(),
                            correlationId,
                            causationId,
                            type.AssemblyQualifiedName,
                            ev.Id,
                            type.Name,
                            ev.SequenceNumber,
                            ev.TimeStamp);
                }).ToList();

            if (events.Count == 0) return new List<EventData>();
            bool isInSequence = IsInSequence(events);
            if (!isInSequence)
                throw new InvalidOperationException("Non continuous sequence provided. Events must be in sequence.");

            await SaveEvents(events);

            instance.ClearUncommittedEvents();

            return events;
        }

        // storage specific handling in this method
        async Task SaveEvents(List<EventData> events)
        {
            await _storage.Save(events);
        }

        // generic handling in this method
        bool IsInSequence(List<EventData> events)
        {
            if (events.First().SequenceNumber != (Version + 1))
                throw new InvalidOperationException("Version exception");

            int[] streamSequence = new int[events.Count];
            var first = events.First().SequenceNumber;
            for (int i = 0; i < events.Count; i++)
                streamSequence[i] = events[i].SequenceNumber - first;

            bool isInSequence = streamSequence.SequenceEqual(Enumerable.Range(0, streamSequence.Count())); // useful to have as extension method list.InSequenceBy<int>(x => x.SomeIntProperty); and list.InSequenceByDescending<int>(x => x.SomeIntProperty);

            return isInSequence;
        }

        // generic handling in this method
        // if error on save, sets the local instance to empty instance,
        // rebuilds state from historic events, apply cmd again. then throw if still fail.
        public async Task<T> GetInstance<T>() where T : Aggregate
        {
            if (_instance != null)
                return (T)_instance;
            var instance = Activator.CreateInstance<T>();

            // dictionaries do not have a gearanteed order, so we must order by the key, which is a strictly incrementing integer (the event number).
            var events = await GetOrderedEvents();

            foreach (var evt in events)
                instance.BuildFromHistory(evt); // await evt

            _instance = instance;
            return instance;
        }

        // storage specific handling in this method
        // dictionaries do not have a guearanteed order, so we must order by the key, which is a strictly incrementing integer (the event number).
        async Task<List<Event>> GetOrderedEvents()
        {
            return (await _storage.LoadAll()).ToList();

            //var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, Event>>();

            //var tasks = StreamEventKeys.Select(async eventKey => // consider parallel instead. The state is maybe not gotten from disk (I/O) one by one, but entire dict at once, in which case async/await is not useful.
            //{
            //    var evData = (await _stateManager.GetStateAsync<EventData>(eventKey.Value));
            //    var @event = evData.GetDeserialized((bytes, type) =>
            //                    (Event)bytes.Parse(type));
            //    bag.Add(new KeyValuePair<int, Event>(eventKey.Key, @event));
            //});
            //await Task.WhenAll(tasks);
            //return bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();
        }

        #region Edit and delete
        //// storage specific handling in this method
        //// Only used with utmost care!
        //// For now, only possible to edit timestamp.
        //public async Task<bool> Edit(Guid eventId, string propertyName, object value)
        //{
        //    throw new NotImplementedException();
        //    //if (propertyName != "TimeStamp" || value == null || value.GetType() != typeof(DateTime))
        //    //    return Transport.RequestResult.BadRequest("Can only edit timestamp for now.");

        //    //#region storage specific
        //    //var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, EventData>>();

        //    //var tasks = StreamEventKeys.Select(async eventKey =>
        //    //{
        //    //    var ev = (await _stateManager.GetStateAsync<EventData>(eventKey.Value));
        //    //    bag.Add(new KeyValuePair<int, EventData>(eventKey.Key, ev));
        //    //});
        //    //await Task.WhenAll(tasks);
        //    //var events = bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();
        //    //#endregion storage specific

        //    ////var events = (await _storage.LoadAllRaw()).ToList();

        //    //var evData = events.SingleOrDefault(c => c.Id == eventId);
        //    //if (evData == null)
        //    //    return Transport.RequestResult.BadRequest();

        //    //// The new timestamp must still be a value between previous and next events' timestamps.
        //    //if (StreamEventKeys.Keys.Max() > evData.SequenceNumber && ((DateTime)value) >= bag.Single(c => c.Key == evData.SequenceNumber + 1).Value.TimeStamp)
        //    //    return Transport.RequestResult.BadRequest("Can not change order of events.");
        //    //if (StreamEventKeys.Keys.Min() < evData.SequenceNumber && ((DateTime)value) <= bag.Single(c => c.Key == evData.SequenceNumber - 1).Value.TimeStamp)
        //    //    return Transport.RequestResult.BadRequest("Can not change order of events.");

        //    //var key = GetStateKey(evData);
        //    //var evtToEdit = evData.GetDeserialized((b, t) => (Event)b.Parse(t));

        //    //// TEMP, non generic as we now only allow timestamp edit!
        //    //var prop = typeof(Event).GetProperty("TimeStamp");
        //    //prop.SetValue(evtToEdit, value);

        //    //var modifiedEvt = new EventData(evtToEdit.AsBytes(),
        //    //    evData.CorrelationId,
        //    //    evData.CausationId,
        //    //    evData.EventClrType,
        //    //    evData.Id,
        //    //    evData.Name,
        //    //    evData.SequenceNumber,
        //    //    evData.StreamName,
        //    //    evData.StreamId,
        //    //    evtToEdit.TimeStamp);

        //    //#region storage specific
        //    //await _stateManager.SetStateAsync(key, modifiedEvt);
        //    //await _stateManager.SaveStateAsync();
        //    //#endregion storage specific

        //    return Result.OK();
        //}

        //// storage specific handling in this method
        //// Only used with utmost care!
        //// For now, only possible to delete very last event in a stream.
        //public async Task<bool> Delete(Guid eventId)
        //{
        //    throw new NotImplementedException();
        //    //var bag = new System.Collections.Concurrent.ConcurrentBag<KeyValuePair<int, EventData>>();

        //    //var tasks = StreamEventKeys.Select(async eventKey =>
        //    //{
        //    //    var evData = (await _stateManager.GetStateAsync<EventData>(eventKey.Value));
        //    //    bag.Add(new KeyValuePair<int, EventData>(eventKey.Key, evData));
        //    //});
        //    //await Task.WhenAll(tasks);
        //    //var events = bag.OrderBy(c => c.Key).Select(c => c.Value).ToList();

        //    //var ev = events.SingleOrDefault(c => c.Id == eventId);
        //    //if (ev == null)
        //    //    return Transport.RequestResult.NotModified();
        //    //if (ev.Id != events.Last().Id) // TEMP rule
        //    //    return Transport.RequestResult.BadRequest("Can only remove the very last event of a stream for now! (Stream version is at ");

        //    //var key = GetStateKey(ev);
        //    //if (StreamEventKeys.Keys.Max() != ev.SequenceNumber) // TEMP rule
        //    //    return Transport.RequestResult.BadRequest("Can only remove the very last event of a stream for now! (Stream version is at ");

        //    //StreamEventKeys.Remove(ev.SequenceNumber);
        //    //if (!await _stateManager.TryRemoveStateAsync(key))
        //    //{
        //    //    StreamEventKeys[ev.SequenceNumber] = key;
        //    //    return Transport.RequestResult.BadRequest($"Could not remove {eventId}!");
        //    //}

        //    //await _stateManager.SetStateAsync(StreamEventKeysId, StreamEventKeys);
        //    //await _stateManager.SaveStateAsync();

        //    //return Result.OK();
        //}
        #endregion Edit and delete

        string GetStateKey(EventData data)
        {
            var key = data.SequenceNumber + "@" + data.Name;
            return key;
        }
    }
}
