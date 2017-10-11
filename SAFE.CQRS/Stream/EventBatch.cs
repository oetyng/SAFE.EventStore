using SAFE.EventStore.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SAFE.CQRS
{
    //public class EventBatch
    //{
    //    internal readonly Guid CausationId;
    //    internal readonly int Count;
    //    List<EventData> _events;

    //    public readonly string StreamKey;

    //    public EventBatch(string streamKey, Guid causationId, List<EventData> events)
    //    {
    //        if (!IsInSequence(events))
    //            throw new ArgumentOutOfRangeException();

    //        _events = events;
    //        Count = events.Count;
    //        CausationId = causationId;
    //        StreamKey = streamKey;
    //    }

    //    internal List<EventData> GetBody()
    //    {
    //        return _events.ToList();
    //    }

    //    bool IsInSequence(List<EventData> events)
    //    {
    //        var firstEvent = events.First();
    //        int[] streamSequence = new int[events.Count];
    //        var first = firstEvent.SequenceNumber;
    //        for (int i = 0; i < events.Count; i++)
    //            streamSequence[i] = events[i].SequenceNumber - first;

    //        bool isInSequence = streamSequence.SequenceEqual(Enumerable.Range(0, streamSequence.Count())); // useful to have as extension method list.InSequenceBy<int>(x => x.SomeIntProperty); and list.InSequenceByDescending<int>(x => x.SomeIntProperty);

    //        return isInSequence;
    //    }
    //}
}