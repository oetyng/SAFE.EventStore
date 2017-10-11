using SAFE.EventStore.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace SAFE.CQRS
{
    public interface IActorStream//<TState> where TState : Aggregate
    {
        Task Init(string streamName, Guid streamId, List<EventData> history);

        Task Init(string streamName, Guid streamId);

        string StreamKey { get; }

        string StreamName { get; }

        Guid StreamId { get; }

        Dictionary<string, object> Metadata { get; }

        int Version { get; }

        // this is more risky than reverting by CausationId..
        // Reverting by causation id is 100% safe. But sending in an integer..
        // If integer is corrupted, all state could be wiped.
        Task<TState> RevertTo<TState>(int version) where TState : Aggregate;


        Task<List<EventData>> Save(Aggregate instance, Guid causation, Guid correlation);

        // if error on save, sets the local instance to empty instance,
        // rebuilds state from historic events, apply cmd again. then throw if still fail.
        Task<TState> GetInstance<TState>() where TState : Aggregate;

        //// Only used with utmost care!
        //Task<bool> Edit(Guid eventId, string propertyName, object value);

        //// Only used with utmost care!
        //Task<bool> Delete(Guid eventId);

        //string GetStateKey(TEventData data);
    }
}