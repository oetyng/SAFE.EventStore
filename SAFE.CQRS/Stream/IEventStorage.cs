using SAFE.EventStore.Models;
using SAFE.SystemUtils.Events;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SAFE.CQRS
{
    // Per stream
    public interface IMutableEventStream
    {
        int Version { get; }

        Task<IEnumerable<Event>> LoadAll();
        Task<IEnumerable<EventData>> LoadAllRaw();

        Task<IEnumerable<Event>> LoadTo(int version);

        Task RevertTo(int version);
        //Task<IEnumerable<Event>> LoadFromEndTo(int version);

        Task Save(/*RelayMsg causer, */IEnumerable<EventData> events);

        Task Commit();
    }
}