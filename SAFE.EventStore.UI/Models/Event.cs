using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SAFE.EventStore.UI.Models
{
    public class Event
    {
        public int SequenceNr { get; set; }
        public string Name { get; set; }

        //public string Key { get { return $"{SequenceNr}@{Name}"; } }

        public Guid Id { get; set; }

        public string StreamName { get; set; }
        public Guid StreamId { get; set; }

        public Guid CorrelationId { get; set; }
        public Guid CausationId { get; set; }

        public DateTime TimeStamp { get; set; }
        public string Json { get; set; }
    }
}