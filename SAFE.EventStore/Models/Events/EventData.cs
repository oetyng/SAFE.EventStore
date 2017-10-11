using Newtonsoft.Json;
using SAFE.SystemUtils.Events;
using System;

namespace SAFE.EventStore.Models
{
    public class EventData
    {
        Event _deserialized;

        [JsonConstructor]
        EventData()
        { }

        public EventData(byte[] payload,
            Guid correlationId,
            Guid causationId,
            string eventClrType,
            Guid id,
            string name,
            int sequenceNumber, DateTime timeStamp)
        {
            Payload = payload;
            CorrelationId = correlationId;
            CausationId = causationId;
            EventClrType = eventClrType;
            Id = id;
            Name = name;
            SequenceNumber = sequenceNumber;
            TimeStamp = timeStamp;
        }

        /// <summary>
        /// Use this method to deserialize the payload into its correct type
        /// using the specified function for deserialization passed as parameter.
        /// </summary>
        /// <typeparam name="T">The type or basetype of the serialized event.</typeparam>
        /// <param name="deserialize">Deserialization function of choice.</param>
        /// <returns>The deserialized event or null if casting failed.</returns>
        public T GetDeserialized<T>(Func<byte[], string, T> deserialize) where T : Event
        {
            if (_deserialized == null)
                _deserialized = deserialize(Payload, EventClrType);
            return _deserialized as T;
        }

        public byte[] Payload { get; private set; }

        public string EventClrType { get; private set; }

        public Guid Id { get; private set; }

        public int SequenceNumber { get; private set; }

        public string Name { get; private set; }

        public DateTime TimeStamp { get; private set; }

        public Guid CorrelationId { get; private set; }
        
        public Guid CausationId { get; private set; }
    }
}