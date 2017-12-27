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

        [JsonProperty("payload")]
        public byte[] Payload { get; private set; }

        [JsonProperty("metaData")]
        public MetaData MetaData { get; private set; }

        public EventData(byte[] payload,
            Guid correlationId,
            Guid causationId,
            string eventClrType,
            Guid id,
            string name,
            int sequenceNumber, DateTime timeStamp)
        {
            Payload = payload;
            MetaData = new MetaData(
                correlationId,
                causationId,
                eventClrType,
                id,
                name,
                sequenceNumber, timeStamp);
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
                _deserialized = deserialize(Payload, MetaData.EventClrType);
            return _deserialized as T;
        }
    }
}