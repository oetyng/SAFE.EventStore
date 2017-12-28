using Newtonsoft.Json;
using SAFE.DotNET.Models;

namespace SAFE.EventStore.Models
{
    public class Database
    {
        [JsonProperty("database_id")]
        public string DbId { get; set; }

        [JsonProperty("categories")]
        public DataArray Categories { get; set; }
        
        [JsonProperty("data_enc_sk")]
        public string DataEncSk { get; set; }

        [JsonProperty("data_enc_pk")]
        public string DataEncPk { get; set; }

        [JsonIgnore]
        public ulong Version { get; internal set; }
    }
}