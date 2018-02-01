using Newtonsoft.Json;
using System.Collections.Generic;

namespace SAFE.DotNET.Models
{
    public struct DataArray
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("data")]
        public List<byte> Data { get; set; }
    }
}