using Newtonsoft.Json;

namespace SAFE.DotNET.Models
{
    public class MessageBox
    {
        [JsonProperty("email_id")]
        public string EmailId { get; set; }

        [JsonProperty("inbox")]
        public DataArray Inbox { get; set; }

        [JsonProperty("archive")]
        public DataArray Archive { get; set; }

        [JsonProperty("email_enc_sk")]
        public string EmailEncSk { get; set; }

        [JsonProperty("email_enc_pk")]
        public string EmailEncPk { get; set; }
    }
}