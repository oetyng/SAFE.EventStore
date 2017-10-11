using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SAFE.EventStore.UI.Models;
using Microsoft.AspNetCore.Mvc;
using SAFE.SystemUtils;

namespace SAFE.EventStore.UI.Pages
{
    public class StreamModel : AuthPageModel
    {
        public StreamModel(Setup.UISession session)
            : base(session)
        { }

        public List<Event> Events { get; set; }

        public async Task<IActionResult> OnGetAsync(string databaseId, string streamKey)
        {
            if (!Authenticated)
                return RedirectToPage("/Login");

            try
            {
                var service = _serviceFactory();
                Message = $"Displaying events in stream {streamKey}.";
                var streamResult = await service.GetStreamAsync(databaseId, streamKey);
                // TODO: Handle error result
                var stream = streamResult.Value;
                Events = stream.Data.Select(e => new Event
                {
                    CausationId = e.CausationId,
                    CorrelationId = e.CorrelationId,
                    Id = e.Id, Name = e.Name,
                    SequenceNr = e.SequenceNumber,
                    StreamId = stream.StreamId,
                    StreamName = stream.StreamName,
                    TimeStamp = e.TimeStamp,
                    Json = e.Payload.GetJson()
                }).ToList();

                Events.Reverse();
            }
            catch (Exception ex)
            {
                return SetErrorMsg(ex);
            }
            return Page();
        }
    }
}