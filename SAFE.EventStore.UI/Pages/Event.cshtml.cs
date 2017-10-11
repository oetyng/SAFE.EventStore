using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SAFE.EventStore.UI.Models;
using SAFE.SystemUtils;

namespace SAFE.EventStore.UI.Pages
{
    public class EventModel : AuthPageModel
    {
        public EventModel(Setup.UISession session)
            : base(session)
        { }

        public Event Event { get; private set; }

        public async Task<IActionResult> OnGet(string databaseId, string streamKey, Guid eventId)
        {
            if (!Authenticated)
                return RedirectToPage("/Login");

            try
            {
                var service = _serviceFactory();
                
                var streamResult = await service.GetStreamAsync(databaseId, streamKey);
                // TODO: Handle error result
                var stream = streamResult.Value;
                Event = stream.Data.Select(e => new Event
                {
                    CausationId = e.CausationId,
                    CorrelationId = e.CorrelationId,
                    Id = e.Id,
                    Name = e.Name,
                    SequenceNr = e.SequenceNumber,
                    StreamId = stream.StreamId,
                    StreamName = stream.StreamName,
                    TimeStamp = e.TimeStamp,
                    Json = e.Payload.GetJson()
                }).Single(ev => ev.Id == eventId);

                Message = $"Displaying event: {Event.SequenceNr}@{Event.Name} with id {eventId} of stream {streamKey}.";
            }
            catch (Exception ex)
            {
                return SetErrorMsg(ex);
            }
            return Page();
        }
    }
}