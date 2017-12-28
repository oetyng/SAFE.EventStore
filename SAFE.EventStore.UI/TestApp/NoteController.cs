using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SAFE.Utils;
using SAFE.TestCQRSApp;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace SAFE.EventStore.UI.Controllers
{
    [Route("api/[controller]")]
    public class NoteController : BaseController
    {
        NoteBookCmdHandler _cmdHandler;

        public NoteController(Setup.UISession session, NoteBookCmdHandler cmdHandler)
            : base(session)
        {
            _cmdHandler = cmdHandler;
        }

        public class NoteModel
        {
            [System.ComponentModel.DataAnnotations.Required]
            public string Note { get; set; }

            public int ExpectedVersion { get; set; } = -1;
        }

        // POST api/note
        [HttpPost]
        public async Task<IActionResult> Post([FromBody]NoteModel model)
        {
            try
            {
                if (!IsAuthenticated)
                    return Unauthorized();

                if (!ModelState.IsValid)
                    return BadRequest();

                var current = SystemUtils.SystemTime.UtcNow.ToString("yyyyMMdd HH:mm");
                var targetId = current.ToDeterministicGuid();
                var result = await _cmdHandler.Handle(new AddNote(targetId, model.ExpectedVersion, model.Note));

                if (result)
                    return Ok();
                else
                    return StatusCode(304);
            }
            catch(Exception ex)
            {
                // log ex.StackTrace
                return BadRequest(ex.Message);
            }
        }

        //// GET api/note
        //[HttpGet]
        //public async Task<IActionResult> Get()
        //{
        //    try
        //    {
        //        if (!IsAuthenticated)
        //            return Unauthorized();


        //    }
        //    catch (Exception ex)
        //    {
        //        return BadRequest(ex.Message + ex.StackTrace);
        //    }
        //}
    }
}