using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SAFE.EventStore.Services;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace SAFE.EventStore.UI.Controllers
{
    [Route("api/[controller]")]
    public class AuthController : Controller
    {
        IAppSession _session;

        public AuthController(Setup.UISession session)
        {
            _session = session.AppSession();
        }

        // POST api/auth
        [HttpPost]
        public async Task<IActionResult> Post([FromBody]AuthModel model)
        {
            try
            {
                await _session.HandleUrlActivationAsync(model.EncodedUrl);
                if (_session.IsAuthenticated)
                    return RedirectToPage("/Databases/Index");
                return BadRequest(model.EncodedUrl);
            }
            catch(Exception ex)
            {
                return BadRequest(ex.Message + ex.StackTrace);
            }
        }

        // GET api/auth?encodedurl=
        [HttpGet]
        public async Task<IActionResult> Get([FromQuery]string encodedUrl)
        {
            try
            {
                await _session.HandleUrlActivationAsync(encodedUrl);
                if (_session.IsAuthenticated)
                    return RedirectToPage("/Databases/Index");
                return BadRequest(encodedUrl);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message + ex.StackTrace);
            }
        }
    }

    public class AuthModel
    {
        [System.ComponentModel.DataAnnotations.Required]
        public string EncodedUrl { get; set; }
    }
}
