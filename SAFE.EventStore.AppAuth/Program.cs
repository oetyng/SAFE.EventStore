using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace SAFE.EventStore.AppAuth
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine();
                Console.WriteLine("Confirming authorisation..");

                if (args.Length == 0 || !args[0].StartsWith("safe-"))
                    throw new ArgumentException("Missing or invalid argument.");

                var encodedUrl = args[0].Split(':')[1];

                var client = new HttpClient();
                var content = JsonConvert.SerializeObject(new { EncodedUrl = encodedUrl });
                var result = client.PostAsync("http://localhost:52794/api/auth", new StringContent(content, Encoding.UTF8, "application/json"))
                    .GetAwaiter()
                    .GetResult();

                Console.WriteLine();
                Console.WriteLine("Authorisation confirmed.");

                Process.Start("http://localhost:52794/Databases");
                //Process.Start("safe:localhost://p:52794/Databases");

                //Console.WriteLine(result.ReasonPhrase);
                //Console.WriteLine($"Request result: {msg}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message + ex.StackTrace}");
                Console.WriteLine("Press any key to exit.");
                Console.ReadKey();
            }
            //"Error Code: -2000. Description: Unexpected (probably a logic error): Could not connect to the SAFE Network"
        }
    }
}
