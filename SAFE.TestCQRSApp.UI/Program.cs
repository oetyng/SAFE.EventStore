using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;

namespace SAFE.TestCQRSApp.UI
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine();
                Console.WriteLine(" -------- SAFE.TestCQRSApp.UI -------- ");

                Console.WriteLine();
                Console.WriteLine("Write any note and press enter to save.");

                int expectedVersion = -1;

                while (true)
                {
                    var note = Console.ReadLine();
                    if (note == null)
                        continue;
                    expectedVersion = SaveNote(note, expectedVersion);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message + ex.StackTrace}");
                Console.WriteLine("Press any key to exit.");
                Console.ReadKey();
            }
        }

        static int SaveNote(string note, int expectedVersion)
        {
            var client = new HttpClient();
            var content = JsonConvert.SerializeObject(new { Note = note, ExpectedVersion = expectedVersion });
            var result = client.PostAsync("http://localhost:52794/api/note", new StringContent(content, Encoding.UTF8, "application/json"))
                .GetAwaiter()
                .GetResult();
            if (result.StatusCode == System.Net.HttpStatusCode.OK)
            {
                if (expectedVersion == -1)
                    ++expectedVersion;
                ++expectedVersion;
                Console.WriteLine("Note saved.");
            }
            else if (result.StatusCode == System.Net.HttpStatusCode.NotModified)
                Console.WriteLine("Note already existed.");
            else
                Console.WriteLine($"Error: {result.Content.ReadAsStringAsync().GetAwaiter().GetResult()}");

            return expectedVersion;
        }
    }
}