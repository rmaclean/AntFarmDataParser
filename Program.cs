using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using LumenWorks.Framework.IO.Csv;
using SHA3.Net;
using UAParser;

namespace views
{
    class Program
    {
        static void Main(string source = "data.csv", Boolean browserStats = false, char sourceSeperator = ';')
        {
            var viewers = new Dictionary<string, Viewer>();
            using (var csv = new CsvReader(new StreamReader(source), true, sourceSeperator))
            {
                csv.ToList().ForEach(row =>
                {
                    var id = GenerateId(row);
                    var viewer = viewers.GetValueOrDefault(id);
                    if (viewer == null)
                    {
                        viewer = new Viewer(id, row);
                        viewers.Add(id, viewer);
                    }
                    else
                    {
                        viewer.update(row);
                    }
                });
            }

            if (browserStats) // --browser-stats
            {
                BrowserStats(viewers);
            }
        }

        static void BrowserStats(Dictionary<string, Viewer> viewers)
        {
            Console.WriteLine($"OS,Major,Minor,Device");
            var browsers = viewers.SelectMany(viewer => viewer.Value.Browsers.Select(browser => browser.Value));
            foreach (var browser in browsers)
            {
                Console.WriteLine($"{browser.OS.Family},{browser.OS.Major},{browser.OS.Minor},{browser.Device.Family}");
            }
        }

        static string GenerateId(string[] row)
        {
            var id = row[1] + row[2] + row[3];
            using (var shaAlg = Sha3.Sha3256())
            {
                return Encoding.UTF8.GetString(shaAlg.ComputeHash(Encoding.UTF8.GetBytes(id)));
            }
        }
    }

    class Viewer
    {
        Parser uaParser = Parser.GetDefault();

        public string Id { get; }
        public Dictionary<string, ClientInfo> Browsers { get; } = new Dictionary<string, ClientInfo>(1);
        public List<StreamView> Streams { get; } = new List<StreamView>();

        public Viewer(string Id, string[] row)
        {
            this.Id = Id;
            this.update(row);
        }

        override public string ToString()
        {
            return this.Id;
        }

        internal void update(string[] row)
        {
            var userAgent = row[7];
            if (!Browsers.ContainsKey(userAgent))
            {
                Browsers.Add(userAgent, uaParser.Parse(userAgent));
            }
        }
    }

    class StreamView
    {
        public int Id { get; }
        public List<Tuple<DateTime, DateTime>> Views { get; } = new List<Tuple<DateTime, DateTime>>();
    }
}
