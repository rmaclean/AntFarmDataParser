using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using LumenWorks.Framework.IO.Csv;
using SHA3.Net;
using UAParser;
using TimeRange = System.ValueTuple<System.DateTime, System.DateTime>;

namespace views
{
    class Program
    {
        static void Main(
            string source = "data.csv",
            Boolean browserStats = false,
            Boolean viewStats = false,
            char sourceSeperator = ';'
        )
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

            if (viewStats) // --view-Stats
            {
                ViewStats(viewers);
            }
        }

        private static void ViewStats(Dictionary<string, Viewer> viewers)
        {
            var watches = viewers.SelectMany(viewer => 
                viewer.Value.Views.SelectMany(view => view.Value.Select(watch => watch.Value))
            );

            Console.WriteLine($"Session;Stream;Time");
            foreach (var watch in watches)
            {
                Console.WriteLine($"{watch.Session};{watch.Stream};{watch.WatchTime}");
            }
        }

        static void BrowserStats(Dictionary<string, Viewer> viewers)
        {
            Console.WriteLine($"OS;Major;Minor;Device");
            var browsers = viewers.SelectMany(viewer => viewer.Value.Browsers.Select(browser => browser.Value));
            foreach (var browser in browsers)
            {
                Console.WriteLine($"{browser.OS.Family};{browser.OS.Major};{browser.OS.Minor};{browser.Device.Family}");
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
        static TimeRange TimeSlot(string startTime, int minutes = 50)
        {
            var startDate = $"2020-04-02 {startTime}";
            var start = DateTime.Parse(startDate);
            var end = start.AddMinutes(minutes);
            return (start, end);
        }

        static List<TimeRange> timeslots = new List<TimeRange> {
            TimeSlot("08:30:00", 15),
            TimeSlot("08:50:00"),
            TimeSlot("10:10:00"),
            TimeSlot("11:10:00"),
            TimeSlot("12:10:00"),
            TimeSlot("13:50:00"),
            TimeSlot("14:50:00"),
            TimeSlot("15:50:00"),
            TimeSlot("16:50:00"),
        };

        static Parser uaParser = Parser.GetDefault();

        public string Id { get; }
        public Dictionary<string, ClientInfo> Browsers { get; } = new Dictionary<string, ClientInfo>(1);
        public Dictionary<int, StreamView> Streams { get; } = new Dictionary<int, StreamView>();
        public Dictionary<int, Dictionary<int, SessionView>> Views { get; } =
            new Dictionary<int, Dictionary<int, SessionView>>();

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
            UpdateBrowser(userAgent);
            UpdateStream(row);
        }

        private int GetStreamId(string text)
        {
            switch (text)
            {
                case "Stream 1":
                    {
                        return 1;
                    }
                case "Stream 2":
                    {
                        return 2;
                    }
                case "Stream 3":
                    {
                        return 3;
                    }
                case "Stream 4":
                    {
                        return 4;
                    }
                case "Stream 5":
                    {
                        return 5;
                    }
                case "Stream 6":
                    {
                        return 6;
                    }
                default:
                    {
                        throw new Exception("Unknown Stream ID");
                    }
            }
        }

        private void UpdateStream(string[] row)
        {
            var streamId = GetStreamId(row[0].Trim());
            var stream = GetStream(streamId);
            var timeRange = stream.AddView(row[4], row[5]);
            Dictionary<int, SessionView> perStreamViews;
            if (Views.ContainsKey(streamId))
            {
                perStreamViews = Views[streamId];
            }
            else
            {
                perStreamViews = new Dictionary<int, SessionView>();
                Views.Add(streamId, perStreamViews);
            }

            foreach (var view in AllocateTime(streamId, timeRange))
            {
                if (perStreamViews.ContainsKey(view.Session))
                {
                    perStreamViews[view.Session].WatchTime += view.WatchTime;
                }
                else
                {
                    perStreamViews.Add(view.Session, view);
                }
            }
        }

        private IEnumerable<SessionView> AllocateTime(int stream, TimeRange timeRange)
        {
            var adverts = 0.0;
            for (int index = 0; index < timeslots.Count; index++)
            {
                var timeSlot = timeslots[index];
                if (timeRange.Item1 > timeSlot.Item2) {
                    // this happened in a later time
                    continue;
                }

                if (timeRange.Item2 <= timeSlot.Item1)
                {
                    // stopped watching before session so allocate all remaining time to adverts
                    adverts += timeRange.Item2.Subtract(timeRange.Item1).TotalSeconds;
                    yield return new SessionView(stream, -1, adverts);
                    break;
                }

                if (timeRange.Item2 <= timeSlot.Item2)
                {
                    // stopped watching before the end of the session 
                    var totalWatchTime = timeRange.Item2.Subtract(timeRange.Item1).TotalSeconds;
                    if (timeRange.Item1 >= timeSlot.Item1)
                    {
                        // joined after the start so allocate all time to the session and return
                        yield return new SessionView(
                            stream,
                            index,
                            totalWatchTime
                        );
                        break;
                    }

                    // count time from start for session
                    var watchTime = timeRange.Item2.Subtract(timeSlot.Item1).TotalSeconds;
                    // count diff for adverts
                    adverts += totalWatchTime - watchTime;
                    yield return new SessionView(
                            stream,
                            index,
                            watchTime
                        );

                    yield return new SessionView(stream, -1, adverts);
                    break;
                }


                // continued watching past the end
                if (timeRange.Item1 >= timeSlot.Item1)
                {
                    // joined after the start so allocate time up to the end of session
                    var watchTime = timeSlot.Item2.Subtract(timeRange.Item1).TotalSeconds;
                    yield return new SessionView(
                           stream,
                           index,
                           watchTime
                       );

                    // move start to end of session for next loop
                    timeRange.Item1 = timeSlot.Item2;
                }
                else
                {
                    // joined before
                    // take time until start for adverts
                    adverts = timeSlot.Item1.Subtract(timeRange.Item1).TotalSeconds;
                    // allocate full session time
                    yield return new SessionView(
                          stream,
                          index,
                          timeSlot.Item2.Subtract(timeSlot.Item1).TotalSeconds
                      );
                    // move start to end of session for next loop
                    timeRange.Item1 = timeSlot.Item2;
                }
            }
        }

        private StreamView GetStream(int streamId)
        {
            if (Streams.ContainsKey(streamId))
            {
                return Streams[streamId];
            }
            else
            {
                return new StreamView(streamId);
            }
        }

        private void UpdateBrowser(string userAgent)
        {
            if (!Browsers.ContainsKey(userAgent))
            {
                Browsers.Add(userAgent, uaParser.Parse(userAgent));
            }
        }
    }

    class SessionView
    {
        public int Stream { get; }
        public int Session { get; }
        public double WatchTime { get; set; }

        public SessionView(int stream, int session, double watchTime)
        {
            this.Stream = stream;
            this.Session = session;
            this.WatchTime = watchTime;
        }
    }

    class StreamView
    {
        public int Id { get; }
        public List<TimeRange> Views { get; } = new List<TimeRange>();

        public StreamView(int Id)
        {
            this.Id = Id;
        }

        public TimeRange AddView(string start, string end)
        {
            var startTime = DateTime.Parse(start);
            var endTime = DateTime.Parse(end);
            var result = (startTime, endTime);
            Views.Add(result);
            return result;
        }
    }
}
