using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using System.Text.RegularExpressions;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using HtmlAgilityPack;
using Robots;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace AskMe_Crawler {
    class Program {
        private static AskMeConfig config;
        private static Regex URLFilter;
        private static Regex cleaner = new Regex(@"[^a-z0-9' ]", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static Regex mimeTypeFilter = new Regex(@"([a-z]+/)?[a-z0-9\-]+(?=;?)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static Regex titleCleaner = new Regex(@"^ +", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static Regex urlCleaner = new Regex(@"^[^&#]+", RegexOptions.Compiled);
        private const string userAgent = "AskMeCrawler/1.0";
        public const int CRAWLSUCCESS = 0;
        public const int CRAWLLATER = 1;
        public const int CRAWLFILTERED = 2;
        public const int CRAWLTOOLONG = 3;
        public const int CRAWLNOTHTTP = 5;
        public const int CRAWLFAIL = -1;
        public const int CRAWLNOTALLOWED = -2;
        static void Main(string[] args) {
            // Open config file.
            using (FileStream configFile = File.OpenRead("askme.json")) {
                using (StreamReader configRaw = new StreamReader(configFile)) {
                    config = JsonConvert.DeserializeObject<AskMeConfig>(configRaw.ReadToEnd());
                }
            }
            try {
                URLFilter = new Regex(config.filter, RegexOptions.IgnoreCase | RegexOptions.Compiled);
            } catch (ArgumentException e) {
                Console.WriteLine("Unable to parse filter! Error: " + e.Message);
                URLFilter = new Regex(".*");
            }
            try {
                SqlConnection dbConn = new SqlConnection($"Server={config.sqlServer}; Database={config.sqlDB}; User Id={config.sqlUsername}; Password={config.sqlPassword}");
                dbConn.Open();
                // Create tables if they don't exist
                using (SqlCommand cmd = new SqlCommand("IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.name = 'PageIndex') BEGIN CREATE TABLE PageIndex (ID BIGINT PRIMARY KEY, word VARCHAR(64), neighbors TEXT, pageID BIGINT, domain VARCHAR(256)); CREATE INDEX idx_word ON PageIndex (word, domain); CREATE INDEX idx_pageID ON PageIndex (pageID); END", dbConn)) {
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.name = 'Pages') BEGIN CREATE TABLE Pages (ID BIGINT PRIMARY KEY, url VARCHAR(512), title TEXT, lastIndexed DATETIME, nextIndex DATETIME, contents TEXT, crawlDepth INT, clicks INT); CREATE UNIQUE INDEX idx_url ON Pages (url); CREATE INDEX idx_nextIndex ON Pages (nextIndex); END", dbConn)) {
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.name = 'Robots') BEGIN CREATE TABLE Robots (domain VARCHAR(256) PRIMARY KEY, robots TEXT, lastIndexed DATETIME, nextCrawl DATETIME); END", dbConn)) {
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.name = 'Queue') BEGIN CREATE TABLE Queue (ID BIGINT PRIMARY KEY, url VARCHAR(512), attempts INT, nextIndex DATETIME, crawlDepth INT); CREATE UNIQUE INDEX idx_url ON Queue (url); CREATE INDEX idx_nextIndex ON Queue (nextIndex); END", dbConn)) {
                    cmd.ExecuteNonQuery();
                }
                using (SqlCommand cmd = new SqlCommand("IF NOT EXISTS (SELECT * FROM sys.tables t WHERE t.name = 'Quarantine') BEGIN CREATE TABLE Quarantine (ID INT IDENTITY(1,1) PRIMARY KEY, url VARCHAR(512)); CREATE UNIQUE INDEX idx_url ON Quarantine (url); END", dbConn)) {
                    cmd.ExecuteNonQuery();
                }
                Task crawlThread = Task.Factory.StartNew(CrawlThread);
                Random rand = new Random();
                byte[] IDraw = new byte[8];
                while (true) {
                    Console.WriteLine("AskMe Crawler Ready.");
                    string command = Console.ReadLine().ToLower();
                    string[] cmdArgs = command.Split(' ');
                    switch (cmdArgs[0]) {
                        case "view":
                            if (cmdArgs.Length > 1) {
                                if (cmdArgs[1] == "quarantine") {
                                    Console.WriteLine("Quarantine:");
                                    DataSet quarantine = new DataSet();
                                    SqlDataAdapter adapter = new SqlDataAdapter {
                                        SelectCommand = new SqlCommand($"SELECT * FROM Quarantine;", dbConn)
                                    };
                                    adapter.Fill(quarantine);
                                    foreach (DataRow row in quarantine.Tables[0].Rows) {
                                        Console.WriteLine($"ID: {(int)row["ID"]} URL: {(string)row["url"]}");
                                    }
                                    Console.WriteLine($"{quarantine.Tables[0].Rows.Count} items in quarantine.");
                                    break;
                                } else if (cmdArgs[1] == "queue") {
                                    Console.WriteLine("Queue:");
                                    DataSet queue = new DataSet();
                                    SqlDataAdapter adapter = new SqlDataAdapter {
                                        SelectCommand = new SqlCommand($"SELECT * FROM Queue;", dbConn)
                                    };
                                    adapter.Fill(queue);
                                    foreach (DataRow row in queue.Tables[0].Rows) {
                                        Console.WriteLine($"ID: {(Int64)row["ID"]} URL: \"{(string)row["url"]}\" Attempts: {(int)row["attempts"]} Next Index: {new SqlDateTime((DateTime)row["nextIndex"])} Crawl Depth: {(int)row["crawlDepth"]}");
                                    }
                                    Console.WriteLine($"{queue.Tables[0].Rows.Count} items in the queue.");
                                    break;
                                }
                            }
                            Console.WriteLine("? Syntax: view <quarentine or queue>");
                            break;
                        case "index":
                            if (cmdArgs.Length > 2) {
                                try {
                                    string url = cmdArgs[1].Replace("'", "''");
                                    int crawlDepth = int.Parse(cmdArgs[2]);
                                    rand.NextBytes(IDraw);
                                    Int64 ID = BitConverter.ToInt64(IDraw, 0);
                                    using (SqlCommand cmd = new SqlCommand($"IF NOT EXISTS (SELECT * FROM Queue WHERE url = '{url}') INSERT INTO Queue (ID, url, attempts, nextIndex, crawlDepth) VALUES ({ID}, '{url}', 0, '{new SqlDateTime(DateTime.UtcNow)}', {crawlDepth});", dbConn)) {
                                        cmd.ExecuteNonQuery();
                                        Console.WriteLine($"Successfully inserted {cmdArgs[1]} into the queue with a crawl depth of {crawlDepth}.");
                                    }
                                } catch (Exception e) {
                                    Console.WriteLine($"An error occurred while procesing the command: {e.Message}");
                                }
                                break;
                            }
                            Console.WriteLine("? Syntax: index <url:string> <crawl depth:int>");
                            break;
                        case "allow":
                            if (cmdArgs.Length > 1) {
                                try {
                                    int ID = int.Parse(cmdArgs[1]);
                                    bool foundEntry = false;
                                    string url = "";
                                    using (SqlCommand cmd = new SqlCommand($"SELECT url FROM Quarantine WHERE ID = {ID};", dbConn)) {
                                        using (SqlDataReader result = cmd.ExecuteReader()) {
                                            if (result.HasRows) {
                                                result.Read();
                                                url = result.GetString(result.GetOrdinal("url"));
                                                foundEntry = true;
                                            }
                                        }
                                    }
                                    if (foundEntry) {
                                        rand.NextBytes(IDraw);
                                        Int64 newID = BitConverter.ToInt64(IDraw, 0);
                                        using (SqlCommand cmd = new SqlCommand($"IF NOT EXISTS (SELECT * FROM Queue WHERE url = '{url.Replace("'", "''")}') INSERT INTO Queue (ID, url, attempts, nextIndex, crawlDepth) VALUES ({newID}, '{url.Replace("'", "''")}', 0, '{new SqlDateTime(DateTime.UtcNow)}', {config.crawlDepth});", dbConn)) {
                                            cmd.ExecuteNonQuery();
                                        }
                                        using (SqlCommand cmd = new SqlCommand($"DELETE FROM Quarantine WHERE ID = {ID}", dbConn)) {
                                            cmd.ExecuteNonQuery();
                                        }
                                        Console.WriteLine($"Successfully moved quarantine ID {ID} (URL: {url}) into the queue.");
                                        break;
                                    }
                                } catch (Exception e) {
                                    Console.WriteLine($"An error occurred while procesing the command: {e.Message}");
                                }
                                break;
                            }
                            Console.WriteLine("? Syntax: allow <quarantine ID: int>");
                            break;
                        case "reject":
                            if (cmdArgs.Length > 1) {
                                try {
                                    int ID = int.Parse(cmdArgs[1]);
                                    using (SqlCommand cmd = new SqlCommand($"DELETE FROM Quarantine WHERE ID = {ID}", dbConn)) {
                                        cmd.ExecuteNonQuery();
                                    }
                                    Console.WriteLine($"Successfully deleted quarantine ID {ID}.");
                                    break;
                                } catch (Exception e) {
                                    Console.WriteLine($"An error occurred while procesing the command: {e.Message}");
                                }
                                break;
                            }
                            Console.WriteLine("? Syntax: reject <quarantine ID: int>");
                            break;
                        case "reindex":
                            if (cmdArgs.Length > 2) {
                                try {
                                    if (cmdArgs[1] == "url") {
                                        bool success = false;
                                        using (SqlCommand cmd = new SqlCommand($"UPDATE Pages SET nextIndex = GETUTCDATE() WHERE url = '{cmdArgs[2].Replace("'", "''")}';", dbConn)) {
                                            success = cmd.ExecuteNonQuery() == 1;
                                        }
                                        if (success) {
                                            Console.WriteLine($"Successfully set \"{cmdArgs[2]}\" to be reindexed.");
                                        } else {
                                            Console.WriteLine($"Something went wrong: No pages were updated.");
                                        }
                                    } else if (cmdArgs[1] == "domain") {
                                        bool success = false;
                                        using (SqlCommand cmd = new SqlCommand($"UPDATE Pages SET nextIndex = GETUTCDATE() WHERE ID IN (SELECT pageID FROM PageIndex WHERE domain = '{cmdArgs[2].Replace("'", "''")}');", dbConn)) {
                                            success = cmd.ExecuteNonQuery() > 0;
                                        }
                                        if (success) {
                                            Console.WriteLine($"Successfully set all pages from domain \"{cmdArgs[2]}\" to be reindexed.");
                                        } else {
                                            Console.WriteLine($"Something went wrong: No pages were updated.");
                                        }
                                    }
                                    break;
                                } catch (Exception e) {
                                    Console.WriteLine($"An error occurred while procesing the command: {e.Message}");
                                }
                            }
                            Console.WriteLine("? Syntax: reindex <domain or url> <domain or url:string>");
                            break;
                        case "delete":
                            if (cmdArgs.Length > 2) {
                                try {
                                    if (cmdArgs[1] == "url") {
                                        bool success = false;
                                        using (SqlCommand cmd = new SqlCommand($"DELETE FROM PageIndex WHERE pageID IN (SELECT ID FROM Pages WHERE url = '{cmdArgs[2].Replace("'", "''")}'); DELETE FROM Pages WHERE url = '{cmdArgs[2].Replace("'", "''")}'; DELETE FROM Queue WHERE url = '{cmdArgs[2].Replace("'", "''")}';", dbConn)) {
                                            success = cmd.ExecuteNonQuery() > 0;
                                        }
                                        if (success) {
                                            Console.WriteLine($"Successfully deleted \"{cmdArgs[2]}\" from the index.");
                                        } else {
                                            Console.WriteLine($"Something went wrong: No pages were deleted.");
                                        }
                                    } else if (cmdArgs[1] == "domain") {
                                        bool success = false;
                                        using (SqlCommand cmd = new SqlCommand($"DELETE FROM Pages WHERE ID IN (SELECT pageID FROM PageIndex WHERE domain = '{cmdArgs[2].Replace("'", "''")}'); DELETE FROM PageIndex WHERE domain = '{cmdArgs[2].Replace("'", "''")}'", dbConn)) {
                                            success = cmd.ExecuteNonQuery() > 0;
                                        }
                                        if (success) {
                                            Console.WriteLine($"Successfully deleted all pages from domain \"{cmdArgs[2]}\".");
                                        } else {
                                            Console.WriteLine($"Something went wrong: No pages were updated.");
                                        }
                                    }
                                    break;
                                } catch (Exception e) {
                                    Console.WriteLine($"An error occurred while procesing the command: {e.Message}");
                                }
                            }
                            Console.WriteLine("? Syntax: delete <domain or url> <domain or url:string>");
                            break;
                        case "stop":
                            System.Environment.Exit(0);
                            break;
                        case "help":
                            Console.Write(
                                $"view <quarantine or queue>{System.Environment.NewLine}" +
                                $"View the items that are currently in the quarantine or in the queue.{System.Environment.NewLine}{System.Environment.NewLine}" +
                                $"index <url:string> <queue depth:int>{System.Environment.NewLine}" +
                                $"Manually add a page to the index queue with a specified queue depth.{System.Environment.NewLine}{System.Environment.NewLine}" +
                                $"allow <quarantine ID:int>{System.Environment.NewLine}" +
                                $"Allow the item in the quarantine with the specified ID to be indexed.{System.Environment.NewLine}{System.Environment.NewLine}" +
                                $"reject <quarantine ID:int>{System.Environment.NewLine}" +
                                $"Remove the item in the quarantine with the specified ID from the queue.{System.Environment.NewLine}{System.Environment.NewLine}" +
                                $"stop{System.Environment.NewLine}" +
                                $"Stop the crawler.{System.Environment.NewLine}{System.Environment.NewLine}" +
                                $"help{System.Environment.NewLine}" +
                                $"Print this help."
                            );
                            break;
                        default:
                            Console.WriteLine($"? Unknown command {cmdArgs[0]}");
                            break;
                    }
                }
            } catch (Exception e) {
                Console.WriteLine("Failed to connect to database! Error: " + e.Message);
                Console.ReadKey(false);
                System.Environment.Exit(-1);
            }
        }
        static void CrawlThread() {
            SqlConnection dbConn = new SqlConnection($"Server={config.sqlServer}; Database={config.sqlDB}; User Id={config.sqlUsername}; Password={config.sqlPassword}");
            dbConn.Open();
            Random rand = new Random();
            while (true) {
                DataSet queue;
                SqlDataAdapter adapter;
                while (true) {
                    queue = new DataSet();
                    adapter = new SqlDataAdapter {
                        SelectCommand = new SqlCommand($"SELECT * FROM Queue WHERE nextIndex < '{new SqlDateTime(DateTime.UtcNow)}';", dbConn)
                    };
                    adapter.Fill(queue);
                    if (queue.Tables[0].Rows.Count == 0) {
                        break;
                    }
                    Task<int>[] threads = new Task<int>[config.threads];
                    int j = 0;
                    foreach (DataRow row in queue.Tables[0].Rows) {
                        if (j < config.threads) {
                            threads[j++] = Task<int>.Factory.StartNew(Crawler, new CrawlState((string)row["url"], (int)row["crawlDepth"], rand.Next(), (Int64)row["ID"], (int)row["attempts"]));
                        } else {
                            int index = Task.WaitAny(threads);
                            Task<int> thread = threads[index];
                            if (thread.Result >= 0 || ((CrawlState)thread.AsyncState).attempts > 2) {
                                using (SqlCommand cmd = new SqlCommand($"DELETE FROM Queue WHERE ID = {((CrawlState)thread.AsyncState).ID};", dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                                Console.WriteLine("Page " + ((CrawlState)thread.AsyncState).URL + " indexed!");
                            } else {
                                using (SqlCommand cmd = new SqlCommand($"UPDATE Queue SET attempts = {((CrawlState)thread.AsyncState).attempts + 1}, nextIndex = '{new SqlDateTime(DateTime.UtcNow + TimeSpan.FromSeconds(432000 + rand.Next(0, 172800)))}' WHERE ID = {((CrawlState)thread.AsyncState).ID};", dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            threads[index] = Task<int>.Factory.StartNew(Crawler, new CrawlState((string)row["url"], (int)row["crawlDepth"], rand.Next(), (Int64)row["ID"], (int)row["attempts"]));
                        }
                    }
                    foreach (Task<int> thread in threads) {
                        if (thread != null) {
                            if (thread.Result != CRAWLSUCCESS && ((CrawlState)thread.AsyncState).attempts > 2) {
                                // If the page we failed to crawl was in the index, remove it from the index.
                                using (SqlCommand cmd = new SqlCommand($"BEGIN TRANSACTION; DELETE FROM PageIndex WHERE pageID IN (SELECT ID FROM Pages WHERE url = '{((CrawlState)thread.AsyncState).URL.Replace("'", "''")}'); DELETE FROM Pages WHERE url = '{((CrawlState)thread.AsyncState).URL.Replace("'", "''")}'; COMMIT;", dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            if (thread.Result >= 0 || ((CrawlState)thread.AsyncState).attempts > 2) {
                                using (SqlCommand cmd = new SqlCommand($"DELETE FROM Queue WHERE ID = {((CrawlState)thread.AsyncState).ID};", dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                            } else {
                                using (SqlCommand cmd = new SqlCommand($"UPDATE Queue SET attempts = {((CrawlState)thread.AsyncState).attempts + 1}, nextIndex = '{new SqlDateTime(DateTime.UtcNow + TimeSpan.FromSeconds(432000 + rand.Next(0, 172800)))}' WHERE ID = {((CrawlState)thread.AsyncState).ID};", dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                    
                }
                // Queue pages that haven't been indexed in a while.
                queue = new DataSet();
                adapter = new SqlDataAdapter {
                    SelectCommand = new SqlCommand($"SELECT * FROM Pages WHERE nextIndex < '{new SqlDateTime(DateTime.UtcNow)}';", dbConn)
                };
                adapter.Fill(queue);
                byte[] IDraw = new byte[8];
                foreach (DataRow row in queue.Tables[0].Rows) {
                    rand.NextBytes(IDraw);
                    // Make sure that a page which enters the queue is not reindexed again any time soon.
                    using (SqlCommand cmd = new SqlCommand($"BEGIN TRANSACTION; IF NOT EXISTS (SELECT * FROM Queue WHERE url = '{((string)row["url"]).Replace("'", "''")}') INSERT INTO Queue (ID, url, attempts, nextIndex, crawlDepth) VALUES ({BitConverter.ToInt64(IDraw, 0)}, '{((string)row["url"]).Replace("'", "''")}', 0, '{new SqlDateTime((DateTime)row["nextIndex"])}', {(int)row["crawlDepth"]}); " 
                        + $"UPDATE Pages SET nextIndex = '{new SqlDateTime(DateTime.UtcNow + TimeSpan.FromSeconds(432000 + rand.Next(0, 172800)))}' WHERE ID = {(Int64)row["ID"]}; COMMIT;", dbConn)) {
                        cmd.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("Crawl thread sleeping for 1 minute.");
                Thread.Sleep(60000);
            }
        }

        // todo: when the backend database is implemented, have crawler take a queue ID.
        public static int Crawler(object state) {
            CrawlState queueEntry = (CrawlState)state;
            if (queueEntry.crawlDepth < 0) {
                return CRAWLSUCCESS;
            }
            string URL = queueEntry.URL;
            // Check the page against the URL filter.
            // todo: use proper error codes
            if (URL == null) {
                return CRAWLFAIL;
            }
            if (!URLFilter.IsMatch(URL)) {
                return CRAWLFILTERED;
            }
            Uri webBase = new Uri(URL);
            if (URL.Length > 512 || webBase.Host.Length > 256) {
                return CRAWLTOOLONG;
            }
            // Reject hosts the operator doesn't want to crawl.
            if (config.reject.Contains(webBase.Host)) {
                return CRAWLFILTERED;
            }
            if (webBase.Scheme != Uri.UriSchemeHttp && webBase.Scheme != Uri.UriSchemeHttps) {
                return CRAWLNOTHTTP;
            }
            SqlConnection dbConn;
            // Open connection to database.
            try {
                dbConn = new SqlConnection($"Server={config.sqlServer}; Database={config.sqlDB}; User Id={config.sqlUsername}; Password={config.sqlPassword}");
                dbConn.Open();
            } catch (Exception e) {
                Console.WriteLine("Failed to connect to database! Error: " + e.Message);
                return CRAWLFAIL;
            }
            // Connect to the web server to detect redirects.
            // (But only actually download the page if we need to.)
            WebRequest request = WebRequest.Create(URL);
            int crawlDelay;
            HtmlDocument htmlDoc = new HtmlDocument();
            // Variables we will need outside the web request scope.
            bool isHtml;
            string page;
            string title = "";
            bool alreadyInIndex = false;
            Random IDgenerator = new Random(queueEntry.rngSeed);
            byte[] IDraw = new byte[8];
            Int64 pageID;
            string SQLsafeURL;
            string SQLsafeDomain;
            try {
                using (WebResponse response = request.GetResponse()) {
                    webBase = response.ResponseUri;
                    URL = urlCleaner.Match(webBase.ToString()).Value;

                    if (URL.Length > 512 || webBase.Host.Length > 256) {
                        dbConn.Close();
                        return CRAWLTOOLONG;
                    }

                    // Reject hosts the operator doesn't want to crawl.
                    if (config.reject.Contains(webBase.Host)) {
                        dbConn.Close();
                        return CRAWLFILTERED;
                    }
                    if (webBase.Scheme != Uri.UriSchemeHttp && webBase.Scheme != Uri.UriSchemeHttps) {
                        dbConn.Close();
                        return CRAWLNOTHTTP;
                    }

                    // Sanitize the URL
                    SQLsafeURL = URL.Replace("'", "''");
                    SQLsafeDomain = webBase.Host.Replace("'", "''");
                    int oldCrawlDepth = queueEntry.crawlDepth + 1;
                    DateTime lastIndexed = DateTime.UtcNow;

                    // Check if the page has already been indexed
                    using (SqlCommand cmd = new SqlCommand($"SELECT ID, nextIndex, lastIndexed, crawlDepth FROM Pages WHERE url = '{SQLsafeURL}';", dbConn)) {
                        using (SqlDataReader result = cmd.ExecuteReader()) {
                            if (result.HasRows) {
                                // If the page has already been indexed, figure out if it's time to index it again.
                                result.Read();
                                lastIndexed = result.GetDateTime(result.GetOrdinal("lastIndexed"));
                                DateTime nextIndex = result.GetDateTime(result.GetOrdinal("nextIndex"));
                                oldCrawlDepth = result.GetInt32(result.GetOrdinal("crawlDepth"));
                                if (nextIndex > DateTime.UtcNow && oldCrawlDepth >= queueEntry.crawlDepth) {
                                    // If not, return.
                                    dbConn.Close();
                                    return CRAWLLATER;
                                }
                                alreadyInIndex = true;
                                pageID = result.GetInt64(result.GetOrdinal("ID"));
                            } else {
                                IDgenerator.NextBytes(IDraw);
                                pageID = BitConverter.ToInt64(IDraw, 0);
                            }
                        }
                    }

                    // Check Robots.txt
                    string rawRobots = "";
                    bool foundRobots = false;
                    DateTime nextAllowedCrawl = DateTime.UtcNow;
                    using (SqlCommand cmd = new SqlCommand($"SELECT * FROM Robots WHERE domain = '{SQLsafeDomain}';", dbConn)) {
                        using (SqlDataReader result = cmd.ExecuteReader()) {
                            DateTime lastIndex = DateTime.UtcNow;
                            if (result.HasRows) {
                                // If we've already grabbed robots.txt, check how old it is.
                                // If it's less than a day old, use it.
                                result.Read();
                                lastIndex = result.GetDateTime(result.GetOrdinal("lastIndexed"));
                                nextAllowedCrawl = result.GetDateTime(result.GetOrdinal("nextCrawl"));
                                if (lastIndex + TimeSpan.FromDays(1) > DateTime.UtcNow) {
                                    rawRobots = result.GetString(result.GetOrdinal("robots"));
                                    goto parseRobots;
                                }
                                foundRobots = true;
                            }
                            // Else, get a new copy of robots.
                            using (WebClient client = new WebClient()) {
                                client.Headers.Add("User-Agent", userAgent);
                                try {
                                    using (Stream file = client.OpenRead(new Uri(webBase, "/robots.txt"))) {
                                        // Check if robots.txt has been modified since we last crawled it.
                                        if (client.ResponseHeaders[HttpResponseHeader.LastModified] != null) {
                                            DateTime lastModified = DateTime.Parse(client.ResponseHeaders[HttpResponseHeader.LastModified]);
                                            if (foundRobots && lastIndex >= lastModified) {
                                                // If it has not been modified, don't bother reading it.
                                                goto updateRobots;
                                            }
                                        }
                                        using (StreamReader reader = new StreamReader(file)) {
                                            rawRobots = reader.ReadToEnd();
                                        }
                                    }
                                } catch (WebException e) {
                                    if (e.Status != WebExceptionStatus.ProtocolError) {
                                        Console.WriteLine("Failed to download robots.txt for " + new Uri(webBase, "/").AbsoluteUri + "!");
                                        dbConn.Close();
                                        return CRAWLFAIL;
                                    }
                                    if (((HttpWebResponse)e.Response).StatusCode == HttpStatusCode.NotModified) {
                                        // Robots.txt hasn't been modified since we last checked. We can skip downloading.
                                        if (!foundRobots) {
                                            throw e;
                                        }
                                        rawRobots = result.GetString(result.GetOrdinal("robots"));
                                    } else if (((HttpWebResponse)e.Response).StatusCode != HttpStatusCode.NotFound) {
                                        throw e;
                                    }
                                }
                            }
                        }
                    }
                updateRobots:
                    // Update the database
                    if (foundRobots) {
                        using (SqlCommand cmd = new SqlCommand($"UPDATE Robots SET robots = '{rawRobots.Replace("'", "''")}', lastIndexed = '{new SqlDateTime(DateTime.UtcNow)}' WHERE domain = '{SQLsafeDomain}';", dbConn)) {
                            cmd.ExecuteNonQuery();
                        }
                    } else {
                        using (SqlCommand cmd = new SqlCommand($"IF NOT EXISTS (SELECT * FROM Robots WHERE domain = '{SQLsafeDomain}') INSERT INTO Robots (domain, robots, lastIndexed, nextCrawl) VALUES ('{SQLsafeDomain}', '{rawRobots.Replace("'", "''")}', '{new SqlDateTime(DateTime.UtcNow)}', '{new SqlDateTime(DateTime.UtcNow)}');", dbConn)) {
                            cmd.ExecuteNonQuery();
                        }
                    }

                parseRobots:
                    Robots.Robots robots = new Robots.Robots();
                    robots.LoadContent(rawRobots, new Uri(webBase, "/"));
                    if (!robots.Allowed(URL, userAgent) || nextAllowedCrawl > DateTime.UtcNow) {
                        // If we aren't allowed, don't crawl the page.
                        dbConn.Close();
                        return CRAWLNOTALLOWED;
                    }
                    crawlDelay = robots.GetCrawlDelay(userAgent);
                    // Update the database to indicate that we've recently crawled this domain.
                    if (crawlDelay > 0) {
                        using (SqlCommand cmd = new SqlCommand($"UPDATE Robots SET nextCrawl = '{new SqlDateTime(DateTime.UtcNow + TimeSpan.FromSeconds(crawlDelay))}' WHERE domain = '{SQLsafeDomain}';", dbConn)) {
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Download the page.
                    using (Stream file = response.GetResponseStream()) {
                        // Check if the page has been modified since we last crawled it.
                        if (response.Headers[HttpResponseHeader.LastModified] != null) {
                            DateTime lastModified = DateTime.Parse(response.Headers[HttpResponseHeader.LastModified]);
                            if (alreadyInIndex && lastIndexed >= lastModified && oldCrawlDepth >= queueEntry.crawlDepth) {
                                // If it has not been modified, return.
                                using (SqlCommand cmd = new SqlCommand($"UPDATE Pages SET lastIndexed = '{new SqlDateTime(DateTime.UtcNow)}', crawlDepth = {queueEntry.crawlDepth} WHERE ID = {pageID}", dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                                dbConn.Close();
                                return CRAWLLATER;
                            }
                        }
                        string mimeType = mimeTypeFilter.Match(response.Headers[HttpResponseHeader.ContentType]).Value;
                        if (mimeType == "text/plain") {
                            using (StreamReader reader = new StreamReader(file)) {
                                page = reader.ReadToEnd();
                            }
                            isHtml = false;
                        } else if (mimeType == "text/html" || mimeType == "text/xml") {
                            using (StreamReader reader = new StreamReader(file)) {
                                string rawHtml = reader.ReadToEnd();
                                htmlDoc.LoadHtml(rawHtml);
                                // Extract the text
                                page = htmlDoc.DocumentNode.InnerText;
                                if (page == null) {
                                    Console.WriteLine("Failed to load URL! URL: " + URL + " Error: No text found.");
                                    dbConn.Close();
                                    return CRAWLFAIL;
                                }
                                HtmlNode titleElement = htmlDoc.DocumentNode.SelectSingleNode("//title");
                                if (titleElement != null && titleElement.InnerText != null) {
                                    title = titleElement.InnerText;
                                }
                                isHtml = true;
                            }
                        } else {
                            dbConn.Close();
                            Console.WriteLine($"URL: {URL} Mime type not plaintext or HTML!");
                            return CRAWLFILTERED;
                        }
                    }
                }
            } catch (Exception e) {
                Console.WriteLine("Failed to download page " + URL + " !");
                dbConn.Close();
                return CRAWLFAIL;
            }

            if (title == "") {
                title = Uri.UnescapeDataString(webBase.Segments[webBase.Segments.Length - 1]);
            }

            title = titleCleaner.Replace(title, "");

            // Consolidate all of the SQL queries into one;
            string query = "BEGIN TRANSACTION;";
            string newQuery;

            // Add the page to the table of pages.
            if (alreadyInIndex) {
                query += $"UPDATE Pages SET lastIndexed = '{new SqlDateTime(DateTime.UtcNow)}', contents = '{page.Replace("'", "''")}', title = '{title.Replace("'", "''")}', crawlDepth = {queueEntry.crawlDepth} WHERE ID = {pageID}; DELETE FROM PageIndex WHERE pageID = {pageID}";
            } else {
                query += $"INSERT INTO Pages (ID, url, title, lastIndexed, nextIndex, contents, crawlDepth, clicks) VALUES ({pageID}, '{SQLsafeURL}', '{title.Replace("'", "''")}', '{new SqlDateTime(DateTime.UtcNow)}', '{new SqlDateTime(DateTime.UtcNow + TimeSpan.FromSeconds(432000 + IDgenerator.Next(0, 172800)))}', '{page.Replace("'", "''")}', {queueEntry.crawlDepth}, 0);";
            }

            if (isHtml) {
                HashSet<string> links = new HashSet<string>();
                HtmlNodeCollection linkNodes = htmlDoc.DocumentNode.SelectNodes("//a[@href]");
                if (linkNodes != null) {
                    foreach (var node in htmlDoc.DocumentNode.SelectNodes("//a[@href]")) {
                        try {
                            // Make sure the link is well formed.
                            if (node.Attributes["href"] != null) {
                                Uri href = new Uri(webBase, node.Attributes["href"].Value);
                                if ((href.Scheme == Uri.UriSchemeHttp || href.Scheme == Uri.UriSchemeHttps) &&
                                        !config.reject.Contains(href.Host)) {
                                    string link = urlCleaner.Match(href.AbsoluteUri).Value;
                                    if (link.Length < 512 && URLFilter.IsMatch(link)) {
                                        links.Add(link.Replace("'", "''"));
                                    }
                                }
                            }
                        } catch (UriFormatException e) { }
                    }
                    foreach (string link in links) {
                        if (queueEntry.crawlDepth > 0) {
                            IDgenerator.NextBytes(IDraw);
                            Int64 ID = BitConverter.ToInt64(IDraw, 0);
                            newQuery = $"IF NOT EXISTS (SELECT * FROM Queue WHERE url = '{link}') INSERT INTO Queue (ID, url, attempts, nextIndex, crawlDepth) VALUES ({ID}, '{link}', 0, '{new SqlDateTime(DateTime.UtcNow + TimeSpan.FromSeconds(crawlDelay))}', {queueEntry.crawlDepth - 1});";
                            if (query.Length + newQuery.Length > 524288) {
                                using (SqlCommand cmd = new SqlCommand(query, dbConn)) {
                                    cmd.ExecuteNonQuery();
                                }
                                query = "";
                            }
                            query += newQuery;
                        }
                    }
                }
            }

            // Seperate the page into words.
            char[] delimiters = { ' ' };
            string[] words = cleaner.Replace(page, " ").ToLower().Split(delimiters, StringSplitOptions.RemoveEmptyEntries);
            // Create the tree of word relations
            Dictionary<string, HashSet<string>> wordEntries = new Dictionary<string, HashSet<string>>();
            for (int i = 0; i < words.Length; i++) {
                string word = words[i];
                if (!wordEntries.ContainsKey(word)) {
                    wordEntries[word] = new HashSet<string>();
                }
                if (i > 0) {
                    wordEntries[word].Add(words[i - 1]);
                    if (i > 1) {
                        wordEntries[word].Add(words[i - 2]);
                    }
                }
                if (i < words.Length - 1) {
                    wordEntries[word].Add(words[i + 1]);
                    if (i < words.Length - 2) {
                        wordEntries[word].Add(words[i + 2]);
                    }
                }
            }
            // Serialize neighbors lists to JSON
            foreach (KeyValuePair<string, HashSet<string>> wordEntry in wordEntries) {
                string word = wordEntry.Key;
                if (word.Length < 64) {
                    IDgenerator.NextBytes(IDraw);
                    Int64 ID = BitConverter.ToInt64(IDraw, 0);
                    string neighbors = JsonConvert.SerializeObject(wordEntry.Value);
                    newQuery = $"INSERT INTO PageIndex (ID, word, neighbors, pageID, domain) VALUES ({ID}, '{word.Replace("'", "''")}', '{neighbors.Replace("'", "''")}', {pageID}, '{SQLsafeDomain}');";
                    if (query.Length + newQuery.Length > 524288) {
                        using (SqlCommand cmd = new SqlCommand(query, dbConn)) {
                            cmd.ExecuteNonQuery();
                        }
                        query = "";
                    }
                    query += newQuery;
                }
            }
            // Execute the final query.
            query += "COMMIT;";
            using (SqlCommand cmd = new SqlCommand(query, dbConn)) {
                cmd.ExecuteNonQuery();
            }
            dbConn.Close();
            return CRAWLSUCCESS;
        }
    }
    public struct AskMeConfig {
        public string sqlServer;
		public string sqlUsername;
		public string sqlPassword;
        public string sqlDB;
        public string filter;
        public HashSet<string> reject;
        public int crawlDepth;
        public int threads;
	}
    class CrawlState {
        public string URL;
        public int crawlDepth;
        public int rngSeed;
        public Int64 ID;
        public int attempts;
        public CrawlState(string URL, int crawlDepth, int rngSeed, Int64 ID, int attempts) {
            this.URL = URL;
            this.crawlDepth = crawlDepth;
            this.rngSeed = rngSeed;
            this.ID = ID;
            this.attempts = attempts;
        }
        public CrawlState() {
            URL = "";
            crawlDepth = -1;
        }
    }
}
