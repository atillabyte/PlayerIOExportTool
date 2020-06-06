using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AutoPlayerIO;
using Konsole;
using PlayerIOClient;
using Serilog;
using AutoPIO = AutoPlayerIO.PlayerIO;
using VenturePIO = PlayerIOClient.PlayerIO;

namespace PlayerIOExportTool
{
    class Program
    {
        public static string CreateSharedSecret(string username, string password, string gameId)
        {
            using (var managed = SHA256.Create())
            {
                return BitConverter.ToString(managed.ComputeHash(Encoding.UTF8.GetBytes(username + gameId + Guid.NewGuid().ToString()))).Replace("-", string.Empty);
            }
        }

        /// <summary>
        /// A tool to assist with exporting a Player.IO game.
        /// </summary>
        /// <param name="username"> The username of your Player.IO account </param>
        /// <param name="password"> The password of your Player.IO account </param>
        /// <param name="gameId"> The ID of the game to export. For example: tictactoe-vk6aoralf0yflzepwnhdvw </param>
        /// <param name="importFolder"> A directory containing the .ZIP BigDB export files given to you by Player.IO. </param>
        static async Task Main(string username, string password, string gameId, string importFolder)
        {
            if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password) || string.IsNullOrEmpty(gameId) || string.IsNullOrEmpty(importFolder))
            {
                Console.WriteLine("Unable to launch program. An argument may be missing or invalid. To view the required arguments, use -h.");
                return;
            }

            #region header
            Console.WriteLine(@"
                ╔═╗┬  ┌─┐┬ ┬┌─┐┬─┐ ╦╔═╗      
                ╠═╝│  ├─┤└┬┘├┤ ├┬┘ ║║ ║      
                ╩  ┴─┘┴ ┴ ┴ └─┘┴└─o╩╚═╝      
            ╔═╗─┐ ┬┌─┐┌─┐┬─┐┌┬┐  ╔╦╗┌─┐┌─┐┬  
            ║╣ ┌┴┬┘├─┘│ │├┬┘ │    ║ │ ││ ││  
            ╚═╝┴ └─┴  └─┘┴└─ ┴    ╩ └─┘└─┘┴─┘
            =================================
            by https://github.com/atillabyte/");
            #endregion

            var log = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            DeveloperAccount developer;
            DeveloperGame game;
            Client client;

            if (!Directory.Exists(importFolder))
            {
                log.Error("Unable to export game. The input directory specified does not exist.");
                return;
            }

            var archive_files = Directory.GetFiles(importFolder, "*.zip", SearchOption.TopDirectoryOnly).ToList();

            if (archive_files.Count == 0)
            {
                log.Error("Unable to export game. The input directory specified does not contain any .ZIP export files.");
                return;
            }

            // attempt to login and select game
            try
            {
                developer = await AutoPIO.LoginAsync(username, password);
                log.Information("Signed in as: " + developer.Username + " (" + developer.Email + ")");
            }
            catch
            {
                log.Error("Unable to export game. The login details provided were invalid.");
                return;
            }

            try
            {
                game = developer.Games.FirstOrDefault(game => game.GameId == gameId);
                log.Information("Selected game: " + game.Name + " (" + game.GameId + ")");
            }
            catch
            {
                log.Error("Unable to export game. No game was found matching the specified gameId.");
                return;
            }

            // delete export connection if already exists
            while (true)
            {
                var connections = await game.LoadConnectionsAsync();

                if (!connections.Any(c => c.Name == "export"))
                    break;

                log.Information("An existing export connection was found - attempting to recreate it. This process should only take a few seconds.");

                await game.DeleteConnectionAsync(connections.First(c => c.Name == "export"));

                // wait a second (we don't want to spam)
                await Task.Delay(1000);
            }

            var shared_secret = CreateSharedSecret(username, password, game.GameId);
            var tables = (await game.LoadBigDBAsync()).Tables;

            log.Information("Now attempting to create export connection with shared_secret = " + shared_secret);

            await game.CreateConnectionAsync("export", "A connection with read access to all BigDB tables - used for exporting games.", DeveloperGame.AuthenticationMethod.BasicRequiresAuthentication, "Default",
                tables.Select(t => (t, true, false, false, false, false, false)).ToList(), shared_secret);

            // ensure the export connection exists before continuing
            while (true)
            {
                var connections = await game.LoadConnectionsAsync();

                if (connections.Any(c => c.Name == "export"))
                    break;

                log.Information("Waiting until we have confirmation that the export connection exists...");
                Thread.Sleep(1000); // we don't want to spam.
            }

            log.Information("The export connection has been created.");

            // connect to the game and start export process.
            try
            {
                client = VenturePIO.Connect(game.GameId, "export", "user", VenturePIO.CalcAuth256("user", shared_secret));
            }
            catch (Exception ex)
            {
                log.Error("Unable to export game. An error occurred while trying to authenticate with the export connection. Details: " + ex.Message);
                return;
            }

            log.Information("Connected to the game. The export process will now begin.");

            var export_tasks = new List<Task<List<DatabaseObject>>>();
            var progress_bars = new ConcurrentBag<ProgressBar>();

            foreach (var archive_file in archive_files)
            {
                var split = new FileInfo(archive_file).Name.Split('_');
                var game_name = split[0];
                var table = split[1];
                var game_db = split[2];

                // create output directory
                var output_directory = Path.Combine("exports", game_name, table, game_db);

                // ensure output directory exists.
                Directory.CreateDirectory(output_directory);

                // find all keys in table export as fujson.
                var archive_keys = GetDatabaseObjectKeysFromArchive(archive_file);
                var already_exported = Directory.GetDirectories(output_directory, "*", SearchOption.TopDirectoryOnly).Select(x => new DirectoryInfo(x).Name).ToList();

                // add progress bar to the console
                var progress_bar = new ProgressBar(PbStyle.DoubleLine, archive_keys.Count);
                progress_bars.Add(progress_bar);
                progress_bar.Refresh(0, table);
                export_tasks.Add(ProcessJob(client, output_directory, table, archive_keys, progress_bar));
            }

            Task.WaitAll(export_tasks.ToArray());
            Console.WriteLine();
            log.Information("The export process has completed successfully. You can now close the program.");
            Console.ReadLine();
        }

        static Task<List<DatabaseObject>> ProcessJob(Client client, string output_directory, string table, List<string> keys, ProgressBar progress_bar)
        {
            return Task.Run(() =>
            {
                var database_objects = new List<DatabaseObject>();

                for (var i = 0; i < keys.Count; i++)
                {
                    var key = keys[i];

                    progress_bar.Refresh(i, table + " - " + key);

                    if (File.Exists(Path.Combine(output_directory, key + ".tson")))
                        continue;

                    try
                    {
                        var database_object = client.BigDB.Load(table, key);

                        if (database_object == null)
                            continue;

                        database_objects.Add(database_object);
                        File.WriteAllText(Path.Combine(output_directory, key + ".tson"), database_object.ToString());
                    }
                    catch (Exception ex)
                    {
                        File.AppendAllLines("errorlog.txt", new[] { DateTime.Now.ToString() + " " + "ProcessJob() " + ex.Message });
                    }
                }

                progress_bar.Refresh(keys.Count(), table + " - " + keys.Last());
                return database_objects;
            });
        }

        public static List<string> GetDatabaseObjectKeysFromArchive(string archiveFile)
        {
            var keys = new List<string>();

            using (var zipToOpen = new FileStream(archiveFile, FileMode.Open))
            {
                using (var archive = new ZipArchive(zipToOpen, ZipArchiveMode.Update))
                {
                    var jsonEntry = archive.Entries.First();
                    var pattern = new byte[] { 0x0D, 0x0A, 0x09, 0x22 };
                    var stream = jsonEntry.Open();
                    var positions = new List<long>();

                    foreach (var position in stream.ScanAOB(pattern))
                        positions.Add(position);

                    stream.Position = 0;
                    foreach (var position in positions)
                    {
                        stream.Position = position + 4;
                        var key = "";

                        while (true)
                        {
                            var b = stream.ReadByte();

                            if (b == (byte)'"')
                                break;

                            key += (char)b;
                        }

                        keys.Add(key);
                    }
                }
            }

            return keys;
        }
    }
}

static class StreamExtensions
{
    internal static IEnumerable<long> ScanAOB(this Stream stream, params byte[] aob)
    {
        long position;
        var buffer = new byte[aob.Length - 1];

        while ((position = stream.Position) < stream.Length)
        {
            if (stream.ReadByte() != aob[0]) continue;
            if (stream.Read(buffer, 0, aob.Length - 1) == 0) continue;

            if (buffer.SequenceEqual(aob.Skip(1)))
            {
                yield return position;
            }
        }
    }
}