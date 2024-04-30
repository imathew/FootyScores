using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace FootyScores
{
    public partial class PlayerScores
    {
        private static readonly int PLAYER_PREVIEW_COUNT = 5;         // how many of the top players to show for upcoming matches
        private static readonly int PLAYER_NAME_LENGTH_SQUISH = 20;   // squash the font of longer names to reduce table size
        private static readonly int MIN_CACHE_LIFETIME_SECONDS = 30;  // cache any API calls for at least this long
        private static readonly int ROUND_CHANGE_DAYS = 2;            // How many days from the next round do we switch to it?

        private static readonly Dictionary<string, int> SCORING = new()
        {
            { "K", 4 }, { "H", 2 }, { "G", 8 }, { "B", 1 }, { "T", 4 },
            { "FF", 2 }, { "FA", -2 }, { "SP", 2 }, { "IED", -2 },
            { "M", 1 }, { "HO", 1 }, { "CP", 1 }, { "R50", 1 }
        };

        private static readonly Dictionary<int, string> POSITIONS = new()
        {
            {1, "B" }, {2, "C" }, {3, "R" }, {4, "F"}
        };

        private const string SQUADS = """
            [{"id":10,"full_name":"City of Churches","name":"Crows","short_name":"ADE"}
            ,{"id":20,"full_name":"Fitzroy","name":"Lions","short_name":"BRL"}
            ,{"id":30,"full_name":"Premiers 2024","name":"Blues","short_name":"CAR"}
            ,{"id":40,"full_name":"Eastern Collingwood","name":"Magpies","short_name":"COL"}
            ,{"id":50,"full_name":"Essington","name":"Bombers","short_name":"ESS"}
            ,{"id":60,"full_name":"Yay Dockers!","name":"Dockers","short_name":"FRE"}
            ,{"id":70,"full_name":"Kingdom of Billy Brownless","name":"Cats","short_name":"GEE"}
            ,{"id":1000,"full_name":"Pointless Expansion Team #1","name":"Suns","short_name":"GCS"}
            ,{"id":1010,"full_name":"Greater Metropolitan Shire Of Western Sydney","name":"Giants","short_name":"GWS"}
            ,{"id":80,"full_name":"Wees and Poos","name":"Hawks","short_name":"HAW"}
            ,{"id":90,"full_name":"Central Business District","name":"Demons","short_name":"MEL"}
            ,{"id":100,"full_name":"Tim&apos;s latest team","name":"Kangaroos","short_name":"NTH"}
            ,{"id":110,"full_name":"Western Collingwood","name":"Power","short_name":"PTA"}
            ,{"id":120,"full_name":"Toby&apos;s Team","name":"Tigers","short_name":"RIC"}
            ,{"id":130,"full_name":"Jane Franklin Hall","name":"Saints","short_name":"STK"}
            ,{"id":160,"full_name":"South Melbourne","name":"Swans","short_name":"SYD"}
            ,{"id":150,"full_name":"All of WA except Freo","name":"Eagles","short_name":"WCE"}
            ,{"id":140,"full_name":"Footscray","name":"Bulldogs","short_name":"WBD"}]
            """;

        private const string VENUES = """
            [{"id":2,"name":"Blundstone Arena","short_name":"Bellerive","timezone":"Australia\/Hobart"}
            ,{"id":6,"name":"Adelaide Oval","short_name":"Adelaide","timezone":"Australia\/Adelaide"}
            ,{"id":9,"name":"Accor Stadium","short_name":"Stadium Aus","timezone":"Australia\/Sydney"}
            ,{"id":20,"name":"Gabba","short_name":"Gabba","timezone":"Australia\/Brisbane"}
            ,{"id":30,"name":"GMHBA Stadium","short_name":"Kardinia","timezone":"Australia\/Melbourne"}
            ,{"id":40,"name":"Melbourne Cricket Ground","short_name":"MCG","timezone":"Australia\/Melbourne"}
            ,{"id":43,"name":"ENGIE Stadium","short_name":"Showgrounds","timezone":"Australia\/Sydney"}
            ,{"id":50,"name":"Ikon Park","short_name":"Princes Park","timezone":"Australia\/Melbourne"}
            ,{"id":60,"name":"Sydney Cricket Ground","short_name":"SCG","timezone":"Australia\/Sydney"}
            ,{"id":81,"name":"People First Stadium","short_name":"Carrara","timezone":"Australia\/Brisbane"}
            ,{"id":150,"name":"Manuka Oval","short_name":"Manuka","timezone":"Australia\/Canberra"}
            ,{"id":160,"name":"TIO Stadium","short_name":"Darwin","timezone":"Australia\/Darwin"}
            ,{"id":181,"name":"Cazalys Stadium","short_name":"Cairns","timezone":"Australia\/Brisbane"}
            ,{"id":190,"name":"Marvel Stadium","short_name":"Docklands","timezone":"Australia\/Melbourne"}
            ,{"id":200,"name":"University of Tasmania Stadium","short_name":"York Park","timezone":"Australia\/Hobart"}
            ,{"id":313,"name":"Mars Stadium","short_name":"Ballarat","timezone":"Australia\/Melbourne"}
            ,{"id":374,"name":"Norwood Oval","short_name":"Norwood","timezone":"Australia\/Adelaide"}
            ,{"id":386,"name":"TIO Traeger Park","short_name":"Alice Springs","timezone":"Australia\/Darwin"}
            ,{"id":2925,"name":"Optus Stadium","short_name":"Perth","timezone":"Australia\/Perth"}
            ,{"id":3805,"name":"Adelaide Hills","short_name":"Adelaide Hills","timezone":"Australia\/Adelaide"}]
            """;

        private static readonly JsonArray? _squads = JsonNode.Parse(SQUADS)?.AsArray();
        private static readonly JsonArray? _venues = JsonNode.Parse(VENUES)?.AsArray();
        private static readonly HttpClient _httpClient = new();
        private static readonly BlobServiceClient _blobServiceClient = new(Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING"));
        private static readonly TimeZoneInfo _timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById("Australia/Melbourne");
        private static readonly BlobContainerClient _containerClient = _blobServiceClient.GetBlobContainerClient("playerscores-cache");

        private static readonly string _allowedOrigin;
        private static readonly Dictionary<string, string> _headers;
        private static readonly string _apiBaseUrl;
        private static readonly string _apiRoundsUrl;
        private static readonly string _apiPlayersUrl;

        static PlayerScores()
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            _allowedOrigin = config["AllowedOrigin"]!;
            _apiBaseUrl = config["API_BASE_URL"]!;
            _apiRoundsUrl = config["API_ROUNDS_URL"]!;
            _apiPlayersUrl = config["API_PLAYERS_URL"]!;

            _headers = new Dictionary<string, string>
            {
                { "Content-Type", "text/html; charset=utf-8" },
                { "Content-Encoding", "gzip" },
                { "Access-Control-Allow-Origin", _allowedOrigin },
                { "Access-Control-Allow-Methods", "GET, POST, OPTIONS" },
                { "Access-Control-Allow-Headers", "Content-Type" }
            };

            _containerClient.CreateIfNotExistsAsync().Wait();
        }

        [Function("PlayerScores")]
        public static async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
        {
            var closestRound = await GetCurrentRoundAsync();

            if (closestRound != null)
            {
                var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
                foreach (var header in _headers)
                {
                    response.Headers.Add(header.Key, header.Value);
                }

                using (var outputStream = new MemoryStream())
                {
                    using (var gzipStream = new GZipStream(outputStream, CompressionMode.Compress))
                    {
                        // generate the HTML output
                        string htmlOutput = await GenerateHtmlOutputAsync(closestRound);
                        var bytes = Encoding.UTF8.GetBytes(htmlOutput);
                        await gzipStream.WriteAsync(bytes);
                    }

                    var compressedBytes = outputStream.ToArray();
                    await response.Body.WriteAsync(compressedBytes);
                }

                return response;
            }

            var errorResponse = req.CreateResponse(System.Net.HttpStatusCode.InternalServerError);
            foreach (var header in _headers)
            {
                errorResponse.Headers.Add(header.Key, header.Value);
            }
            return errorResponse;
        }

        private static async Task<string> GenerateHtmlOutputAsync(JsonNode closestRound)
        {
            var htmlBuilder = new StringBuilder();

            htmlBuilder.Append($@"
<h1 title='Refresh' class='refresh-button'>The Masters &ndash; Round {closestRound["id"]}</h1>
<table>
");

            if (closestRound["matches"] is JsonArray matches)
            {
                var statsData = await GetPlayerStatsAsync(closestRound);
                var scores = statsData?["playerScores"] as JsonObject;
                var players = await GetPlayerDataAsync();
                var sortedMatches = matches.OrderBy(m => m, new MatchComparer());

                foreach (var match in sortedMatches)
                {
                    var status = match!["status"]?.ToString() ?? string.Empty;

                    htmlBuilder.AppendLine($@"
<thead><tr class='blank_header'><td colspan='18'></td></tr>");

                    htmlBuilder.Append(GetMatchHtml(match, _squads!, _venues!));

                    htmlBuilder.AppendLine($@"
<tr class='stats_header'><th colspan='2' class='minion'>Minion</th><th class='pos_head'>Pos</th><th>AF</th><th>ToG</th>{string.Join(string.Empty, SCORING.Select(stat => $"<th title='{stat.Value}' class='stat_head'>{stat.Key}</th>"))}</tr></thead>");

                    if (players != null)
                    {
                        var matchPlayers = GetMatchPlayers(match, players);
                        htmlBuilder.Append(GetPlayersHtml(matchPlayers.Where(p => p != null)!.Cast<JsonNode>(), scores!, _squads!, statsData as JsonObject, status));
                    }
                    else
                    {
                        htmlBuilder.AppendLine(@"</thead>");
                    }
                }
            }

            htmlBuilder.AppendLine(@"</table>");

            return htmlBuilder.ToString();
        }

        private static async Task<JsonNode?> GetCurrentRoundAsync()
        {
            return await GetCachedDataAsync<JsonNode>("round.json", GetCacheExpiry, async () =>
            {
                if (await MakeRequestAsync($"{_apiBaseUrl}/{_apiRoundsUrl}") is JsonArray roundsData)
                {
                    var now = GetNow();
                    var today = now.Date;
                    var roundChange = today.AddDays(ROUND_CHANGE_DAYS);

                    var currentRound = roundsData
                        .Select(roundData =>
                        {
                            var start = DateTimeOffset.Parse(roundData!["start"]!.GetValue<string>());
                            var end = DateTimeOffset.Parse(roundData!["end"]!.GetValue<string>());
                            return new { Round = roundData, Start = start, End = end };
                        })
                        .OrderByDescending(roundInfo => roundInfo.Start)
                        .FirstOrDefault(roundInfo =>
                        {
                            var startDate = roundInfo.Start.Date;
                            return startDate <= today || (startDate >= today && startDate <= roundChange);
                        });

                    return currentRound?.Round;
                }

                return null;
            });
        }

        private static async Task<JsonArray?> GetPlayerDataAsync()
        {
            return await GetCachedDataAsync<JsonArray>("players.json", GetCacheExpiry, async () =>
            {
                return await MakeRequestAsync($"{_apiBaseUrl}/{_apiPlayersUrl}") as JsonArray;
            });
        }

        private static async Task<JsonObject?> GetPlayerStatsAsync(JsonNode closestRound)
        {
            int roundNumber = closestRound["id"]!.GetValue<int>();

            return await GetCachedDataAsync<JsonObject>($"stats{roundNumber}.json", GetCacheExpiry, async () =>
            {
                JsonObject? statsData = await MakeRequestAsync($"{_apiBaseUrl}/stats/{roundNumber}.json") as JsonObject;

                if (statsData != null)
                {
                    var playerScores = new JsonObject();
                    foreach (var playerStats in statsData)
                    {
                        var playerId = playerStats.Key;
                        var score = playerStats.Value?.AsObject().Sum(p => SCORING.GetValueOrDefault(p.Key, 0) * p.Value!.GetValue<int>()) ?? 0;
                        playerScores[playerId] = score;
                    }
                    statsData["playerScores"] = playerScores;
                }

                return statsData;
            });
        }

        private static string GetPlayersHtml(IEnumerable<JsonNode> players, JsonObject scores, JsonArray squads, JsonObject? statsData, string matchStatus)
        {
            var htmlBuilder = new StringBuilder();

            //just show a subset if the match hasn't started
            int subset = matchStatus == "scheduled" ? PLAYER_PREVIEW_COUNT : int.MaxValue;

            var sortedPlayers = players
                .OrderByDescending(p =>
                {
                    var score = scores[p["id"]!.ToString()];
                    return score != null ? score.GetValue<int>() : 0;
                })
                .ThenBy(p =>
                {
                    var seasonRank = p["stats"]?["season_rank"]?.GetValue<int>();
                    return seasonRank == 0 ? int.MaxValue : seasonRank ?? int.MaxValue;
                })
                .Take(subset);

            foreach (var player in sortedPlayers)
            {
                var playerHtml = GetPlayerHtml(player, scores, squads, statsData, matchStatus);

                if (!string.IsNullOrEmpty(playerHtml))
                {
                    htmlBuilder.AppendLine(playerHtml);
                }
            }

            return htmlBuilder.ToString();
        }

        private static string GetPlayerHtml(JsonNode player, JsonObject scores, JsonArray squads, JsonObject? statsData, string matchStatus)
        {
            var squad = squads.FirstOrDefault(s => s!["id"]!.GetValue<int>() == player["squad_id"]!.GetValue<int>());
            var team = squad?["name"]?.GetValue<string>() ?? string.Empty;
            var teamShort = squad?["short_name"]?.GetValue<string>() ?? string.Empty;

            var playerName = $"{player["first_name"]} {player["last_name"]}";
            var playerClass = playerName.Length >= PLAYER_NAME_LENGTH_SQUISH ? "playername long" : "playername";
            var playerRank = $"Season rank: {player["stats"]?["season_rank"]?.GetValue<int>() ?? 0}";
            var playerAge = GetAgeString(player["dob"]?.ToString() ?? String.Empty);

            var playerPositions = player["positions"]?.AsArray()?.Select(p => p!.GetValue<int>()).ToList() ?? [];
            var sortedPositions = playerPositions.Select(p => new { Id = p, Letter = POSITIONS.GetValueOrDefault(p, "") })
                                                 .OrderBy(p => p.Id)
                                                 .Select(p => p.Letter);
            var positionString = string.Join("", sortedPositions);

            var playerRecord = statsData?[player["id"]!.ToString()];

            var statCells = new StringBuilder();

            if (playerRecord != null)
            {
                var score = scores[player["id"]!.GetValue<int>().ToString()]!.GetValue<int>();
                var playerStats = playerRecord?.AsObject().ToDictionary(p => p.Key, p => p.Value!.GetValue<int>()) ?? [];
                var tog = playerStats.GetValueOrDefault("TOG", 0);

                foreach (var stat in SCORING)
                {
                    int statValue = playerStats.GetValueOrDefault(stat.Key, 0);
                    statCells.Append($"<td title='{stat.Value * statValue}' class='stat'>{statValue}</td>");
                }

                return $@"<tr class='stats_row'><td title='{team}' class='playerteam {team.ToLower()}'>{teamShort}</td><td title='{playerAge}' class='{playerClass}'>{playerName}</td><td title='{playerRank}' class='pos'>{positionString}</td><td class='af'>{score}</td><td title='{GetTogScore(score, tog)}' class='tog'>{tog}</td>{statCells}</tr>";
            }
            else if (matchStatus == "scheduled")
            {
                foreach (var stat in SCORING)
                {
                    statCells.Append("<td class='stat'>0</td>");
                }

                return $@"<tr class='stats_row'><td title='{team}' class='playerteam {team.ToLower()}'>{teamShort}</td><td title='{playerAge}' class='{playerClass}'>{playerName}</td><td title='{playerRank}' class='pos'>{positionString}</td><td class='af'>0</td><td class='tog'>0</td>{statCells}</tr>";
            }

            return string.Empty;
        }

        private static string GetMatchHtml(JsonNode match, JsonArray squads, JsonArray venues)
        {
            var homeSquad = squads.FirstOrDefault(squad => squad!["id"]!.GetValue<int>() == match["home_squad_id"]!.GetValue<int>());
            var awaySquad = squads.FirstOrDefault(squad => squad!["id"]!.GetValue<int>() == match["away_squad_id"]!.GetValue<int>());
            var venueElem = venues.FirstOrDefault(v => v!["id"]!.GetValue<int>() == match["venue_id"]!.GetValue<int>());

            var homeTeam = homeSquad?["name"]?.ToString() ?? string.Empty;
            var homeTeamFull = homeSquad?["full_name"]?.ToString() ?? string.Empty;
            var awayTeam = awaySquad?["name"]?.ToString() ?? string.Empty;
            var awayTeamFull = awaySquad?["full_name"]?.ToString() ?? string.Empty;
            var venue = venueElem?["short_name"]?.ToString() ?? string.Empty;
            var venueAlt = venueElem?["name"]?.ToString() ?? string.Empty;

            var timeStr = FormatMatchTime(match, venueElem?["timezone"]?.ToString() ?? string.Empty);
            var matchScore = FormatMatchScore(match);

            return $@"<tr class='match_header {homeTeam.ToLower()}'><td colspan='18'><span title='{homeTeamFull}' class='teamname home'>{homeTeam}</span>{matchScore["home"]} - {matchScore["away"]}<span title='{awayTeamFull}' class='teamname away'>{awayTeam}</span><span class='matchtime'>{timeStr}</span> @ <span title='{venueAlt}' class='venuename'>{venue}</span></td></tr>{System.Environment.NewLine}";
        }

        private static Dictionary<string, string> FormatMatchScore(JsonNode match)
        {
            var homeGoals = match["home_goals"]?.GetValue<int?>() ?? 0;
            var homeBehinds = match["home_behinds"]?.GetValue<int?>() ?? 0;
            var homeScore = match["home_score"]?.GetValue<int?>() ?? 0;

            var awayGoals = match["away_goals"]?.GetValue<int?>() ?? 0;
            var awayBehinds = match["away_behinds"]?.GetValue<int?>() ?? 0;
            var awayScore = match["away_score"]?.GetValue<int?>() ?? 0;

            return new Dictionary<string, string>
            {
                { "home", $"<span title='{homeGoals}.{homeBehinds}' class='totalscore home'>{homeScore}</span>" },
                { "away", $"<span title='{awayGoals}.{awayBehinds}' class='totalscore away'>{awayScore}</span>" }
            };
        }

        private static string FormatMatchTime(JsonNode match, string timezoneStr)
        {
            if (match["status"]?.ToString() == "complete")
            {
                return "FT";
            }

            if (match["clock"] is JsonNode clock)
            {
                var quarter = clock["p"]?.ToString();
                var seconds = clock["s"]!.GetValue<int>();

                if (seconds < 0)
                {
                    return quarter switch
                    {
                        "Q1" => "QT",
                        "Q2" => "HT",
                        "Q3" => "3QT",
                        "Q4" => "FT",
                        _ => quarter ?? string.Empty
                    };
                }

                var minutes = seconds / 60;
                seconds %= 60;
                return $"{quarter} {minutes}:{seconds:D2}";
            }

            var matchDate = DateTimeOffset.Parse(match["date"]!.ToString());
            var matchDateStr = matchDate.ToString("MMMM d, h:mmtt");

            // Convert match time to local time
            var localDate = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(matchDate, timezoneStr);

            // Check if local time is different from match time
            if (localDate.TotalOffsetMinutes != matchDate.TotalOffsetMinutes)
            {
                return $"{matchDateStr} ({localDate:h:mmtt})";
            }

            return matchDateStr;
        }

        private static IEnumerable<JsonNode?> GetMatchPlayers(JsonNode match, JsonArray players)
        {
            var homeSquadId = match["home_squad_id"]!.GetValue<int>();
            var awaySquadId = match["away_squad_id"]!.GetValue<int>();
            var matchActive = match["status"]?.GetValue<string>() == "active";

            return players.Where(player =>
            {
                var squadId = player!["squad_id"]!.GetValue<int>();
                var isPlaying = player!["status"]?.GetValue<string>() == "playing";
                return (squadId == homeSquadId || squadId == awaySquadId) && (isPlaying || !matchActive);
            });
        }

        private static string GetAgeString(string dateOfBirth)
        {
            if (DateTime.TryParse(dateOfBirth, out DateTime dob))
            {
                var today = DateTime.Today;
                var age = today.Year - dob.Year;
                var months = today.Month - dob.Month;

                if (today.Day < dob.Day)
                {
                    months--;
                }

                if (months < 0)
                {
                    age--;
                    months += 12;
                }

                return $"{age} years, {months} months";
            }

            return "Ageless";
        }

        private static decimal GetTogScore(int score, int tog)
        {
            return tog == 0 ? 0 : Math.Round(score * (100 / (decimal)tog), 0);
        }

        private static async Task<JsonNode?> MakeRequestAsync(string url)
        {
            url = $"{url}?t={GetNow().ToUnixTimeSeconds()}";
            var response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            using var content = response.Content;
            var contentEncoding = content.Headers.ContentEncoding.FirstOrDefault();
            var stream = await content.ReadAsStreamAsync();

            if (contentEncoding != null && contentEncoding.Equals("gzip", StringComparison.OrdinalIgnoreCase))
            {
                using var gzipStream = new GZipStream(stream, CompressionMode.Decompress);
                return await JsonSerializer.DeserializeAsync<JsonNode>(gzipStream);
            }
            else
            {
                return await JsonSerializer.DeserializeAsync<JsonNode>(stream);
            }
        }

        private partial class MatchComparer : IComparer<JsonNode?>
        {
            private static readonly Regex playingOrCompleteRegex = MatchStatus();

            public int Compare(JsonNode? x, JsonNode? y)
            {
                var statusX = x?["status"]?.ToString();
                var statusY = y?["status"]?.ToString();

                bool isPlayingOrCompleteX = playingOrCompleteRegex.IsMatch(statusX ?? "");
                bool isPlayingOrCompleteY = playingOrCompleteRegex.IsMatch(statusY ?? "");

                if (isPlayingOrCompleteX != isPlayingOrCompleteY)
                {
                    return isPlayingOrCompleteX ? -1 : 1;
                }

                var dateX = x?["date"]?.ToString();
                var dateY = y?["date"]?.ToString();

                if (dateX != null && dateY != null)
                {
                    return isPlayingOrCompleteX
                        ? DateTimeOffset.Parse(dateY).CompareTo(DateTimeOffset.Parse(dateX))
                        : DateTimeOffset.Parse(dateX).CompareTo(DateTimeOffset.Parse(dateY));
                }

                return 0;
            }

            [GeneratedRegex("^(playing|complete)$", RegexOptions.Compiled)]
            private static partial Regex MatchStatus();
        }

        private static async Task<T?> GetCachedDataAsync<T>(string blobName, Func<T, DateTimeOffset> getCacheExpiry, Func<Task<T?>>? fetchData = null)
            where T : JsonNode
        {
            fetchData ??= () => Task.FromResult(default(T));

            var blobClient = _containerClient.GetBlobClient(blobName);

            // Create the container if it doesn't exist
            await _containerClient.CreateIfNotExistsAsync();

            if (await blobClient.ExistsAsync())
            {
                var blobProperties = await blobClient.GetPropertiesAsync();
                if (blobProperties.Value.Metadata.TryGetValue("ExpiresOn", out var expiresOnString) &&
                    DateTimeOffset.TryParse(expiresOnString, out var cachedExpiresOn) &&
                    cachedExpiresOn > GetNow())
                {
                    using var stream = new MemoryStream();
                    await blobClient.DownloadToAsync(stream);
                    stream.Position = 0;
                    var data = await JsonSerializer.DeserializeAsync<T>(stream);
                    return data;
                }
            }

            T? fetchedData = await fetchData();

            if (fetchedData != null)
            {
                var cacheExpiry = getCacheExpiry(fetchedData);

                using var stream = new MemoryStream();
                await JsonSerializer.SerializeAsync(stream, fetchedData);
                stream.Position = 0;

                await blobClient.UploadAsync(stream, overwrite: true);

                var metadata = new Dictionary<string, string>
                {
                    { "ExpiresOn", cacheExpiry.ToString("o") }
                };
                await blobClient.SetMetadataAsync(metadata);

                var httpHeaders = new BlobHttpHeaders
                {
                    ContentType = "application/json",
                    CacheControl = "public, max-age=86400"
                };
                await blobClient.SetHttpHeadersAsync(httpHeaders);
            }

            return fetchedData;
        }

        private static DateTimeOffset GetCacheExpiry(JsonNode currentRound)
        {
            if (currentRound is JsonObject roundObject && roundObject["matches"] is JsonArray matches)
            {
                // Check if any match in the current round is "playing"
                if (matches.Any(match => match?["status"]?.ToString() == "playing"))
                {
                    // Use the short cache if any match is "playing"
                    return GetNow().AddMinutes(MIN_CACHE_LIFETIME_SECONDS);
                }
                else
                {
                    // Find the next scheduled match in the round
                    var nextScheduledMatch = matches.FirstOrDefault(match => match?["status"]?.ToString() == "scheduled");

                    if (nextScheduledMatch != null)
                    {
                        // Cache until the nearly the start time of the next scheduled match
                        return DateTimeOffset.Parse(nextScheduledMatch["date"]!.ToString());
                    }
                }
            }

            // If there is no scheduled match left or no matches at all, cache until the end of the day
            return GetMidnight();
        }

        private static DateTimeOffset GetMidnight()
        {
            // return the first instance of tomorrow
            var currentTime = GetNow();
            return new
                DateTimeOffset(currentTime.Year, currentTime.Month, currentTime.Day, 0, 0, 0, _timeZoneInfo.BaseUtcOffset)
                .AddDays(1);
        }

        private static DateTimeOffset GetNow()
        {
            var now = TimeZoneInfo.ConvertTimeFromUtc(DateTimeOffset.UtcNow.DateTime, _timeZoneInfo);
            return new DateTimeOffset(now, _timeZoneInfo.GetUtcOffset(now));
        }

    }
}