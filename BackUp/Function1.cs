
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using System;

public static class BackUp
{
    [FunctionName("BackUp")]
    public static async Task<IActionResult> Run(
    [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
    ILogger log)
    {
        if (req.Body == null)
        {
            log.LogError("The HTTP request body is null.");
            return new BadRequestObjectResult("The HTTP request body is null.");
        }

        var str = "Server=tcp:bluedbserver.database.windows.net,1433;Initial Catalog=bluetekdb;Persist Security Info=False;User ID=CloudSAe35f64c8;Password=@Bluetek2023;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

        using (SqlConnection con = new SqlConnection(str))
        {
            await con.OpenAsync();

            using (StreamReader reader = new StreamReader(req.Body))
            {
                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();

                    if (!string.IsNullOrEmpty(line))
                    {
                        try
                        {
                            dynamic data = JsonConvert.DeserializeObject(line);

                            var id = (string)data.id;
                            var Time = (DateTime)data.ts;
                            var RH = (float)data.RH;
                            var T = (float)data.T;
                            var TVOC = (long)data.TVOC;
                            var CO2 = (long)data.CO2;
                            var pm1 = (long)data.pm1;
                            var pm2_5 = (long)data.pm2_5;
                            var pm10 = (long)data.pm10;

                            var query = @"INSERT INTO [dbo].[TablaMu]
                                    ([id]
                                    ,[Time]
                                    ,[RH]
                                    ,[T]
                                    ,[TVOC]
                                    ,[CO2]
                                    ,[pm1]
                                    ,[pm2_5]
                                    ,[pm10])
                                VALUES
                                    (@id
                                    ,@Time
                                    ,@RH
                                    ,@T
                                    ,@TVOC
                                    ,@CO2
                                    ,@pm1
                                    ,@pm2_5
                                    ,@pm10)";

                            using (SqlCommand cmd = new SqlCommand(query, con))
                            {
                                cmd.Parameters.AddWithValue("@id", id);
                                cmd.Parameters.AddWithValue("@Time", Time);
                                cmd.Parameters.AddWithValue("@RH", RH);
                                cmd.Parameters.AddWithValue("@T", T);
                                cmd.Parameters.AddWithValue("@TVOC", TVOC);
                                cmd.Parameters.AddWithValue("@CO2", CO2);
                                cmd.Parameters.AddWithValue("@pm1", pm1);
                                cmd.Parameters.AddWithValue("@pm2_5", pm2_5);
                                cmd.Parameters.AddWithValue("@pm10", pm10);

                                var row = await cmd.ExecuteNonQueryAsync();

                                log.LogInformation($"Row inserted: {DateTime.Now}");
                            }
                        }
                        catch (Exception ex)
                        {
                            log.LogError(ex, "Failed to process line");
                        }
                    }
                }
            }
        }

        log.LogInformation("C# HTTP trigger function processed a request.");

        return new OkResult();
    }
}


