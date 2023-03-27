using System;
using System.Threading.Tasks;
using Azure.Storage.Files.Shares;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Ascend
{
    public class AscendTimerTrigger
    {
        [FunctionName("AscendTimerTrigger")]
        public void Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            // Build configuration object
            var Config = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            AscendTimerTrigger ATT = new AscendTimerTrigger();

            string SourceConnection = Config["SourceShareConnectionString"];
            string SourceShare = Config["SourceShareName"];
            string SourceFile = Config["SourceFilePath"];
            string DestinationConnection = Config["DestinationShareConnectionString"];
            string DestinationShare = Config["DestinationShareName"];
            string DestinationFile = Config["DestinationFilePath"];
            ATT.CopyFileAsync(SourceConnection, SourceShare, SourceFile, DestinationConnection, DestinationShare, DestinationFile).GetAwaiter().GetResult();
            log.LogInformation($"Copy File Sync is completed at: {DateTime.Now}");
        }

        //-------------------------------------------------
        // Copy file within a directory
        //-------------------------------------------------
        public async Task CopyFileAsync(string sourceConnection, string sourceShare, string sourceFilePath, string destinationConnection, string destinationShare, string destFilePath)
        {

            // Get a reference to the file we created previously
            ShareFileClient sourceFile = new ShareFileClient(sourceConnection, sourceShare, sourceFilePath);

            // Ensure that the source file exists
            if (await sourceFile.ExistsAsync())
            {
                // Get a reference to the destination file
                ShareFileClient destFile = new ShareFileClient(destinationConnection, destinationShare, destFilePath);

                // Start the copy operation
                await destFile.StartCopyAsync(sourceFile.Uri);

                if (await destFile.ExistsAsync())
                {
                    Console.WriteLine($"{sourceFile.Uri} copied to {destFile.Uri}");
                }
            }
        }
    }
}
