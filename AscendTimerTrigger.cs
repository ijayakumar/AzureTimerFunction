using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using ExcelDataReader;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;
using Newtonsoft.Json;

namespace Ascend
{
    public class AscendTimerTrigger
    {
        [FunctionName("AscendTimerTrigger")]
        public async Task RunAsync([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // Build configuration object
            var Config = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            AscendTimerTrigger ATT = new AscendTimerTrigger();

            string AccountName = Config["StorageAccountName"];
            string AccountKey = Config["StorageAccountKey"];
            string SourceConnection = Config["SourceShareConnectionString"];
            string SourceShare = Config["SourceShareName"];
            string SourceDirectory = Config["SourceFilePath"];
            string DestinationConnection = Config["DestinationShareConnectionString"];
            string DestinationShare = Config["DestinationShareName"];
            string DestinationDirectory = Config["DestinationFilePath"];
            string BlobContainerName = Config["BlobContainerName"];
            string BlobName = Config["BlobName"];
            Boolean CSVEnabled = false;
            if (Config["CSVEnabled"].Equals("True")){
                CSVEnabled = true;
            }

            // Build Blob Client Object for logging
            Task<CloudBlockBlob> BlobLogTask = GetBlobClient(SourceConnection, BlobContainerName, BlobName);
            CloudBlockBlob BlobLog = await BlobLogTask;

            Task<List<string>> FileListTask = GetAllFiles(SourceConnection, SourceShare, SourceDirectory);
            List<string> fileList = await FileListTask;
            int Count = 0;
            var ExcelSupportedExtn = new List<string>();
            ExcelSupportedExtn.Add(".XLSX");
            ExcelSupportedExtn.Add(".XLS");

            foreach (string fileName in fileList)
            {
                Count++;
                string fileExtension = Path.GetExtension(fileName);
                if (fileExtension.Equals(".CSV", StringComparison.OrdinalIgnoreCase) && CSVEnabled)
                {
                    string fileContent = ReadLastLineFromAzureFileShare(SourceConnection, SourceShare, SourceDirectory, fileName).GetAwaiter().GetResult();
                    await LogIt(BlobLog, $"{DateTime.Now} CSV File, Reference number is {Count}, Last Record is {fileContent}");
                    // Copy file from Server 1 to 2
                    string SourceFileDirPath = SourceDirectory + "/" + fileName;
                    string DestinationFileDirPath = DestinationDirectory + "/" + fileName;
                    Boolean Copied = CopyFileAsync(SourceConnection, SourceShare, SourceFileDirPath, DestinationConnection, DestinationShare, DestinationFileDirPath).GetAwaiter().GetResult();
                    if (Copied)
                    {
                        DeleteFileAsync(SourceConnection, SourceShare, SourceDirectory, fileName).GetAwaiter().GetResult();
                    }
                    Task<string> TitleValue = GetTitle(Count);
                    string Title = await TitleValue;
                    await SendEmail(Count,Title);
                }
                else if (ExcelSupportedExtn.Contains(fileExtension, StringComparer.OrdinalIgnoreCase))
                {
                    string fileContent = ReadLastRowExcelFromAzureFileShare(AccountName, AccountKey, SourceShare, SourceDirectory, fileName).GetAwaiter().GetResult();
                    await LogIt(BlobLog, $"{DateTime.Now} Excel File, Reference number is {Count}, Last Record is {fileContent}");
                    // Copy file from Server 1 to 2
                    string SourceFileDirPath = SourceDirectory + "/" + fileName;
                    string DestinationFileDirPath = DestinationDirectory + "/" + fileName;
                    Boolean Copied = CopyFileAsync(SourceConnection, SourceShare, SourceFileDirPath, DestinationConnection, DestinationShare, DestinationFileDirPath).GetAwaiter().GetResult();
                    if (Copied)
                    {
                        DeleteFileAsync(SourceConnection, SourceShare, SourceDirectory, fileName).GetAwaiter().GetResult();
                    }
                    Task<string> TitleValue = GetTitle(Count);
                    string Title = await TitleValue;
                    await SendEmail(Count,Title);
                }

            }

            //ATT.CopyFileAsync(SourceConnection, SourceShare, SourceDirectory, DestinationConnection, DestinationShare, DestinationDirectory).GetAwaiter().GetResult();
            //log.LogInformation($"Copy File Sync is completed at: {DateTime.Now}");
        }

        private async Task<CloudBlockBlob> GetBlobClient(string sourceConnection, string blobContainerName, string blobName)
        {
            // Create a BlobClient to connect to the Azure Blob container and Blob
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(sourceConnection);
            //BlobServiceClient blobServiceClient = new BlobServiceClient(sourceConnection);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(blobContainerName);
            //BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            //BlobClient blobClient = containerClient.GetBlobClient(blobName);

            // Check if the Blob already exists, and create it if it doesn't
            if (!await blob.ExistsAsync())
            {
                await blob.UploadTextAsync("");
            }

            return blob;
        }

        private async Task LogIt(CloudBlockBlob blob, string text)
        {
            string contents = blob.DownloadTextAsync().Result;
            await blob.UploadTextAsync(contents + text + "\n");
        }

        public static async Task<string> ReadLastLineFromAzureFileShare(string connectionString, string shareName, string directoryName, string fileName)
        {
            ShareClient shareClient = new ShareClient(connectionString, shareName);
            ShareDirectoryClient directoryClient = shareClient.GetDirectoryClient(directoryName);
            ShareFileClient fileClient = directoryClient.GetFileClient(fileName);

            ShareFileProperties properties = await fileClient.GetPropertiesAsync();
            long fileSize = properties.ContentLength;
            long rangeStart = Math.Max(0, fileSize - 100); // download last 100 bytes of file, or less if file size is smaller
            HttpRange range = new HttpRange(rangeStart, fileSize - rangeStart);

            ShareFileDownloadOptions downloadOptions = new ShareFileDownloadOptions()
            {
                Range = range
            };
            ShareFileDownloadInfo download = await fileClient.DownloadAsync(downloadOptions);
            byte[] buffer = new byte[download.ContentLength];
            await download.Content.ReadAsync(buffer, 0, (int)download.ContentLength);

            string fileContent = Encoding.UTF8.GetString(buffer);
            string[] lines = fileContent.Split('\n');
            string lastLine = lines[lines.Length - 1];
            return lastLine;
        }

        public static async Task<string> ReadLastRowExcelFromAzureFileShare(string accountName, string accountKey, string fileShareName, string directoryName, string fileName)
        {

            // Create a CloudStorageAccount object with the account name and account key or connection string of your Azure Storage account.
            CloudStorageAccount storageAccount = new CloudStorageAccount(new StorageCredentials(accountName, accountKey), true);

            // Create a CloudFileClient object using the CloudStorageAccount object.
            CloudFileClient fileClient = storageAccount.CreateCloudFileClient();

            // Get a reference to the CloudFileShare object representing the file share.
            CloudFileShare fileShare = fileClient.GetShareReference(fileShareName);

            // Get a reference to the CloudFileDirectory object representing the directory containing the Excel file.
            CloudFileDirectory fileDirectory = fileShare.GetRootDirectoryReference().GetDirectoryReference(directoryName);

            // Get a reference to the CloudFile object representing the Excel file.
            CloudFile cloudFile = fileDirectory.GetFileReference(fileName);
            string LastRowString = "";

            try
            {
                // Download the Excel file as a stream.
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
                    await cloudFile.DownloadToStreamAsync(memoryStream);

                    // Read the excel file
                    using (var stream = new MemoryStream(memoryStream.ToArray()))
                    {
                        using (IExcelDataReader reader = ExcelReaderFactory.CreateReader(stream))
                        {
                            DataSet result = reader.AsDataSet();

                            // Get the first worksheet
                            DataTable worksheet = result.Tables[0];

                            // Get the last row of the worksheet
                            DataRow lastRow = worksheet.Rows[worksheet.Rows.Count - 1];

                            // Get the values of the last row as an array
                            object[] lastRowValues = lastRow.ItemArray;

                            // Concatenate the values of the last row with comma separator
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < lastRowValues.Length; i++)
                            {
                                sb.Append(lastRowValues[i].ToString());
                                if (i != lastRowValues.Length - 1)
                                {
                                    sb.Append(",");
                                }
                            }

                            // Get the value in the first column of the last row
                            LastRowString = sb.ToString();

                        }
                    }
                }
            }
            catch
            {
                // Supposed to be an empty file
            }

            return LastRowString;
        }

        private async Task<string> GetTitle(int ReferenceNum)
        {
            // Create an instance of HttpClient
            using (var httpClient = new HttpClient())
            {
                // Construct the API URL with the parameter value
                var apiUrl = $"https://ascend-http-function.azurewebsites.net/api/title?refCounter={ReferenceNum}";

                // Call the GET API with the parameter and get the response
                var response = await httpClient.GetAsync(apiUrl);

                // If the response is successful
                if (response.IsSuccessStatusCode)
                {
                    // Read the response content as a string
                    return await response.Content.ReadAsStringAsync();
                }
                else
                {
                    // If the response is not successful
                    return "Title not found";
                }
            }
        }

        //-------------------------------------------------
        // Get the list of files from a directory
        //-------------------------------------------------
        private async Task<List<string>> GetAllFiles(string sourceConnection, string sourceShare, string sourceDirectory)
        {
            ShareClient share = new ShareClient(sourceConnection, sourceShare);
            ShareDirectoryClient directory = share.GetDirectoryClient(sourceDirectory);

            List<string> results = new List<string>();

            await foreach (ShareFileItem fileItem in directory.GetFilesAndDirectoriesAsync())
            {
                results.Add(fileItem.Name);
            }

            return results;
        }

        //-------------------------------------------------
        // Copy file within a directory
        //-------------------------------------------------
        public async Task<Boolean> CopyFileAsync(string sourceConnection, string sourceShare, string sourceFilePath, string destinationConnection, string destinationShare, string destFilePath)
        {

            // Get a reference to the file we created previously
            ShareFileClient sourceFile = new ShareFileClient(sourceConnection, sourceShare, sourceFilePath);
            Boolean CopySuccess = false;

            // Ensure that the source file exists
            if (await sourceFile.ExistsAsync())
            {
                // Get a reference to the destination file
                ShareFileClient destFile = new ShareFileClient(destinationConnection, destinationShare, destFilePath);

                // Start the copy operation
                await destFile.StartCopyAsync(sourceFile.Uri);

                if (await destFile.ExistsAsync())
                {
                    CopySuccess = true;
                }
            }
            return CopySuccess;
        }

        public async Task DeleteFileAsync(string connectionString, string shareName, string directoryName, string fileName)
        {
            // Initialize the share client
            ShareServiceClient shareServiceClient = new ShareServiceClient(connectionString);
            ShareClient shareClient = shareServiceClient.GetShareClient(shareName);

            // Get a reference to the file
            ShareDirectoryClient directoryClient = shareClient.GetDirectoryClient(directoryName);
            ShareFileClient fileClient = directoryClient.GetFileClient(fileName);

            // Delete the file
            Response response = await fileClient.DeleteAsync();
        }

        private async Task<Boolean> SendEmail(int RefNum,string Title)
        {
            // Set up email details
            Boolean MailSent = false;
            string fromAddress = "jayakumar.inbaraj@yahoo.com";
            string toAddress = "jayakumar.inbaraj@walmart.com";
            string subject = "Email notification for ASCEND Capstone";
            string body = $"<html><body><h4>Hi</h4><h4>Please find below the details related to file processing and transfer.</h4><p><b>Reference Counter: </b>{RefNum}</p><p><b>Title: </b>{Title}</p><p><b>Date and Time: </b>{DateTime.Now}</p><p><i>--------------------------System Generated Email--------------------------</i></p></body></html>";

            // Create email message
            MailMessage message = new MailMessage(fromAddress, toAddress, subject, body);
            message.IsBodyHtml = true;

            // Set up SMTP client
            SmtpClient client = new SmtpClient("smtp.mail.yahoo.com");
            client.Port = 587;
            client.Credentials = new NetworkCredential("jayakumar.inbaraj@yahoo.com", "fngwhfiimpearshb");
            client.EnableSsl = true;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.UseDefaultCredentials = false;

            try
            {
                // Send email message
                await client.SendMailAsync(message);
                MailSent = true;
            }
            catch
            {
                MailSent = false;
            }

            return MailSent;
        }
    }
}
