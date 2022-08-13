using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Capsulemon.Scheduler.ReplayPick
{
    public class App
    {
        IConfiguration configuration;
        IAmazonDynamoDB dynamoDBClient;
        string env { get; set; }
        string tblReplay { get; set; }
        string tblReplayPick { get; set; }
        List<string> datehours = new List<string>();

        public App(IConfiguration configuration_, IAmazonDynamoDB dynamoDBClient_)
        {
            configuration = configuration_;
            dynamoDBClient = dynamoDBClient_;
            env = configuration.GetValue<string>("Env");
            tblReplay = configuration.GetValue<string>("TableReplay");
            tblReplayPick = configuration.GetValue<string>("TableReplayPick");
        }

        public App Init(int interval = 60, List<string> datehours_ = null)
        {
            var now = DateTime.UtcNow;
            LambdaLogger.Log($"[ReplayPick] now : {now}");
            var datehour_d = now.AddMinutes(-1 * (interval + now.Minute % interval)).ToString("yyyyMMddHHmm");

            datehours = datehours_ ?? new List<string> { datehour_d };
            LambdaLogger.Log($"[ReplayPick] datehours count : {datehours.Count}");
            return this;
        }

        public void Run()
        {
            if (env == "dev")
            {
                DeletePick(datehours[0].Substring(0, 8));
            }

            foreach (string datehour in datehours)
            {
                LambdaLogger.Log($"[ReplayPick] datehour : {datehour}");
                //continue;
                List<Document> list_pick = GetPickedList(datehour);
                LambdaLogger.Log("[ReplayPick] " + list_pick.Count().ToString());
                foreach (var doc in list_pick)
                {
                    CopyDocumentToPick(doc["uniqueKey"]);
                }
            }
            LambdaLogger.Log("[ReplayPick] complete.");
        }

        private List<Document> GetPickedList(string datehour)
        {
            List<Document> list;
            Table table = Table.LoadTable(dynamoDBClient, tblReplay);

            QueryOperationConfig config = new QueryOperationConfig()
            {
                IndexName = "dateHour-index",
                Filter = new QueryFilter("dateHour", QueryOperator.Equal, datehour),
                Select = SelectValues.AllProjectedAttributes,
            };
            Search query = table.Query(config);
            list = query.GetRemainingAsync().Result;

            List<Document> list_pick = list
                    //.Where(p => p["score"].AsInt() >= 1500)
                    .Where(p => p["arena"].AsInt() >= 2)
                    .GroupBy(p => p["arena"])
                    .Select(g => g.OrderByDescending(p => p["score"].AsInt()).FirstOrDefault())
                    .ToList();

            return list_pick;
        }

        private void CopyDocumentToPick(string uniqueKey)
        {
            Table table1 = Table.LoadTable(dynamoDBClient, tblReplay);
            Table table2 = Table.LoadTable(dynamoDBClient, tblReplayPick);

            Document doc1 = table1.GetItemAsync(uniqueKey).Result;
            table2.PutItemAsync(doc1).Wait();
        }

        private void DeletePick(string datehour)
        {
            List<Document> list;
            Table table = Table.LoadTable(dynamoDBClient, tblReplayPick);
            DocumentBatchWrite batchWrite = table.CreateBatchWrite();

            List<DocumentBatchWrite> items = new List<DocumentBatchWrite>();
            ScanFilter filter = new ScanFilter();
            filter.AddCondition("dateHour", ScanOperator.LessThan, datehour);

            Search scan = table.Scan(filter);

            list = scan.GetRemainingAsync().Result;
            list.ForEach((x) => { batchWrite.AddItemToDelete(x); });

            batchWrite.ExecuteAsync().Wait();
        }
    }
}
