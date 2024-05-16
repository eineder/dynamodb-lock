const dynamodb = require("@aws-sdk/client-dynamodb");
const { createLocksTable } = require("../lock");

const createPrerequisites = async (client, locksTable) => {
  await createLocksTable(client, locksTable);
};

const clearLocks = async (client, locksTable) => {
  const scanCommand = new dynamodb.ScanCommand({ TableName: locksTable });
  const scanResp = await client.send(scanCommand);
  for (const item of scanResp.Items) {
    const deleteCommand = new dynamodb.DeleteItemCommand({
      TableName: locksTable,
      Key: {
        PK: item.PK,
      },
    });
    await client.send(deleteCommand);
  }
};

module.exports = { createPrerequisites, clearLocks };
