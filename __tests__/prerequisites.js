const dynamodb = require("@aws-sdk/client-dynamodb");

const createPrerequisites = async (client, locksTable) => {
  try {
    const command = new dynamodb.CreateTableCommand({
      TableName: locksTable,
      KeySchema: [{ AttributeName: "PK", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "PK", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    });
    await client.send(command);
    console.log(`Table '${locksTable}' created.`);
  } catch (err) {
    if (!(err.name === "ResourceInUseException")) {
      throw err;
    }
  }
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
