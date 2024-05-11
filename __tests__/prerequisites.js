const dynamodb = require("@aws-sdk/client-dynamodb");

const createPrerequisites = async (client, locksTable, lockableItemsTable) => {
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

  try {
    const command = new dynamodb.CreateTableCommand({
      TableName: lockableItemsTable,
      KeySchema: [{ AttributeName: "PK", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "PK", AttributeType: "S" }],
      BillingMode: "PAY_PER_REQUEST",
    });
    await client.send(command);
    console.log(`Table '${lockableItemsTable}' created.`);
  } catch (err) {
    if (!(err.name === "ResourceInUseException")) {
      throw err;
    }
  }

  const putItemsCommand = new dynamodb.BatchWriteItemCommand({
    RequestItems: {
      [lockableItemsTable]: Array.from({ length: 10 }, (_, i) => ({
        PutRequest: {
          Item: {
            PK: { S: `${i}` },
            Item: { S: `This is item ${i}.` },
          },
        },
      })),
    },
  });
  await client.send(putItemsCommand);
  console.log("Items added to LockableItems table.");
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
