const {
  UpdateItemCommand,
  DeleteItemCommand,
} = require("@aws-sdk/client-dynamodb");
const crypto = require("crypto");

class Lock {
  constructor(
    client,
    resourceName,
    transactionId,
    locksTable = "LOCKS",
    timeoutSeconds = 600
  ) {
    this.client = client;
    this.resourceName = resourceName;
    this.locksTable = locksTable;
    this.transactionId = transactionId;
    this.timeoutSeconds = timeoutSeconds;
  }

  async release() {
    const command = new DeleteItemCommand({
      TableName: this.locksTable,
      Key: {
        PK: { S: this.resourceName },
      },
      ConditionExpression: "transactionId = :transactionId",
      ExpressionAttributeValues: {
        ":transactionId": { S: this.transactionId },
      },
    });
    try {
      await this.client.send(command);
      return true;
    } catch (err) {
      if (
        [
          "ConditionalCheckFailedException",
          "ResourceNotFoundException",
        ].includes(err.name)
      ) {
        return false;
      }
      throw err;
    }
  }
}

const acquireLock = async (
  client,
  resourceName,
  locksTable = "LOCKS",
  timeoutSeconds = 600
) => {
  const transactionId = crypto.randomUUID();
  const now = new Date().toISOString();
  const timeoutRaw = Date.now() + timeoutSeconds * 1000;
  const expiresAt = new Date(timeoutRaw).toISOString();
  const command = new UpdateItemCommand({
    TableName: locksTable,
    Key: {
      PK: { S: resourceName },
    },
    UpdateExpression:
      "SET transactionId = :transactionId, expiresAt = :expiresAt",
    ConditionExpression:
      "attribute_not_exists(transactionId) OR expiresAt < :now",
    ExpressionAttributeValues: {
      ":transactionId": { S: transactionId },
      ":expiresAt": { S: expiresAt },
      ":now": { S: now },
    },
  });
  try {
    await client.send(command);
    return new Lock(
      client,
      resourceName,
      transactionId,
      locksTable,
      timeoutSeconds
    );
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      // Item already locked
      return null;
    }
    throw err;
  }
};

module.exports = { acquireLock };
