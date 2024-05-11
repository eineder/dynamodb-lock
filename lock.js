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

  /**
   * Releases the lock on the resource.
   * @returns {Promise<boolean>} - True if the lock was released, false if the lock was not found.
   */
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

/**
 * Acquires a lock on a resource.
 * @param client - An AWS SDK v3 DynamoDBClient
 * @param resourceName - The name of the resource being locked, e.g. MY_TABLE:MY_ITEM. Note that this
 * does not have follow any formal format as long as all locks use the same format to identify a resource.
 * @param locksTable - The name of the table where locks are stored. Defaults to "LOCKS".
 * @param timeoutSeconds - The number of seconds before the lock expires. Defaults to 600 (10 minutes).
 * @returns {Promise<Lock|null>} - A lock object if the lock was acquired, or null if the resource is already locked.
 */
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
