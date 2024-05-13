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
async function acquireLock(
  client,
  resourceName,
  locksTable = "LOCKS",
  timeoutSeconds = 600,
  giveUpAfterSeconds = 10
) {
  const transactionId = crypto.randomUUID();
  const command = createUpdateCommand(
    timeoutSeconds,
    locksTable,
    resourceName,
    transactionId
  );
  let locked = await sendUpdateCommand(client, command);

  if (locked)
    return new Lock(
      client,
      resourceName,
      transactionId,
      locksTable,
      timeoutSeconds
    );

  const giveUpAt = Date.now() + giveUpAfterSeconds * 1000;
  while (!locked && Date.now() < giveUpAt) {
    console.log(`${giveUpAt - Date.now()} - Waiting for lock...\n`);
    await new Promise((resolve) => setTimeout(resolve, 1000));
    locked = await sendUpdateCommand(client, command);
  }

  if (locked)
    return new Lock(
      client,
      resourceName,
      transactionId,
      locksTable,
      timeoutSeconds
    );

  return null;
}

async function sendUpdateCommand(client, command) {
  try {
    await client.send(command);
    return true;
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      // Item already locked
      return false;
    }
    throw err;
  }
}

function createUpdateCommand(
  timeoutSeconds,
  locksTable,
  resourceName,
  transactionId
) {
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
  return command;
}

module.exports = { acquireLock };
