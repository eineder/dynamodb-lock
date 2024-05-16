const crypto = require("crypto");
const { acquireLock, LockTableNotFoundError } = require("../lock");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { createPrerequisites, clearLocks } = require("./prerequisites");

const AWS_REGION = "eu-west-1";
const LOCKS_TABLE = "Locks-TEST";

describe("Given a locks table exists", () => {
  let client;
  beforeAll(async () => {
    client = new DynamoDBClient({ region: AWS_REGION });
    await createPrerequisites(client, LOCKS_TABLE);
  });

  afterEach(async () => {
    await clearLocks(client, LOCKS_TABLE);
  });

  describe("When calling acquireLock for a free resource", () => {
    it("Returns a lock", async () => {
      const RESOURCE_NAME = resourceName();
      const lock = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE);

      expect(lock).toBeDefined();
    });
  });

  describe("When calling acquireLock for a resource with an expired lock", () => {
    it("Returns a lock", async () => {
      const RESOURCE_NAME = resourceName();
      const lock = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE, 1);
      expect(lock).toBeDefined();

      await new Promise((resolve) => setTimeout(resolve, 1500));

      const lock2 = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE);
      expect(lock2).toBeDefined();
    });
  });

  describe("When calling acquireLock for a locked resource that doesn't get released", () => {
    it("Returns null", async () => {
      const RESOURCE_NAME = resourceName();
      const lock = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE);
      expect(lock).toBeDefined();

      const lock2 = await acquireLock(
        client,
        RESOURCE_NAME,
        LOCKS_TABLE,
        600,
        1
      );
      expect(lock2).toBeNull();
    });
  });

  describe("When calling acquireLock for a locked resource that expires while waiting", () => {
    it("Returns a lock", async () => {
      const RESOURCE_NAME = resourceName();
      const lock = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE, 1, 10);
      expect(lock).toBeDefined();

      const lock2 = await acquireLock(
        client,
        RESOURCE_NAME,
        LOCKS_TABLE,
        600,
        10
      );
      expect(lock2).toBeDefined();
    });
  });

  describe("When calling acquireLock for a locked resource that gets released while waiting", () => {
    it("Returns a lock", async () => {
      const RESOURCE_NAME = resourceName();
      const lock = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE);
      expect(lock).toBeDefined();

      const lock2Promise = acquireLock(
        client,
        RESOURCE_NAME,
        LOCKS_TABLE,
        600,
        10
      );
      const releaseLockPromise = new Promise((resolve) =>
        setTimeout(resolve, 2000)
      ).then(() => lock.release());
      const results = await Promise.all([lock2Promise, releaseLockPromise]);
      const lock2 = results[0];
      expect(lock2).toBeDefined();
    });
  });

  describe("When calling release on the lock that currently locks a resource", () => {
    it("Returns true", async () => {
      const RESOURCE_NAME = resourceName();
      const lock = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE);
      expect(lock).toBeDefined();

      const released = await lock.release();
      expect(released).toBe(true);

      const lock2 = await acquireLock(client, RESOURCE_NAME, LOCKS_TABLE);
      expect(lock2).toBeDefined();
    });
  });
});

describe("Given a locks table does not exist", () => {
  let client;
  beforeAll(async () => {
    client = new DynamoDBClient({ region: AWS_REGION });
  });

  describe("When calling acquireLock", () => {
    it("Throws LockTableNotFoundError error", async () => {
      const RESOURCE_NAME = resourceName();
      await expect(
        acquireLock(client, RESOURCE_NAME, "NonExistentTable")
      ).rejects.toThrow(LockTableNotFoundError);
    });
  });
});

function resourceName() {
  return `${"TheNameOfSomeTable"}#${crypto.randomUUID()}`;
}
