const { acquireLock } = require("../lock");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { createPrerequisites, clearLocks } = require("./prerequisites");

const AWS_REGION = "eu-west-1";
const LOCKS_TABLE = "Locks-TEST";
const LOCKABLE_ITEMS_TABLE = "LockableItems-TEST";

describe("Given the tables Locks and LockableItems exist", () => {
  let client;
  beforeAll(async () => {
    client = new DynamoDBClient({ region: AWS_REGION });
    await createPrerequisites(client, LOCKS_TABLE, LOCKABLE_ITEMS_TABLE);
  });

  afterEach(async () => {
    await clearLocks(client, LOCKS_TABLE);
  });

  describe("When locking a free resource", () => {
    it("Returns a lock", async () => {
      const itemId = 5;
      const resourceName = `${LOCKABLE_ITEMS_TABLE}#${itemId}`;
      const lock = await acquireLock(client, resourceName, LOCKS_TABLE);

      expect(lock).toBeDefined();
    });
  });

  describe("When trying to lock a locked resource", () => {
    it("Returns null", async () => {
      const itemId = 5;
      const resourceName = `${LOCKABLE_ITEMS_TABLE}#${itemId}`;
      const lock = await acquireLock(client, resourceName, LOCKS_TABLE);
      expect(lock).toBeDefined();

      const lock2 = await acquireLock(client, resourceName, LOCKS_TABLE);
      expect(lock2).toBeNull();
    });
  });

  describe("When trying to lock a resource with an expired lock", () => {
    it("Returns true", async () => {
      const itemId = 5;
      const resourceName = `${LOCKABLE_ITEMS_TABLE}#${itemId}`;
      const lock = await acquireLock(client, resourceName, LOCKS_TABLE, 1);
      expect(lock).toBeDefined();

      await new Promise((resolve) => setTimeout(resolve, 1200));

      const lock2 = await acquireLock(client, resourceName, LOCKS_TABLE);
      expect(lock2).toBeDefined();
    });
  });

  describe("When trying to unlock with the right locker", () => {
    it("Returns true", async () => {
      const itemId = 5;
      const resourceName = `${LOCKABLE_ITEMS_TABLE}#${itemId}`;
      const lock = await acquireLock(client, resourceName, LOCKS_TABLE);
      expect(lock).toBeDefined();

      const released = await lock.release();
      expect(released).toBe(true);

      const lock2 = await acquireLock(client, resourceName, LOCKS_TABLE);
      expect(lock2).toBeDefined();
    });
  });
});
