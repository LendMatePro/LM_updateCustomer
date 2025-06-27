import {
  GetItemCommand,
  TransactWriteItemsCommand
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { ddbClient } from "./ddbClient.js";

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

export const handler = async (event) => {
  const respond = (statusCode, message) => ({
    statusCode,
    headers: {
      "Access-Control-Allow-Origin": "*"
    },
    body: typeof message === "string" ? message : JSON.stringify(message)
  });

  try {
    const payload = JSON.parse(event.body);
    const { customerId, name, address, email, phone } = payload;

    if (!customerId || !phone || !name) {
      return respond(400, { message: "Missing required fields." });
    }

    const existing = await ddbClient.send(
      new GetItemCommand({
        TableName: TABLE_NAME,
        Key: marshall({ PK: "CUSTOMER", SK: customerId })
      })
    );

    if (!existing.Item) {
      return respond(404, { message: "Customer not found." });
    }

    const oldData = unmarshall(existing.Item);
    const oldPhone = oldData.phone;
    const oldNormalizedName = oldData.name.trim().toUpperCase().replace(/\s+/g, "_");
    const newNormalizedName = name.trim().toUpperCase().replace(/\s+/g, "_");

    const transactItems = [];

    // Always update main CUSTOMER record
    transactItems.push({
      Put: {
        TableName: TABLE_NAME,
        Item: marshall({
          PK: "CUSTOMER",
          SK: customerId,
          customerId,
          name,
          address,
          email,
          phone
        }),
        ConditionExpression: "attribute_exists(PK)"
      }
    });

    if (oldPhone !== phone) {
      // New phone lock - ensure uniqueness
      transactItems.push({
        Put: {
          TableName: TABLE_NAME,
          Item: marshall({
            PK: `CUSTOMER_PHONE#${phone}`,
            SK: "LOCK",
            customerId
          }),
          ConditionExpression: "attribute_not_exists(PK)"
        }
      });

      // New CUSTOMER_LOOKUP record
      transactItems.push({
        Put: {
          TableName: TABLE_NAME,
          Item: marshall({
            PK: "CUSTOMER_LOOKUP",
            SK: `${phone}#${newNormalizedName}`,
            Info: {
              customerId,
              name,
              address,
              email,
              phone
            }
          }),
          ConditionExpression: "attribute_not_exists(PK)"
        }
      });

      // Delete old phone lock
      transactItems.push({
        Delete: {
          TableName: TABLE_NAME,
          Key: marshall({
            PK: `CUSTOMER_PHONE#${oldPhone}`,
            SK: "LOCK"
          })
        }
      });

      // Delete old CUSTOMER_LOOKUP
      transactItems.push({
        Delete: {
          TableName: TABLE_NAME,
          Key: marshall({
            PK: "CUSTOMER_LOOKUP",
            SK: `${oldPhone}#${oldNormalizedName}`
          })
        }
      });
    } else if (oldNormalizedName !== newNormalizedName) {
      // Name changed but phone is the same – delete old lookup, insert new
      transactItems.push({
        Delete: {
          TableName: TABLE_NAME,
          Key: marshall({
            PK: "CUSTOMER_LOOKUP",
            SK: `${phone}#${oldNormalizedName}`
          })
        }
      });

      transactItems.push({
        Put: {
          TableName: TABLE_NAME,
          Item: marshall({
            PK: "CUSTOMER_LOOKUP",
            SK: `${phone}#${newNormalizedName}`,
            Info: {
              customerId,
              name,
              address,
              email,
              phone
            }
          }),
          ConditionExpression: "attribute_not_exists(PK)"
        }
      });
    } else {
      // No change to lookup key – just update contents
      transactItems.push({
        Put: {
          TableName: TABLE_NAME,
          Item: marshall({
            PK: "CUSTOMER_LOOKUP",
            SK: `${phone}#${newNormalizedName}`,
            Info: {
              customerId,
              name,
              address,
              email,
              phone
            }
          }),
          ConditionExpression: "attribute_exists(PK)"
        }
      });
    }

    await ddbClient.send(new TransactWriteItemsCommand({ TransactItems: transactItems }));

    return respond(200, { message: "Customer updated successfully." });

  } catch (error) {
    console.error("UpdateCustomer Error:", error);
    const message =
      error.name === "TransactionCanceledException"
        ? "Phone number already exists."
        : error.message;
    return respond(400, { message });
  }
};
