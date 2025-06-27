import {DynamoDBClient} from "@aws-sdk/client-dynamodb";

const REGION= "ap-southeast-1";
export const ddbClient = new DynamoDBClient({ region: REGION });
