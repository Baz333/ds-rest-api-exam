import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { Movie, MovieCast } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

type ResponseBody = {
  data: {
    movie: Movie;
    cast?: MovieCast[];
  };
};

const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
  schema.definitions["MovieQueryParams"] || {}
);
const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    const parameters = event?.pathParameters;
    const crewRole = parameters?.role || undefined;
    const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;

    if (!crewRole || !movieId) {
      return {
        statusCode: 404,
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({ Message: "Invalid parameters" }),
      };
    }

    const commandInput: QueryCommandInput = {
        TableName: process.env.TABLE_NAME,
        KeyConditionExpression: "movieId = :m and crewRole = :r",
        ExpressionAttributeValues: {
            ":m": movieId,
            ":r": crewRole,
        },
    };

    const commandOutput = await ddbDocClient.send(
        new QueryCommand(commandInput)
    );

    if(!commandOutput.Items) {
        return {
            statusCode: 404,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ Message: "Invalid expression" })
        }
    }

    let names = undefined
    for (let item of commandOutput.Items) {
        if(item.crewRole === crewRole) {
            names = item.names
        }
    }

    if(!names) {
        return {
            statusCode: 404,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ Message: "No names found for this role and movie" })
        }
    } else {
        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ names })
        }
    }
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({ error }),
    };
  }
};

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}