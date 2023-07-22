const { DynamoDBClient, QueryCommand, DeleteItemCommand } = require("@aws-sdk/client-dynamodb")
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb")

const dynamoDB = new DynamoDBClient({
	region: 'localhost',
	endpoint: 'http://localhost:8000',
})

// const dynamoDB = new DynamoDBClient({
// 	region: 'us-east-1',
// })

const tableName = `game-hub-user-api-dev`

const params = {
	TableName: tableName,
	KeyConditionExpression: `#entityType = :entityType`,
	ExpressionAttributeNames: {
		'#entityType': `entityType`,
	},
	ExpressionAttributeValues: marshall({
		':entityType': `Game`,
	}),
}

const queryCommand = new QueryCommand(params)

dynamoDB
	.send(queryCommand)
	.then(data => {
		try {
			data.Items.map(i => {
				const game = unmarshall(i)
				deleteGame(game)
			})
		} catch (e) {
			throw new Error(e)
		}
	})
	.catch(e => {
		console.error(e)
	})

function deleteGame(game) {
	const deleteCommand = new DeleteItemCommand({
		"TableName": tableName,
		"Key": {
			"entityType": {
				"S": "Game"
			},
			"id": {
				"S": game.id
			}
		},
	})

	dynamoDB
		.send(deleteCommand)
		.then(response => console.log(`--> success`))
		.catch(e => console.error(e))
}
