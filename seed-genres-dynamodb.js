const https = require('https')
const { v4: uuid } = require('uuid')
const { DynamoDBClient, QueryCommand, DeleteItemCommand, ScanCommand } = require("@aws-sdk/client-dynamodb")
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb")

class GenreProcessor {
	tables = new Map([
		['dev', 'game-hub-user-api-dev'],
		['int', 'game-hub-user-api-int'],
	])

	constructor(env) {
		this.env = env
		this.table = this.tables.get(this.env)

		let options = {}
		if (this.env === 'dev') {
			options = {
				region: `localhost`,
				endpoint: `http://localhost:8000`,
			}
		}

		this.docClient = new DynamoDBClient(options)
	}

	async process() {
		const rawgGenres = await this.fetchGenresFromRawg()
		const genres = await this.mapRawgGenresToGenres(rawgGenres)
		// await this.saveGenresToDynamo(rawgGenres)
	}

	async fetchGenresFromRawg() {
		const params = new URLSearchParams()
		params.set('key', '36d790888755494d956479f67b742e58')
		const rawgGenres = await this.makeRequest(params)
		const genres = await Promise.all(rawgGenres.map(async g => await this.mapRawgGenresToGenres(g)))
		return genres
	}

	async saveGenresToDynamo(genres) {
		const tableName = this.tables.get(this.env)
		const params = {

		}
	}

	async mapRawgGenresToGenres(rawgGenre) {
		let games = []
		if (rawgGenre.games) {
			games = await Promise.all(rawgGenre.games.map(g => this.mapRawgGameToGame(g)))
		}
		return {
			id: uuid(),
			sourceId: rawgGenre.id,
			name: rawgGenre.name,
			slug: rawgGenre.slug,
			gamesCount: rawgGenre.games_count,
			imageBackground: rawgGenre.image_background,
			games: games ?? [],
		}
	}

	async mapRawgGameToGame(rawgGame) {
		const game = await this.getGameBySourceId(rawgGame.id)
		return {
			...rawgGame,
			sourceId: game?.sourceId ?? -1,
			id: game?.id ?? uuid(),
		}
	}

	async getGameBySourceId(sourceId) {
		if (!sourceId || typeof sourceId !== 'number') {
			return []
		}

		const params = {
			Limit: 21182,
			TableName: this.table,
			ExpressionAttributeNames: {
				'#entityType': 'entityType',
				'#sourceId': 'sourceId',
			},
			ExpressionAttributeValues: marshall({
				':entityType': 'Game',
				':sourceId': sourceId,
			}),
			FilterExpression: `#entityType = :entityType AND #sourceId = :sourceId`,
		}

		const command = new ScanCommand(params)
		const response = await this.docClient.send(command)
		const items = response.Items?.map(i => unmarshall(i))
		return items?.length > 0 ? items[0] : {}
	}

	async makeRequest(params) {
		return new Promise((resolve, reject) => {
			https.get(
				`https://api.rawg.io/api/genres?${params.toString()}`,
				(res) => {
					let data = ''

					res.on('data', (chunk) => {
						data += chunk
					})

					res.on('end', () => {
						const dataJson = JSON.parse(data)
						resolve(dataJson.results)
					})
				}).on('error', (err) => {
					console.error(err)
					reject(new Error('Could not fetch genres'))
				})
		})
	}
}

const env = process.argv[2]

if (env !== 'dev' && env !== 'int') {
	console.log('--> invalid environment')
	process.exit(1)
}

console.log(`--> fetching data for the following environment: ${env}`)

const genreProcessor = new GenreProcessor(env)
genreProcessor
	.process()
	.then(() => console.log(`--> success!`))
	.catch((err) => console.error(err))
	.finally(() => process.exit(1))
