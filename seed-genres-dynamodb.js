const https = require('https')
const { v4: uuid } = require('uuid')
const { DynamoDBClient, BatchWriteItemCommand } = require("@aws-sdk/client-dynamodb")
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
		const genres = rawgGenres.map(g => this.mapRawgGenresToGenres(g))
		await this.saveGenresToDynamo(genres)
	}

	async fetchGenresFromRawg() {
		const params = new URLSearchParams()
		params.set('key', '36d790888755494d956479f67b742e58')
		const rawgGenres = await this.makeRequest(params)
		return rawgGenres
	}

	async saveGenresToDynamo(genres) {
		const params = {
			RequestItems: {
				[this.table]: genres.map(g => {
					return {
						PutRequest: {
							Item: marshall(g, { removeUndefinedValues: true }),
						}
					}
				})
			}
		}

		try {
			await this.docClient.send(new BatchWriteItemCommand(params))
		} catch (e) {
			console.error(`--> error saving genres to dynamo`)
			console.error(e)
		}
	}

	mapRawgGenresToGenres(rawgGenre) {
		return {
			id: uuid(),
			entityType: 'Genre',
			sourceId: rawgGenre.id,
			name: rawgGenre.name,
			slug: rawgGenre.slug,
			gamesCount: rawgGenre.games_count,
			imageBackground: rawgGenre.image_background,
		}
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
