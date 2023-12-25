const https = require('https')
const { v4: uuid } = require('uuid')
const { DynamoDBClient, BatchWriteItemCommand } = require("@aws-sdk/client-dynamodb")
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb")

class PlatformProcessor {
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
		const rawgplatforms = await this.fetchplatformsFromRawg()
		const platforms = rawgplatforms.map(g => this.mapRawgplatformsToplatforms(g))
		const splitIndex = Math.ceil(platforms.length / 2)
		const [first, second] = [platforms.slice(0, splitIndex), platforms.slice(splitIndex)]
		await this.saveplatformsToDynamo(first)
		await this.saveplatformsToDynamo(second)
	}

	async fetchplatformsFromRawg() {
		const params = new URLSearchParams()
		params.set('key', '36d790888755494d956479f67b742e58')
		const rawgplatforms = await this.makeRequest(params)
		return rawgplatforms
	}

	async saveplatformsToDynamo(platforms) {
		const params = {
			RequestItems: {
				[this.table]: platforms.map(g => {
					return {
						PutRequest: {
							Item: marshall(g, { removeUndefinedValues: true }),
						}
					}
				})
			}
		}

		try {
			const response = await this.docClient.send(new BatchWriteItemCommand(params))
			console.log(response)
		} catch (e) {
			console.error(`--> error saving platforms to dynamo`)
			console.error(e)
		}
	}

	mapRawgplatformsToplatforms(rawgplatform) {
		return {
			id: uuid(),
			entityType: 'Platform',
			sourceId: rawgplatform.id,
			name: rawgplatform.name,
			slug: rawgplatform.slug,
			gamesCount: rawgplatform.games_count,
			imageBackground: rawgplatform.image_background,
			image: rawgplatform.image,
			yearStart: rawgplatform.year_start,
			yearEnd: rawgplatform.year_end,
		}
	}

	async makeRequest(params) {
		return new Promise((resolve, reject) => {
			https.get(
				`https://api.rawg.io/api/platforms?${params.toString()}`,
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
					reject(new Error('Could not fetch platforms'))
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

const platformProcessor = new PlatformProcessor(env)
platformProcessor
	.process()
	.then(() => console.log(`--> success!`))
	.catch((err) => console.error(err))
	.finally(() => process.exit(1))
