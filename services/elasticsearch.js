require("array.prototype.flatmap").shim();
const { Client } = require("@elastic/elasticsearch");
const elasticsearch = require("elasticsearch");

class ElasticsearchEasypeasy {
  constructor({ host }) {
    this.host = host;
    this.client = new Client({ node: host });
    this.client_old = new elasticsearch.Client({ host, log: "trace" });
  }

  indiceCreate = async (index, body) => {
    try {
      return await this.client.indices.create(
        {
          index,
          body
        },
        { ignore: [400] }
      );
    } catch (err) {
      return err;
    }
  };

  search = async (index, body) => {
    try {
      return this.client_old.search({
        index,
        body
      });
    } catch (err) {
      return err;
    }
  };

  bulkCreate = async (index, datas) => {
    try {
      const body = datas.flatMap(doc => [{ index: { _index: index } }, doc]);

      const { body: bulkResponse } = await this.client.bulk({
        refresh: true,
        body
      });

      if (bulkResponse.errors) {
        const erroredDocuments = [];

        bulkResponse.items.forEach((action, i) => {
          const operation = Object.keys(action)[0];
          if (action[operation].error) {
            erroredDocuments.push({
              status: action[operation].status,
              error: action[operation].error,
              operation: body[i * 2],
              document: body[i * 2 + 1]
            });
          }
        });

        return erroredDocuments;
      }

      return await this.client.count({ index: "tweets" });
    } catch (err) {
      return err;
    }
  };
}

module.exports = ElasticsearchEasypeasy;
