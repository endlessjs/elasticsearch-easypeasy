require("array.prototype.flatmap").shim();
const { Client } = require("@elastic/elasticsearch");
const elasticsearch = require("elasticsearch");

class ElasticsearchEasypeasy {
  constructor({ host, host_url, user, password }) {
    this.host = host;
    this.user = user;
    this.password = password;

    this.client = new Client({
      node: host,
      auth: {
        username,
        password
      }
    });

    this.client_old = new elasticsearch.Client({
      host: host_url,
      log: "trace"
    });
  }

  indiceCreate = async (index, body) => {
    return await this.client.indices.create(
      {
        index,
        body
      },
      { ignore: [400] }
    );
  };

  search = async (index, body) => {
    const response = this.client_old.search({
      index,
      body
    });

    return response;
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

      const response = await this.client.count({ index: "tweets" });

      return response;
    } catch (err) {
      return err;
    }
  };
}

module.exports = ElasticsearchEasypeasy;
