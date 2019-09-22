from elasticsearch import Elasticsearch
from elasticsearch import helpers

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        # body={
        #    'mappings': {},
        #    'settings': {},
        #},
    )
    # pdts = all_products()
    # for p in pdts:
        # index_product(es, p)
    products_index(es)


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body={
            "name": product.name,
            "image": product.image,
        }
    )

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Indexed {}".format("A Great Product"))

def products_index(es):
    actions = []
    pdts = all_products()
    for product in pdts:
        action = {
            "_index": INDEX_NAME,
            "_type": DOC_TYPE,
            "_id": product.id,
            "_source": {
                "name": product.name,
                "image": product.image,
                "description":product.description
                }
                }
        actions.append(action)

    helpers.bulk(es, actions)


if __name__ == '__main__':
    main()
