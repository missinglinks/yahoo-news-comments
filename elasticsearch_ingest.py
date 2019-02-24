from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
from zip_archive import ZipArchive
from config import ES_SERVER

FILE = "data/articles2.zip"

INDEX = "yahoo_news_comments"
DOC_TYPE = "comment"
MAPPING = {
    "comment": {
    }
}

def init_es(es):

    if es.indices.exists(index=INDEX):
        es.indices.delete(index=INDEX)
    
    es.indices.create(INDEX)
    es.indices.put_mapping(index=INDEX, doc_type=DOC_TYPE, body=MAPPING)    

def ingest():

    es = Elasticsearch(ES_SERVER)

    archive = ZipArchive(FILE)
    for filename in tqdm(archive):
        if archive.contains(filename):
            print(filename)
            docs = []

            try:
                article = archive.get(filename)
                if article["comments"]:
                    for comment in article["comments"]:
                        comemnt["user_id"] = comment["user_id"].replace("/comments/", "")
                        for reply in comment["replies"]:
                            reply["replies_count"] = 0
                            reply["user_id"] = reply["user_id"].replace("/comments/", "")
                            docs.append({
                                "_index": INDEX,
                                "_type": DOC_TYPE,
                                "_id": reply["id"],
                                "_source": reply
                            })

                        #all_comments += comment["replies"]
                        del comment["replies"]
                        comment["parent"] = comment["id"]
                        docs.append({
                            "_index": INDEX,
                            "_type": DOC_TYPE,
                            "_id": reply["id"],
                            "_source": comment
                        })

                        #all_comments.append(comment)
            except:
                continue


            if len(docs) > 0:
                helpers.bulk(es, docs)
            
if __name__ == "__main__":
    ingest()