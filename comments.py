import requests
from datetime import datetime
from bs4 import BeautifulSoup
from zip_archive import ZipArchive


ARCHIVE_URL = "https://news.yahoo.co.jp/list/?c=domestic&p={page}"
COMMENT_URL = "https://headlines.yahoo.co.jp/cm/main?d={article_id}"
DOMESTIC_NEWS = "https://news.yahoo.co.jp/hl?c=dom&p={page}&d={date}"

COMMENT_API = "https://news.yahoo.co.jp/comment/plugin/v1/full/?origin=https%3A%2F%2Fheadlines.yahoo.co.jp&sort=lost_points&order=desc&page={page}&type=t&keys={keys}&full_page_url={url}&comment_num=50"
REPLIES_API = "https://news.yahoo.co.jp/comment/plugin/v1/Markup/ReplyCommentList/?propertyId=sp_news&topicId={topic_id}&parentId={parent_id}&startNum={start}&resultNum=50"

OUT_FILE = "data/articles.zip"

def get_article_links(date):
    
    page = 1
    article_links = []
    
    while True:
        print("\tfetches articles form page {}".format(page))
        rsp = requests.get(DOMESTIC_NEWS.format(page=page, date=date))
        soup = BeautifulSoup(rsp.text, "html.parser")
        new_links = []
        for item in soup.find_all("li", {"class": "listFeedWrap"}):
            try:
                new_links.append(item.find("a")["href"])
            except:
                continue
                
        if len(new_links) == 0:
            break
            
        article_links += new_links
        page += 1
        
    return article_links

def get_article_data(article_link):
    article = requests.get(article_link)
    id_ = article_link.split("?a=")[-1]
    return {
        "timestamp": datetime.now().isoformat(),
        "id": id_,
        "link": article_link,
        "html": article.text,
    }

def topic_id(article_id):
    t_id = "-".join(article_id.split("-")[:-1])
    return t_id

def parent_id(comment_id):
    p_id = comment_id.replace("comment-", "").replace("-", ".")
    return p_id

def get_comment_api_credentials(article_id):
    rsp = requests.get(COMMENT_URL.format(article_id=article_id))
    #print(COMMENT_URL.format(article_id=article_id))
    soup = BeautifulSoup(rsp.text, "html.parser")
    comment_plugin = soup.find("div", { "class": "news-comment-plugin" })
    try:
        keys = comment_plugin["data-keys"]
        url = comment_plugin["data-full-page-url"]
        return {
            "keys": keys, 
            "url": url
        }
    except:
        return None

def extract_comment_data(comment, article_id):
    
    c = {}
    c["id"] = comment["id"]
    
    #timestamp
    c["time"] = comment.find("time", { "class": "date" })["datetime"]
    
    #user
    header = comment.find("header")
    user_link = header.find("a")
    c["user_name"] = user_link.text.strip()
    c["user_id"] = user_link["href"].split("/id/")[-1]
    c["user_img"] = header.find("img")["src"]
    
    #text
    c["text"] = comment.find("p", { "class": "comment"} ).text.strip()
    
    #reactions
    good = comment.find("li", { "class": "good" })
    c["agree"] =  int(good.find("em").text.strip())
    bad = comment.find("li", { "class": "bad" })
    c["disagree"] =  int(bad.find("em").text.strip())
    
    #replies 
    replies = []
    replies_p = comment.find("p", { "class": "reply" })
    c["replies_count"] = int(replies_p.find("span", { "class": "num" }).text.strip())
    if c["replies_count"] > 0:
        #fetch replies

        start = 1
        print(c["replies_count"])
        while True:
            print(start)
            rsp = requests.get(REPLIES_API.format(topic_id=topic_id(article_id), parent_id=parent_id(c["id"]), start=start))
            data = rsp.json()
            for item in data["list"]:
                replies.append({
                    "id": item["commentId"],
                    "parent": item["parentId"],
                    "time": item["basicDatetime"],
                    "user_name": item["dispName"],
                    "user_id": item["creatorId"],
                    "user_img": item["profileImgUrl"],
                    "text": item["commentText"],
                    "agree": item["agreePoint"],
                    "disagree": item["disagreePoint"],
                    "device": item["device"]
                })
            
            start += 50
            if start >= c["replies_count"]:
                break

    c["replies"] = replies
    return c

def get_comments(article_id):
    api_cred = get_comment_api_credentials(article_id)
    
    if not api_cred:
        return None

    page = 1
    
    comments = []
    
    while True:
        rsp = requests.get(COMMENT_API.format(keys=api_cred["keys"], url=api_cred["url"], page=page))
        soup = BeautifulSoup(rsp.text, "html.parser")
                
        new_comments = []
            
        for comment in soup.find_all("li", { "class": "commentListItem" }):
            try:
                print("\t\t", comment["id"])
                new_comments.append(extract_comment_data(comment, article_id))
            except:
                pass
        
        comments += new_comments  
        page += 1
        print(" ")
        
        if len(new_comments) == 0:
            break

    return comments


dates = [
    "20190222",
    "20190221",
    "20190220",
    "20190219",
    "20190218",
    "20190217",
    "20190216",
    "20190215",
    "20190214",
    "20190213",
]


def fetch():
    print("fetch article links ...")

    archive = ZipArchive(OUT_FILE)

    for date in dates:
        print(date)
        article_links = get_article_links(date=date)
        articles = []

        for i, link in enumerate(article_links):

            article_id = link.split("?a=")[-1].strip()

            if not archive.contains("{}.json".format(article_id)):
                print(link, "({}/{})".format(i, len(article_links)))

                print("\tfetch article data ...")
                article = get_article_data(link)
                
                if "byline" in article["link"]:
                    print("\tskip byline")
                    continue

                print("\tfetch comments ...")
                comments = get_comments(article["id"])
                article["comments"] = comments
                archive.add("{}.json".format(article["id"]), article)
                #print(comments)
        



if __name__ == "__main__":
    fetch()
    

    