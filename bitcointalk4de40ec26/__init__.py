
from typing import AsyncGenerator
import random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import aiohttp
import re
import time
import random
from exorde_data import (
    Item,
    Content,
    CreatedAt,
    Title,
    Url,
    Domain,
)
import logging

BASE_URL = "https://bitcointalk.org"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

async def fetch_page(session, url):
    async with session.get(url, headers=headers) as response:
        return await response.text()

async def extract_all_urls(root_url):
    async with aiohttp.ClientSession() as session:
        response_text = await fetch_page(session, root_url)
        soup = BeautifulSoup(response_text, "html.parser")
        all_links = soup.find_all("a", href=True)
        all_urls = [link['href'] for link in all_links]
        all_boards_urls = [link for link in all_urls if "/index.php?board=" in link]
        return all_boards_urls

def parse_datetime(input_string):
    """Parse a string containing a datetime substring and convert it to a timestamp UTC+0."""
    pattern_today = r"Today at (\d{1,2}):(\d{2}):(\d{2}) (AM|PM)"
    pattern_date = r"(\w+ \d{1,2}, \d{4}), (\d{1,2}):(\d{2}):(\d{2}) (AM|PM)"

    match_today = re.search(pattern_today, input_string)
    match_date = re.search(pattern_date, input_string)

    if match_today:
        hour, minute, second, am_pm = match_today.groups()
        now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        if am_pm.lower() == "pm" and hour != "12":
            hour = int(hour) + 12

        parsed_datetime = now + timedelta(hours=int(hour), minutes=int(minute), seconds=int(second))
        return int(parsed_datetime.timestamp())

    elif match_date:
        full_date, hour, minute, second, am_pm = match_date.groups()
        months_dict = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }

        date_parts = full_date.split()
        month = months_dict.get(date_parts[0], 1)
        day = int(date_parts[1].strip(','))
        year = int(date_parts[2])
        
        if am_pm.lower() == "pm" and hour != "12":
            hour = int(hour) + 12

        parsed_datetime = datetime(year, month, day, hour=int(hour), minute=int(minute), second=int(second), tzinfo=timezone.utc)
        return int(parsed_datetime.timestamp())

    return None


async def extract_topics(board_url):
    async with aiohttp.ClientSession() as session:
        response_text = await fetch_page(session, board_url)
        soup = BeautifulSoup(response_text, "html.parser")

        topic_urls = []
        topic_last_ts = []

        topics_tbody = soup.find("body")
        if not topics_tbody:
            return

        for topic_tr in topics_tbody.find_all("tr"):
            urls = []
            smalltext_content = ""
            timestamp  = None

            topic_links = topic_tr.find_all("a", href=True)
            for link in topic_links:
                url = link["href"]
                urls.append(url)

            smalltext_span = topic_tr.find("span", class_="smalltext")
            if smalltext_span:
                smalltext_content = smalltext_span.text.strip()
                try:
                    timestamp  = parse_datetime(smalltext_content)
                except Exception as e:
                    logging.exception(f"[Datetime parsing ERROR] {e}")
                    continue
                
            if timestamp is None:
                continue

            for url in urls:
                if "/index.php?topic=" in url and url[-2:]==".0": #if the link to the topic itself
                    topic_urls.append(url)
                    topic_last_ts.append(timestamp)
                    break
        return (topic_urls, topic_last_ts)


def extract_main_post_data(soup):
    windowbg = soup.find("td", class_="windowbg")

    if windowbg is None:
        return None, None, None, None

    # Extracting title text and URL
    title_div = windowbg.find("div", class_="subject")
    title_link = title_div.find("a")
    title_text = title_link.get_text() if title_link else None
    title_url = title_link['href'] if title_link else None

    # Extracting main post datetime text
    datetime_div = title_div.find_next_sibling("div", class_="smalltext")
    main_post_datetime = datetime_div.get_text() if datetime_div else None

    # Extracting post text
    post_div = windowbg.find("div", class_="post")
    post_text = post_div.get_text(strip=True) if post_div else None

    return title_text, title_url, main_post_datetime, post_text


async def extract_pages_on_topic(topic_url):
    async with aiohttp.ClientSession() as session:
        response_text = await fetch_page(session, topic_url)
        soup = BeautifulSoup(response_text, "html.parser")

        # Check if the main post date exists and extract it
        try:
            main_title_text, main_post_url, main_post_datetime, main_post_text = extract_main_post_data(soup)
        except:
            main_post_url = None
        if main_post_url is None:
            return None, None
        main_post_ts = parse_datetime(main_post_datetime)

        # Extract the number of pages
        pages_links = soup.find_all("a", class_="navPages")

        # Extract all the links from pages_links and sort them based on the last value
        page_urls = [link['href'] for link in pages_links]
        page_urls = list(set(page_urls))
        page_numbers = [int(url.split(".")[-1]) for url in page_urls]
        sorted_page_urls = [url for _, url in sorted(zip(page_numbers, page_urls), reverse=True)]

        main_post_dict = {"url": main_post_url, "timestamp": main_post_ts, "content": main_post_text, "title": main_title_text }
        return main_post_dict, sorted_page_urls


def convert_ts_to_standard_format(ts):
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    formatted_date = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    return formatted_date


async def extract_latest_posts_on_page(topic_page_url, max_oldness_seconds):
    async with aiohttp.ClientSession() as session:
        response_text = await fetch_page(session, topic_page_url)
        soup = BeautifulSoup(response_text, "html.parser")

        posts_dicts = []
        posts_divs = soup.find_all("div", class_="post")
        need_to_check_previous_page = True

        for post_div in posts_divs:
            for tag in post_div.find_all(["div", "a"]):
                tag.extract()
            post_text = post_div.get_text(strip=True)

            if not post_text.isnumeric():
                smalltext_div = post_div.find_previous(lambda tag: tag.name == "div" and "smalltext" in tag.get("class", [])).text
                if smalltext_div:
                    post_url = post_div.find_previous(lambda tag: tag.name == "a" and "message_number" in tag.get("class", []))['href']
                    try:
                        post_ts = parse_datetime(smalltext_div)
                        if post_ts is not None and is_recent_timestamp(post_ts, max_oldness_seconds):
                            logging.info(f"\n[Bitcointalk] Found new post = \n\t{post_url}\n\t{post_ts}\n\t{post_text}")
                            posts_dicts.append({"url": post_url, "timestamp": post_ts, "content": post_text })
                        else:                        
                            # logging.info(f"[Bitcointalk] Old post = \n\t{post_url}\n\t{post_ts}")
                            need_to_check_previous_page = False
                    except Exception as e:
                        logging.info(f"[Bitcoin talk] Error parsing text & date : {e}")

        return posts_dicts, need_to_check_previous_page


def is_recent_timestamp(timestamp, max_oldness_seconds):
    current_time = time.time()
    return current_time - timestamp <= max_oldness_seconds


async def scrape_bitcointalk_latest_posts(max_oldnesss_seconds, nb_selections)  -> AsyncGenerator[Item, None]:
    root_url = "https://bitcointalk.org/"
    boards_urls = await extract_all_urls(root_url)
    nb_selections_ = min(len(boards_urls), nb_selections)
    selected_general_boards = random.sample(boards_urls, k=nb_selections_)
    selected_main_boards = random.sample(['https://bitcointalk.org/index.php?board=1.0', 
                                              'https://bitcointalk.org/index.php?board=6.0',
                                              'https://bitcointalk.org/index.php?board=4.0', 
                                              'https://bitcointalk.org/index.php?board=12.0',
                                              'https://bitcointalk.org/index.php?board=7.0',
                                              'https://bitcointalk.org/index.php?board=8.0',
                                              'https://bitcointalk.org/index.php?board=67.0',
                                              'https://bitcointalk.org/index.php?board=161.0',
                                              'https://bitcointalk.org/index.php?board=224.0'], k=nb_selections)
    selected_boards = selected_main_boards + selected_general_boards
    selected_boards = [url+";sort=last_post;desc" for url in selected_boards]
    logging.info(f"[Bitcointalk] Found {len(boards_urls)} boards in total. Selected {len(selected_boards)} boards for analysis.")
    for board_url in selected_boards:
        logging.info(f"[Bitcointalk] Selected Board URL: {board_url}")
        (topic_urls, topic_last_ts) = await extract_topics(board_url)
        selectable_topics = []
        for topic_url, topic_ts in zip(topic_urls, topic_last_ts):
            if is_recent_timestamp(topic_ts, max_oldnesss_seconds):
                selectable_topics.append(topic_url)
        if len(selectable_topics) == 0:
            logging.info("continuing, no recent topic found")
            continue
        nb_selections_ = min(len(selectable_topics), nb_selections)
        selected_topics = random.sample(selectable_topics, k=nb_selections_)
        for topic_url in selected_topics:
            logging.info(f"[Bitcointalk] Parsing topic page: {topic_url}")
            main_post_dict, sorted_page_urls = await extract_pages_on_topic(topic_url)
            if main_post_dict is None:
                continue
            main_post_dict['created_at'] = convert_ts_to_standard_format(main_post_dict["timestamp"])
            if  is_recent_timestamp(main_post_dict["timestamp"], max_oldnesss_seconds):
                yield Item(
                    title=Title(str(main_post_dict["title"])),
                    content=Content(str(main_post_dict["content"])),
                    created_at=CreatedAt(str(main_post_dict["created_at"])),
                    url=Url(str(main_post_dict["url"])),
                    domain=Domain("bitcointalk.org")
                )
            sorted_page_urls.insert(0, topic_url)
            is_there_many_pages = len(sorted_page_urls) > 0
            logging.info(f"[Bitcointalk] Identified {len(sorted_page_urls)} pages in topic, checking latest posts..; ")
            if is_there_many_pages:
                # let's iterate reverse from latest comment to initial post                
                for i in range(len(sorted_page_urls)):
                    new_posts_found_dicts, need_to_check_previous_page = await extract_latest_posts_on_page(sorted_page_urls[i], max_oldnesss_seconds)
                    if new_posts_found_dicts is not None and len(new_posts_found_dicts) > 0:
                        for post in new_posts_found_dicts:
                            post_dict = {"title":main_post_dict['title'], "url":post["url"], "created_at":convert_ts_to_standard_format(post["timestamp"]), "content":post["content"]  }
                            yield Item(
                                title=Title(str(post_dict["title"])),
                                content=Content(str(post_dict["content"])),
                                created_at=CreatedAt(str(post_dict["created_at"])),
                                url=Url(str(post_dict["url"])),
                                domain=Domain("bitcointalk.org")
                            )
                    if need_to_check_previous_page is not True:
                        break

# default values
DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_NB_SELECTIONS = 3

def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

        try:
            nb_selections = parameters.get("nb_selections", DEFAULT_NB_SELECTIONS)
        except KeyError:
            nb_selections = DEFAULT_NB_SELECTIONS

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        nb_selections = DEFAULT_NB_SELECTIONS

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, nb_selections


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length, nb_selections = read_parameters(parameters)
    logging.info(f"[BitcoinTalk] - Scraping posts & comments newer than {max_oldness_seconds} seconds.")
    async for item in scrape_bitcointalk_latest_posts(max_oldness_seconds, nb_selections):
        yielded_items += 1
        yield item
        if yielded_items >= maximum_items_to_collect:
            return