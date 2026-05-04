"""Fetch latest blog post from RSS and update README.md."""

import re
import sys
import urllib.request
from pathlib import Path
from urllib.error import URLError

import defusedxml.ElementTree as ET

RSS_URL = "https://mrncstt.github.io/rss.xml"
ALLOWED_DOMAIN = "https://mrncstt.github.io/"
README = Path(__file__).resolve().parent.parent / "README.md"
START = "<!-- BLOG-POST:START -->"
END = "<!-- BLOG-POST:END -->"
TIMEOUT = 30


def sanitize_title(title: str) -> str:
    return re.sub(r'[\[\]\(\)<>\n\r]', '', title).strip()


def validate_link(link: str) -> bool:
    return link.startswith(ALLOWED_DOMAIN)


def get_latest_post():
    try:
        with urllib.request.urlopen(RSS_URL, timeout=TIMEOUT) as resp:
            if not resp.url.startswith(ALLOWED_DOMAIN):
                print(f"RSS redirected to unexpected domain: {resp.url}")
                sys.exit(1)
            tree = ET.parse(resp)
    except URLError as e:
        print(f"Failed to fetch RSS: {e}")
        sys.exit(1)
    except ET.ParseError as e:
        print(f"Failed to parse RSS XML: {e}")
        sys.exit(1)

    item = tree.find(".//channel/item")
    if item is None:
        return None

    title = item.findtext("title")
    link = item.findtext("link")
    if not title or not link:
        print(f"RSS item missing title or link (title={title}, link={link})")
        sys.exit(1)

    if not validate_link(link):
        print(f"RSS link points to unexpected domain: {link}")
        sys.exit(1)

    return {"title": sanitize_title(title), "link": link}


def update_readme(post):
    text = README.read_text(encoding="utf-8")

    if START not in text or END not in text:
        print("README is missing BLOG-POST markers, skipping update.")
        sys.exit(1)

    start_idx = text.index(START) + len(START)
    end_idx = text.index(END)

    if start_idx > end_idx:
        print("BLOG-POST markers are in wrong order, skipping update.")
        sys.exit(1)

    entry = f"\n[{post['title']}]({post['link']})\n"
    updated = text[:start_idx] + entry + text[end_idx:]

    if updated != text:
        README.write_text(updated, encoding="utf-8")
        print(f"Updated: {post['title']}")
        return True

    print("No changes needed.")
    return False


if __name__ == "__main__":
    post = get_latest_post()
    if post:
        update_readme(post)
    else:
        print("No posts found in RSS feed.")
