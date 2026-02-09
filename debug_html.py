import requests
from bs4 import BeautifulSoup

url = "https://www.wheresthematch.com/live-sport-on-tv/"
headers = {"User-Agent": "Mozilla/5.0"}
r = requests.get(url, headers=headers)
soup = BeautifulSoup(r.text, 'html.parser')

# Find first few competition cells
comps = soup.find_all('td', class_='competition-name', limit=5)
for i, td in enumerate(comps):
    print(f"--- Item {i} ---")
    print(td.prettify())
