import json
import requests
import datetime
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
FALLBACK_ICON = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
BASE_URL = "https://ninoiptv.com/"
MAX_PROFILES = 100

# Validation settings
MIN_PLAYLIST_SIZE = 100
PLAYLIST_TIMEOUT = 12
DAYS_TO_SCRAPE = 7
MAX_ARTICLES = 10

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive"
}

# Game of Thrones themed slot names with custom logo URLs
# Format: {"name": "Slot Name", "logo": "URL or None"}
GOT_SLOTS = [
    {"name": "Westeros", "logo": "https://static.digitecgalaxus.ch/im/Files/2/1/1/3/9/7/9/6/game_of_thrones_intro_map_westeros_elastic21.jpeg?impolicy=teaser&resizeWidth=1000&resizeHeight=500"},
    {"name": "Essos", "logo": "https://imgix.bustle.com/uploads/image/2017/7/12/4e391a2f-8663-4cdd-91eb-9102c5f731d7-52be1751932bb099d5d5650593df5807b50fc3fbbee7da6a556bd5d1d339f39a.jpg?w=800&h=532&fit=crop&crop=faces"},
    {"name": "Beyond the Wall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/27/706_Tormund_Beric_Sandor_Jon_Jorah_Gendry.jpg/revision/latest/scale-to-width-down/1000?cb=20170821092659"},
    {"name": "Winterfell", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/08/1x01_Winterfell.jpg/revision/latest?cb=20170813191451"},
    {"name": "King's Landing", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/83/King%27s_Landing_HotD.png/revision/latest/scale-to-width-down/1000?cb=20220805155800"},
    {"name": "The Wall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/The_Wall.jpg/revision/latest/scale-to-width-down/1000?cb=20150323200738"},
    {"name": "Castle Black", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7b/Castle_Black.jpg/revision/latest/scale-to-width-down/1000?cb=20110920111941"},
    {"name": "Dragonstone", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a4/Dragonstone-season7-low.png/revision/latest/scale-to-width-down/1000?cb=20170717082952"},
    {"name": "Highgarden", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/704_Highgarden.png/revision/latest?cb=20170807030944"},
    {"name": "Casterly Rock", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a8/Casterly-rock.png/revision/latest/scale-to-width-down/1000?cb=20170731025431"},
    {"name": "The Eyrie", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/59/The_Eyrie.jpg/revision/latest?cb=20110615190250"},
    {"name": "Dorne", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/Dorne.png/revision/latest/scale-to-width-down/1000?cb=20120719190909"},
    {"name": "Braavos", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7b/Titan_of_Braavos.jpg/revision/latest/scale-to-width-down/1000?cb=20150504024857"},
    {"name": "Meereen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/89/Meereen.png/revision/latest/scale-to-width-down/1000?cb=20150328211743"},
    {"name": "Valyria", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/19/Valyria_5x05_%284%29.png/revision/latest/scale-to-width-down/1000?cb=20150511133123"},
    {"name": "White Harbor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1a/WhiteHarbor.jpg/revision/latest/scale-to-width-down/1000?cb=20140218212230"},
    {"name": "Red Keep", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/71/S8_Red_Keep.jpg/revision/latest?cb=20190305171013"},
    {"name": "Riverrun", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/66/Riverrun._battlements.png/revision/latest/scale-to-width-down/1000?cb=20160606102912"},
    {"name": "Harrenhal", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9b/Harrenhal.jpg/revision/latest/scale-to-width-down/1000?cb=20150328214605"},
    {"name": "Storm's End", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/25/Storm%27s_End_Official_Guide.jpg/revision/latest?cb=20221024024717"},
    {"name": "Oldtown", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/01/BtD_AIH_Oldtown.png/revision/latest/scale-to-width-down/1000?cb=20220805155230"},
    {"name": "The Citadel", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4e/S06E10_-_The_Citadel.png/revision/latest?cb=20160627153650"},
    {"name": "The Iron Islands", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/eb/Iron_Islands.png/revision/latest?cb=20120719194710"},
    {"name": "Pyke", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9d/Pyke.jpg/revision/latest?cb=20120402161004"},
    {"name": "The Reach", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/89/The_Reach.png/revision/latest/scale-to-width-down/1000?cb=20120719200419"},
    {"name": "The North", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2b/Distant_Winterfell.jpg/revision/latest/scale-to-width-down/1000?cb=20220201081605"},
    {"name": "The Iron Throne", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c8/Iron_throne.jpg/revision/latest/scale-to-width-down/1000?cb=20131005175755"},
    {"name": "The Red Woman", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0d/The_night_is_dark_and_full_of_terrors.jpg/revision/latest/scale-to-width-down/1000?cb=20160802075640"},
    {"name": "The Great Sept of Baelor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d9/GreatSeptS3.jpg/revision/latest/scale-to-width-down/1000?cb=20150412170141"},
    {"name": "Night's Watch", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/25/Jon%2C_Sam_and_Pyp.jpg/revision/latest/scale-to-width-down/1000?cb=20101128160538"},
    {"name": "Kingsguard", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9e/Targ_Kingsguard.png/revision/latest?cb=20160419222717"},
    {"name": "The Free Cities", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/38/Longbridge.png/revision/latest/scale-to-width-down/1000?cb=20150328214200"},
    {"name": "The Narrow Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/6c/S04E10_-_Arya_%28on_the_ship%29.png/revision/latest/scale-to-width-down/1000?cb=20140618063538"},
    {"name": "Caraxes", "logo": "https://staticg.sportskeeda.com/editor/2022/12/03519-16722910130144-1920.jpg"},
    {"name": "Bear Island", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/16/Bear-island-02.jpg/revision/latest/scale-to-width-down/1000?cb=20160606033558"},
    {"name": "Sunspear", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b6/Sunspear-0.png/revision/latest?cb=20170918150538"},
    {"name": "Astapor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/ca/Astapor_city_view.jpg/revision/latest/scale-to-width-down/1000?cb=20150328211103"},
    {"name": "Balerion The Black Dread", "logo": "https://staticg.sportskeeda.com/editor/2022/12/62cdf-16722910130058-1920.jpg"},
    {"name": "Qarth", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/60/Qarth2.jpg/revision/latest/scale-to-width-down/1000?cb=20140422045311"},
    {"name": "Daenerys", "logo": "https://www.thepopverse.com/_next/image?url=https%3A%2F%2Fmedia.thepopverse.com%2Fmedia%2Femilia-clarke-game-of-thrones-dothraki-language-i2yd4ljq4zpfvikmv0onmgmvy2.png&w=1280&q=75"},
    {"name": "Slaver's Bay", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c5/Slaver%27s_Bay.png/revision/latest/scale-to-width-down/1000?cb=20120719201919"},
    {"name": "The Dothraki Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/67/S6_Horse_Statues.png/revision/latest/scale-to-width-down/1000?cb=20170819032739"},
    {"name": "The Trident", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/6d/Riverlands_map.jpg/revision/latest/scale-to-width-down/1000?cb=20110314205331"},
    {"name": "Godswood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ea/Godtree_S8_EP2.jpg/revision/latest/scale-to-width-down/1000?cb=20190417202906"},
    {"name": "Heart Tree", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/de/Old_Gods_infobox.jpg/revision/latest?cb=20130130103158"},
    {"name": "Weirwood Trees", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/cf/Winterfell_Godswood.jpg/revision/latest/scale-to-width-down/1000?cb=20110302144610"},
    {"name": "The Old Gods", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3d/Black_and_White_Weirwood_face.jpg/revision/latest?cb=20160716022634"},
    {"name": "The Seven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7e/501_Great_Sept_Seven_statues.jpg/revision/latest/scale-to-width-down/1000?cb=20150416164510"},
    {"name": "The Lord of Light", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0e/Black_and_White_Lord_of_Light.jpg/revision/latest/scale-to-width-down/1000?cb=20150608200849"},
    {"name": "White Walkers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5a/WhiteWalkersandArmy.png/revision/latest/scale-to-width-down/1000?cb=20211009053915"},
    {"name": "The Long Night", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/75/Grey_Worm_S8_Ep3.jpg/revision/latest/scale-to-width-down/1000?cb=20190429030211"},
    {"name": "Dragonglass", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/85/Dragonglass_Spear.jpg/revision/latest/scale-to-width-down/1000?cb=20150407175621"},
    {"name": "Valyrian Steel", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/08/Viserys_Dagger_3.png/revision/latest?cb=20221011000312"},
    {"name": "Wildlings", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/32/Ygritte.jpg/revision/latest?cb=20120524071810"},
    {"name": "Syrax", "logo": "https://staticg.sportskeeda.com/editor/2022/12/934cb-16722910129818-1920.jpg"},
    {"name": "Children of the Forest", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3b/Childrenoftheforest.jpg/revision/latest/scale-to-width-down/1000?cb=20160517184932"},
    {"name": "Faceless Men", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b0/Jaqen.png/revision/latest?cb=20160826000828"},
    {"name": "The Unsullied", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/51/UNSULLIEDS8.png/revision/latest/scale-to-width-down/1000?cb=20190517173434"},
    {"name": "The Dothraki", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e2/Dothraki_archery.jpg/revision/latest?cb=20180106203557"},
    {"name": "The Mountain", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/GregorGregorLightenedProfile.png/revision/latest?cb=20190724144505"},
    {"name": "Drogon", "logo": "https://staticg.sportskeeda.com/editor/2022/12/9eb54-16722910132128-1920.jpg"},
    {"name": "Rhaegal", "logo": "https://staticg.sportskeeda.com/editor/2022/12/ef298-16722910129506-1920.jpg"},
    {"name": "Viserion", "logo": "https://staticg.sportskeeda.com/editor/2022/12/2d488-16722910129839-1920.jpg"},
    {"name": "Ghost", "logo": "https://i.cbc.ca/ais/1.3078255,1431974097000/full/max/0/default.jpg?im=Crop%2Crect%3D%280%2C91%2C400%2C225%29%3BResize%3D860"},
    {"name": "Nymeria", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d3/Nymeria_bites_Joffrey.png/revision/latest/scale-to-width-down/1000?cb=20150404115158"},
    {"name": "The Three-Eyed Raven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3a/Three-eyed_raven.png/revision/latest?cb=20110622101243"},
    {"name": "The Golden Company", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f4/Golden_Company_S8.jpg/revision/latest/scale-to-width-down/1000?cb=20190408221754"},
    {"name": "The Red Wedding", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/Robb_Wind_MHYSA_new_lightened.jpg/revision/latest/scale-to-width-down/1000?cb=20160830004546"},
    {"name": "The Purple Wedding", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4d/Purple_Wedding.png/revision/latest/scale-to-width-down/1000?cb=20150210223603"},
    {"name": "Blackwater Bay", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/41/Wildfire_explosion.jpg/revision/latest/scale-to-width-down/1000?cb=20150328212702"},
    {"name": "Hardhome", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b0/Hardhome_%28episode%29_.jpg/revision/latest/scale-to-width-down/1000?cb=20150601113829"},
    {"name": "The Moon Door", "logo": "https://static.independent.co.uk/s3fs-public/thumbnails/image/2016/04/29/14/gameofthrones-moon-door.jpg?quality=75&width=1368&crop=3%3A2%2Csmart&auto=webp"},
    {"name": "The Faith Militant", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f6/Sparrows.png/revision/latest/scale-to-width-down/1000?cb=20150323105505"},
    {"name": "The High Sparrow", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3b/Blood_of_My_Blood_16.jpg/revision/latest/scale-to-width-down/1000?cb=20160527164016"},
    {"name": "A Song of Ice and Fire", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0d/KOTNS_Targaryen_dagger.png/revision/latest/scale-to-width-down/1000?cb=20220905120253"},
    {"name": "House Targaryen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1e/House_Targaryen.svg/revision/latest?cb=20230905234715"},
    {"name": "House Stark", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7e/House_Stark.svg/revision/latest?cb=20230905233833"},
    {"name": "House Lannister", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d5/House_Lannister.svg/revision/latest?cb=20230905230248"},
    {"name": "The Crownlands", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b4/S2_Crownlands_Forest.jpg/revision/latest/scale-to-width-down/1000?cb=20240323063113"},
    {"name": "The Riverlands", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d0/The_Riverlands.png/revision/latest?cb=20120719200633"},
    {"name": "The Vale of Arryn", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d7/Vale_of_Arryn.png/revision/latest?cb=20240330004915"},
    {"name": "ADELE", "logo": "https://charts-static.billboard.com/img/2008/02/adele-9x5-344x344.jpg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Tinga", "logo": "https://images.theconversation.com/files/698357/original/file-20251024-66-5tljkb.jpg?ixlib=rb-4.1.0&rect=0%2C340%2C4096%2C2048&q=45&auto=format&w=668&h=334&fit=crop"},
    {"name": "DRACARYS", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7f/Dragons_destroy_ships_in_Meereen.jpg/revision/latest/scale-to-width-down/1000?cb=20160621014157"},
    {"name": "BATTLE OF THE BASTARDS", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/df/Knights_of_the_Vale_S6E09_5.PNG/revision/latest/scale-to-width-down/1000?cb=20160826132900"},
    {"name": "The Hound", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b5/SandorConfrontsGregor.PNG/revision/latest?cb=20210722093812"},
    {"name": "tHeOn (sorry) REEK", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/84/Theon-fealty.png/revision/latest?cb=20160812055322"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"},
    {"name": "Janabi", "logo": "https://www.insideke.online/wp-content/uploads/2025/11/IMG_0713.jpeg"}
]

def log_to_file(message):
    """Saves a summary to the permanent log.txt file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {message}\n")

def parse_m3u_streams(content):
    """Extract stream URLs from M3U playlist content."""
    streams = []
    lines = content.split('\n')
    
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#'):
            if line.startswith('http://') or line.startswith('https://'):
                streams.append(line)
    
    return streams

def validate_playlist(url):
    """
    Validates playlist and returns detailed info.
    Returns (is_valid, reason, stream_count)
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=PLAYLIST_TIMEOUT, allow_redirects=True)
        
        if response.status_code >= 400:
            return False, f"HTTP {response.status_code}", 0
        
        content_length = len(response.content)
        if content_length < MIN_PLAYLIST_SIZE:
            return False, "empty/invalid", 0
        
        content = response.text
        
        has_m3u_header = '#EXTM3U' in content
        has_extinf = '#EXTINF' in content
        
        if not (has_m3u_header or has_extinf):
            return False, "not M3U format", 0
        
        streams = parse_m3u_streams(content)
        stream_count = len(streams)
        
        if stream_count == 0:
            return False, "no streams", 0
        
        return True, f"valid ({stream_count} streams)", stream_count
        
    except requests.exceptions.Timeout:
        return False, "timeout", 0
    except Exception as e:
        return False, "error", 0

def test_logo_url(logo_url):
    """Test if logo URL is reachable. Returns True if accessible."""
    if not logo_url:
        return False
    try:
        response = requests.head(logo_url, headers=HEADERS, timeout=5, allow_redirects=True)
        return response.status_code < 400
    except:
        return False

def get_logo_for_slot(slot_id):
    """Get logo URL for slot, with fallback to default icon."""
    slot_data = GOT_SLOTS[slot_id]
    logo_url = slot_data.get("logo")
    
    # If no custom logo or unreachable, use fallback
    if not logo_url or not test_logo_url(logo_url):
        return FALLBACK_ICON
    
    return logo_url

def get_domain(url):
    """Extract clean domain name from URL."""
    domain = urlparse(url).netloc
    domain = domain.replace('www.', '')
    return domain

def extract_date_from_title(title):
    """Extract date from article titles."""
    date_pattern = r'(\d{2})-(\d{2})-(\d{4})'
    match = re.search(date_pattern, title)
    
    if match:
        day, month, year = match.groups()
        try:
            return datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            return None
    return None

def scrape_nino():
    """Enhanced scraper for recent articles."""
    print(f"\n--- STEP 1: ACCESSING {BASE_URL} ---")
    all_links = []
    
    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        now = datetime.datetime.now()
        cutoff_date = now - datetime.timedelta(days=DAYS_TO_SCRAPE)
        
        print(f"Looking for articles from {cutoff_date.strftime('%d-%m-%Y')} to {now.strftime('%d-%m-%Y')}")
        
        articles = soup.find_all('h2', class_='entry-title')
        recent_articles = []
        
        for article_tag in articles[:MAX_ARTICLES]:
            title_link = article_tag.find('a')
            if not title_link:
                continue
                
            title = title_link.get_text(strip=True)
            url = title_link['href']
            
            article_date = extract_date_from_title(title)
            
            if article_date and article_date >= cutoff_date:
                recent_articles.append({
                    'title': title,
                    'url': url,
                    'date': article_date
                })
                print(f"  ✓ Found: {title}")
        
        if not recent_articles:
            print("⚠ No recent articles found within date range")
            return []
        
        print(f"\nFound {len(recent_articles)} recent articles to scrape")
        
        for idx, article in enumerate(recent_articles, 1):
            print(f"\n--- Scraping Article {idx}/{len(recent_articles)} ---")
            print(f"Title: {article['title'][:60]}...")
            
            try:
                r_art = requests.get(article['url'], headers=HEADERS, timeout=15)
                soup_art = BeautifulSoup(r_art.text, 'html.parser')
                
                article_links = []
                for a in soup_art.find_all('a', href=True):
                    href = a['href']
                    if "get.php?username=" in href:
                        article_links.append(href)
                
                unique_article_links = list(dict.fromkeys(article_links))
                all_links.extend(unique_article_links)
                
                print(f"  → Extracted {len(unique_article_links)} playlist links")
                
            except Exception as e:
                print(f"  ✗ Error scraping article: {e}")
                continue
        
        unique_all_links = list(dict.fromkeys(all_links))
        
        print(f"\n{'─' * 50}")
        print(f"Total unique playlist links scraped: {len(unique_all_links)}")
        print(f"{'─' * 50}")
        
        return unique_all_links

    except Exception as e:
        print(f"CRITICAL ERROR during scrape: {e}")
        return []

def main():
    print("═══════════════════════════════════════════════════")
    print("   O.R CONTENT MANAGER - GAME OF THRONES EDITION")
    print("═══════════════════════════════════════════════════\n")
    
    # Load existing data
    try:
        with open(JSON_FILE, 'r') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}
    
    existing_slots = data.get("featured_content", [])
    
    # Create slot mapping (preserve existing slot assignments)
    slot_registry = {}  # slot_id -> slot_data
    
    # Load existing slot assignments
    for item in existing_slots:
        slot_id = item.get("slot_id")
        if slot_id is not None and slot_id < len(GOT_SLOTS):
            slot_registry[slot_id] = item
    
    print(f"\n--- STEP 2: VALIDATING EXISTING {len(existing_slots)} SLOTS ---")
    
    timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    validated_domains = set()
    
    # Validate existing slots
    for slot_id, item in list(slot_registry.items()):
        url = item.get("url")
        old_domain = item.get("domain", get_domain(url))
        got_name = GOT_SLOTS[slot_id]["name"]
        
        print(f"\n[Slot {slot_id + 1:03d}] {got_name}")
        print(f"  Current: {old_domain}")
        print(f"  → Testing playlist...")
        
        is_valid, reason, stream_count = validate_playlist(url)
        
        if is_valid:
            # Update slot with current info
            item["slot_id"] = slot_id
            item["name"] = got_name
            item["channel_count"] = stream_count
            item["domain"] = old_domain
            item["logo_url"] = get_logo_for_slot(slot_id)
            item["last_validated"] = timestamp_now
            validated_domains.add(old_domain)
            print(f"  ✓ ALIVE ({stream_count} channels)")
        else:
            print(f"  ✗ DEAD ({reason}) - slot will be reassigned")
            del slot_registry[slot_id]
    
    # Find empty slots
    empty_slots = [i for i in range(MAX_PROFILES) if i not in slot_registry]
    
    print(f"\n{'─' * 50}")
    print(f"Active slots: {len(slot_registry)}")
    print(f"Empty slots: {len(empty_slots)}")
    print(f"{'─' * 50}")
    
    # Scrape for new content to fill empty slots
    if empty_slots:
        print(f"\n--- STEP 3: FILLING {len(empty_slots)} EMPTY SLOTS ---")
        new_potential_links = scrape_nino()
        
        newly_validated_domains = set()
        
        for idx, link in enumerate(new_potential_links, 1):
            if not empty_slots:
                print("\n✓ All slots filled!")
                break
            
            domain = get_domain(link)
            
            # Skip if we already have this domain
            if domain in validated_domains or domain in newly_validated_domains:
                print(f"\n[Link {idx}/{len(new_potential_links)}] Skipping {domain[:40]} (already in use)")
                continue
            
            print(f"\n[Link {idx}/{len(new_potential_links)}] Testing {domain[:40]}...")
            is_valid, reason, stream_count = validate_playlist(link)
            
            if is_valid:
                # Assign to next available slot
                slot_id = empty_slots.pop(0)
                got_name = GOT_SLOTS[slot_id]["name"]
                
                # Check if this slot had a previous assignment
                old_slot_data = existing_slots[slot_id] if slot_id < len(existing_slots) else None
                old_domain = old_slot_data.get("domain", "None") if old_slot_data else "None"
                
                print(f"  ✓ ASSIGNED to Slot {slot_id + 1:03d} ({got_name})")
                print(f"    {stream_count} channels from {domain}")
                
                # Create change log entry
                change_log = f"Changed from {old_domain} to {domain}" if old_domain != "None" else f"Initial assignment: {domain}"
                
                slot_registry[slot_id] = {
                    "slot_id": slot_id,
                    "name": got_name,
                    "domain": domain,
                    "channel_count": stream_count,
                    "logo_url": get_logo_for_slot(slot_id),
                    "type": "m3u",
                    "url": link,
                    "server_url": None,
                    "id": f"got_slot_{str(slot_id + 1).zfill(2)}",
                    "last_changed": timestamp_now,
                    "last_validated": timestamp_now,
                    "change_log": change_log
                }
                
                validated_domains.add(domain)
                newly_validated_domains.add(domain)
            else:
                print(f"  ✗ REJECTED: {reason}")
    
    # Build final sorted list
    final_list = []
    for slot_id in range(MAX_PROFILES):
        if slot_id in slot_registry:
            final_list.append(slot_registry[slot_id])
    
    # Save
    data["featured_content"] = final_list
    with open(JSON_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Summary
    total_channels = sum(item.get("channel_count", 0) for item in final_list)
    summary_msg = f"Sync Complete. Active Slots: {len(final_list)}/{MAX_PROFILES} | Total Channels: {total_channels}"
    log_to_file(summary_msg)
    
    print(f"\n{'═' * 50}")
    print(f"✓ FINISHED: {summary_msg}")
    print(f"{'═' * 50}\n")

if __name__ == "__main__":
    main()
