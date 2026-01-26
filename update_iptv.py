import json
import requests
import datetime
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "log.txt"
FALLBACK_ICON = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"
NINO_URL = "https://ninoiptv.com/"
IPTVCODES_URL = "https://www.iptvcodes.online/"
M3UMAX_URL = "https://m3umax.blogspot.com/"

# Validation settings
MIN_PLAYLIST_SIZE = 500
MIN_STREAM_COUNT = 10
PLAYLIST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_DELAY = 2
MAX_ARTICLES_TO_CHECK = 1

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

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
    {"name": "theon (sorry) REEK", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/84/Theon-fealty.png/revision/latest?cb=20160812055322"},
    {"name": "Levels, Jerry, Levels", "logo": "https://pbs.twimg.com/media/B_rFi9ZWYAEowmo?format=png&name=small"},
    {"name": "ART VANDALEY", "logo": "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/orstreams11.webp"},
    {"name": "VANDALEY INDUSTRIES", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/9c/The_Seinfeld_Chronicles.jpg/revision/latest?cb=20080801224556"},
    {"name": "SAGMAN", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/97/The_Stake_Out_00010.jpg/revision/latest?cb=20111216223006"},
    {"name": "BENNETT", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/97/The_Stake_Out_00010.jpg/revision/latest?cb=20111216223006"},
    {"name": "ROBBINS", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/97/The_Stake_Out_00010.jpg/revision/latest?cb=20111216223006"},
    {"name": "OPPENHEIM", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/97/The_Stake_Out_00010.jpg/revision/latest?cb=20111216223006"},
    {"name": "TAFT", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/97/The_Stake_Out_00010.jpg/revision/latest?cb=20111216223006"},
    {"name": "Del Boca Vista", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/9d/The_puerto_rican_day.jpg/revision/latest?cb=20120917024357"},
    {"name": "THE DREAM CAFE", "logo": "https://static.wikia.nocookie.net/seinfeld/images/9/94/DreamCafe.jpg/revision/latest?cb=20120312180840"},
    {"name": "THE PIGMAN", "logo": "https://i0.wp.com/the-avocado.org/wp-content/uploads/2024/12/daa-1.jpg?w=1000&ssl=1"}
]


def create_session():
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def log_to_file(message):
    """Saves a summary to the permanent log.txt file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")


def parse_m3u_streams(content):
    """Extract stream URLs from M3U playlist content with detailed counting."""
    streams = []
    lines = content.split('\n')
    
    current_stream = None
    for line in lines:
        line = line.strip()
        
        # Track EXTINF lines (channel definitions)
        if line.startswith('#EXTINF'):
            current_stream = {'info': line}
        # Track actual stream URLs
        elif line and not line.startswith('#'):
            if line.startswith('http://') or line.startswith('https://'):
                if current_stream:
                    current_stream['url'] = line
                    streams.append(current_stream)
                    current_stream = None
                else:
                    # Stream without EXTINF
                    streams.append({'url': line})
    
    return streams


def validate_playlist(url, session, verbose=True):
    """
    Validates playlist thoroughly with proper error handling.
    Returns: (is_valid, status_message, stream_count)
    """
    for attempt in range(MAX_RETRIES):
        try:
            if verbose and attempt > 0:
                print(f"    Retry {attempt + 1}/{MAX_RETRIES}...")
            
            response = session.get(
                url, 
                headers=HEADERS, 
                timeout=PLAYLIST_TIMEOUT, 
                allow_redirects=True,
                stream=True
            )
            
            # Check HTTP status
            if response.status_code == 401:
                return False, "Authentication Required (Invalid Credentials)", 0
            elif response.status_code == 403:
                return False, "Access Forbidden (Blocked/Expired)", 0
            elif response.status_code == 404:
                return False, "Not Found (Dead Link)", 0
            elif response.status_code >= 500:
                return False, f"Server Error (HTTP {response.status_code})", 0
            elif response.status_code >= 400:
                return False, f"Client Error (HTTP {response.status_code})", 0
            
            # Read content
            try:
                content = response.text
            except Exception as e:
                return False, f"Content Read Error: {str(e)[:30]}", 0
            
            # Check minimum size
            content_length = len(content)
            if content_length < MIN_PLAYLIST_SIZE:
                return False, f"Too Small ({content_length} bytes)", 0
            
            # Validate M3U format
            has_m3u_header = '#EXTM3U' in content
            has_extinf = '#EXTINF' in content
            
            if not has_m3u_header and not has_extinf:
                return False, "Invalid Format (Not M3U)", 0
            
            # Parse streams
            streams = parse_m3u_streams(content)
            stream_count = len(streams)
            
            if stream_count < MIN_STREAM_COUNT:
                return False, f"Too Few Channels ({stream_count} found)", 0
            
            # Additional validation: check if streams have valid URLs
            valid_streams = [s for s in streams if 'url' in s and s['url'].startswith('http')]
            if len(valid_streams) < MIN_STREAM_COUNT:
                return False, f"Invalid Stream URLs ({len(valid_streams)} valid)", 0
            
            return True, f"✓ VALID", stream_count
            
        except requests.exceptions.Timeout:
            if attempt == MAX_RETRIES - 1:
                return False, "Timeout (Server Too Slow/Unresponsive)", 0
            time.sleep(RETRY_DELAY)
            continue
            
        except requests.exceptions.ConnectionError as e:
            error_msg = str(e).lower()
            if 'name or service not known' in error_msg or 'nodename nor servname' in error_msg:
                return False, "DNS Error (Domain Doesn't Exist)", 0
            elif 'connection refused' in error_msg:
                return False, "Connection Refused (Server Down)", 0
            elif 'incomplete read' in error_msg:
                return False, "Incomplete Data (Connection Dropped)", 0
            else:
                if attempt == MAX_RETRIES - 1:
                    return False, f"Connection Failed: {error_msg[:40]}", 0
                time.sleep(RETRY_DELAY)
                continue
                
        except requests.exceptions.TooManyRedirects:
            return False, "Too Many Redirects (Broken Link)", 0
            
        except requests.exceptions.SSLError:
            return False, "SSL Certificate Error", 0
            
        except Exception as e:
            error_msg = str(e)
            if attempt == MAX_RETRIES - 1:
                return False, f"Unknown Error: {error_msg[:40]}", 0
            time.sleep(RETRY_DELAY)
            continue
    
    return False, "Max Retries Exceeded", 0


def get_logo_for_slot(slot_id):
    """Get logo URL for slot, with fallback."""
    if slot_id < len(GOT_SLOTS):
        logo_url = GOT_SLOTS[slot_id].get("logo")
        if logo_url:
            return logo_url
    return FALLBACK_ICON


def get_name_for_slot(slot_id, domain):
    """Get name for a slot."""
    if slot_id < len(GOT_SLOTS):
        return GOT_SLOTS[slot_id]["name"]
    return f"IPTV Stream #{slot_id + 1}"


def get_domain(url):
    """Extract clean domain name from URL."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        domain = domain.replace('www.', '')
        return domain
    except:
        return url


def scrape_nino():
    """Scraper for Nino IPTV - only most recent post."""
    print(f"\n{'='*60}")
    print(f"  SCRAPING: NINO IPTV ({NINO_URL})")
    print(f"{'='*60}")
    all_links = []
    
    try:
        r = requests.get(NINO_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        articles = soup.find_all('h2', class_='entry-title')
        if not articles:
            print("  ✗ No articles found")
            return []
            
        article_tag = articles[0]
        title_link = article_tag.find('a')
        
        if title_link:
            title = title_link.get_text(strip=True)
            url = title_link['href']
            print(f"  → Article: {title[:70]}...")
            
            try:
                r_art = requests.get(url, headers=HEADERS, timeout=15)
                soup_art = BeautifulSoup(r_art.text, 'html.parser')
                
                article_links = []
                for a in soup_art.find_all('a', href=True):
                    href = a['href']
                    if "get.php?username=" in href:
                        article_links.append(href)
                
                unique = list(set(article_links))
                print(f"  ✓ Extracted {len(unique)} playlist links")
                all_links.extend(unique)
                
            except Exception as e:
                print(f"  ✗ Article read error: {str(e)[:50]}")
    except Exception as e:
        print(f"  ✗ Scraping failed: {str(e)[:50]}")
        
    return all_links


def scrape_iptvcodes():
    """Scraper for iptvcodes.online - handles both direct URLs and Xtream codes."""
    print(f"\n{'='*60}")
    print(f"  SCRAPING: IPTV CODES ({IPTVCODES_URL})")
    print(f"{'='*60}")
    all_links = []
    
    try:
        r = requests.get(IPTVCODES_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        articles = soup.find_all('h2', class_='post-title entry-title')
        if not articles:
            print("  ✗ No articles found")
            return []
            
        article_tag = articles[0]
        title_link = article_tag.find('a')
        
        if title_link:
            title = title_link.get_text(strip=True)
            url = title_link['href']
            print(f"  → Article: {title[:70]}...")
            
            try:
                r_art = requests.get(url, headers=HEADERS, timeout=15)
                soup_art = BeautifulSoup(r_art.text, 'html.parser')
                
                # Strategy 1: Find already-complete M3U URLs
                for a in soup_art.find_all('a', href=True):
                    href = a['href']
                    if "get.php?" in href and "username=" in href and "password=" in href:
                        all_links.append(href)
                
                # Strategy 2: Parse URL/User/Pass patterns and construct URLs
                text_content = soup_art.get_text()
                
                # Find all Xtream Code blocks
                xtream_pattern = r'URL\s*[➤>:]+\s*(https?://[^\s<]+)\s+.*?User\s*[➤>:]+\s*([^\s<]+)\s+.*?Pass\s*[➤>:]+\s*([^\s<]+)'
                matches = re.finditer(xtream_pattern, text_content, re.DOTALL | re.IGNORECASE)
                
                for match in matches:
                    base_url = match.group(1).strip().rstrip('/')
                    username = match.group(2).strip()
                    password = match.group(3).strip()
                    
                    # Construct the M3U URL
                    constructed_url = f"{base_url}/get.php?username={username}&password={password}&type=m3u_plus"
                    all_links.append(constructed_url)
                
                unique = list(set(all_links))
                print(f"  ✓ Extracted {len(unique)} playlist links")
                
            except Exception as e:
                print(f"  ✗ Article read error: {str(e)[:50]}")
                
    except Exception as e:
        print(f"  ✗ Scraping failed: {str(e)[:50]}")
        
    return all_links


def scrape_m3umax():
    """Scraper for m3umax.blogspot.com - only most recent post."""
    print(f"\n{'='*60}")
    print(f"  SCRAPING: M3UMAX ({M3UMAX_URL})")
    print(f"{'='*60}")
    all_links = []
    
    try:
        r = requests.get(M3UMAX_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find first article
        articles = soup.find_all('h3', class_='pTtl')
        if not articles:
            print("  ✗ No articles found")
            return []
            
        article_tag = articles[0]
        title_link = article_tag.find('a')
        
        if title_link:
            title = title_link.get_text(strip=True)
            url = title_link['href']
            print(f"  → Article: {title[:70]}...")
            
            try:
                r_art = requests.get(url, headers=HEADERS, timeout=15)
                soup_art = BeautifulSoup(r_art.text, 'html.parser')
                
                article_links = []
                for a in soup_art.find_all('a', href=True):
                    href = a['href']
                    if "get.php?username=" in href:
                        article_links.append(href)
                
                unique = list(set(article_links))
                print(f"  ✓ Extracted {len(unique)} playlist links")
                all_links.extend(unique)
                
            except Exception as e:
                print(f"  ✗ Article read error: {str(e)[:50]}")
    except Exception as e:
        print(f"  ✗ Scraping failed: {str(e)[:50]}")
        
    return all_links


def main():
    print("\n" + "="*70)
    print("  O.R CONTENT MANAGER - PROFESSIONAL EDITION")
    print("  Enhanced Validation | Retry Logic | Accurate Channel Counts")
    print("="*70 + "\n")
    
    session = create_session()
    
    # Load existing data
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"featured_content": []}
    
    existing_slots = data.get("featured_content", [])
    
    slot_registry = {}
    validated_domains = set()
    
    print(f"\n{'='*70}")
    print(f"  PHASE 1: VALIDATING EXISTING CONTENT ({len(existing_slots)} items)")
    print(f"{'='*70}\n")
    
    timestamp_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    valid_count = 0
    invalid_count = 0
    
    for idx, item in enumerate(existing_slots, 1):
        slot_id = item.get("slot_id")
        url = item.get("url")
        domain = get_domain(url)
        name = item.get("name")
        
        # Skip duplicates
        if domain in validated_domains:
            print(f"[{idx}/{len(existing_slots)}] SKIP: {name} - Duplicate domain")
            invalid_count += 1
            continue
        
        print(f"[{idx}/{len(existing_slots)}] TESTING: {name}")
        print(f"  Domain: {domain}")
        
        is_valid, reason, stream_count = validate_playlist(url, session)
        
        if is_valid:
            item["channel_count"] = stream_count
            item["last_validated"] = timestamp_now
            item["logo_url"] = get_logo_for_slot(slot_id)
            
            slot_registry[slot_id] = item
            validated_domains.add(domain)
            valid_count += 1
            print(f"  ✓ Status: {reason} | Channels: {stream_count:,}\n")
        else:
            invalid_count += 1
            print(f"  ✗ Status: {reason}\n")
    
    print(f"{'='*70}")
    print(f"  VALIDATION SUMMARY: {valid_count} Valid | {invalid_count} Invalid")
    print(f"{'='*70}\n")
    
    # Scraping phase
    print(f"\n{'='*70}")
    print(f"  PHASE 2: DISCOVERING NEW CONTENT")
    print(f"{'='*70}")
    
    new_links = []
    new_links.extend(scrape_nino())
    new_links.extend(scrape_iptvcodes())
    new_links.extend(scrape_m3umax())
    
    new_links = list(dict.fromkeys(new_links))
    
    print(f"\n{'='*70}")
    print(f"  DISCOVERY SUMMARY")
    print(f"{'='*70}")
    print(f"  Total Links Found: {len(new_links)}")
    print(f"  Existing Domains: {len(validated_domains)}")
    print(f"  Links to Test: {len([l for l in new_links if get_domain(l) not in validated_domains])}")
    print(f"{'='*70}\n")
    
    added_count = 0
    tested_count = 0
    
    for link in new_links:
        domain = get_domain(link)
        
        if domain in validated_domains:
            continue
        
        tested_count += 1
        print(f"[NEW {tested_count}] TESTING: {domain}")
        
        is_valid, reason, stream_count = validate_playlist(link, session)
        
        if is_valid:
            # Find available slot
            target_id = -1
            for i in range(len(GOT_SLOTS)):
                if i not in slot_registry:
                    target_id = i
                    break
            
            if target_id == -1:
                max_id = max(slot_registry.keys()) if slot_registry else -1
                target_id = max_id + 1
            
            name = get_name_for_slot(target_id, domain)
            logo = get_logo_for_slot(target_id)
            
            print(f"  ✓✓✓ SUCCESS! Adding to Slot {target_id}")
            print(f"  Name: {name}")
            print(f"  Channels: {stream_count:,}\n")
            
            new_entry = {
                "slot_id": target_id,
                "name": name,
                "domain": domain,
                "channel_count": stream_count,
                "logo_url": logo,
                "type": "m3u",
                "url": link,
                "server_url": None,
                "id": f"slot_{str(target_id).zfill(3)}",
                "last_changed": timestamp_now,
                "last_validated": timestamp_now,
                "change_log": f"Added via scraper on {timestamp_now}"
            }
            
            slot_registry[target_id] = new_entry
            validated_domains.add(domain)
            added_count += 1
        else:
            print(f"  ✗ Failed: {reason}\n")

    # Save
    final_list = [slot_registry[sid] for sid in sorted(slot_registry.keys())]
    data["featured_content"] = final_list
    
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    total_channels = sum(x['channel_count'] for x in final_list)
    
    print(f"\n{'='*70}")
    print(f"  FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"  Active Playlists: {len(final_list)}")
    print(f"  New Additions: {added_count}")
    print(f"  Total Channels: {total_channels:,}")
    print(f"  JSON Updated: {JSON_FILE}")
    print(f"{'='*70}\n")
    
    summary = f"Update Complete | Active: {len(final_list)} | Added: {added_count} | Channels: {total_channels:,}"
    log_to_file(summary)


if __name__ == "__main__":
    main()
