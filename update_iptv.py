import json
import requests
import datetime
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime as dt

# --- CONFIGURATION ---
JSON_FILE = "lovestory.json"
LOG_FILE = "scraper_log.json"  # Changed to JSON for better tracking
FALLBACK_ICON = "https://cdn.jsdelivr.net/gh/drnewske/tyhdsjax-nfhbqsm@main/logos/myicon.png"

# Sources
SOURCES = {
    "ninoiptv": "https://ninoiptv.com/",
    "iptvcodes": "https://www.iptvcodes.online/",
    "m3umax": "https://m3umax.blogspot.com/"
}

# Validation settings
MIN_PLAYLIST_SIZE = 500
MIN_STREAM_COUNT = 10
PLAYLIST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_DELAY = 2

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

# GOT_SLOTS - Full list
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
    {"name": "The Greyjoys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f7/House-Greyjoy-Main-Shield.png/revision/latest?cb=20170321185051"},
    {"name": "The Boltons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c8/House-Bolton-Main-Shield.png/revision/latest?cb=20170321185256"},
    {"name": "The Freys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0d/House-Frey-Main-Shield.png/revision/latest?cb=20170321185357"},
    {"name": "The Tullys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/House-Tully-Main-Shield.png/revision/latest?cb=20170321185449"},
    {"name": "The Arryns", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2a/House-Arryn-Main-Shield.png/revision/latest?cb=20170321185539"},
    {"name": "The Martells", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f0/House-Martell-Main-Shield.png/revision/latest?cb=20170321185631"},
    {"name": "The Tyrells", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e4/House-Tyrell-Main-Shield.png/revision/latest?cb=20170321185722"},
    {"name": "The Baratheons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/House-Baratheon-Main-Shield.png/revision/latest?cb=20170321185807"},
    {"name": "The Mormonts", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/31/House-Mormont-Main-Shield.png/revision/latest?cb=20170321185851"},
    {"name": "The Umbers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9b/House-Umber-Main-Shield.png/revision/latest?cb=20170321185943"},
    {"name": "The Karstarks", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/80/House-Karstark-Main-Shield.png/revision/latest?cb=20170321190034"},
    {"name": "The Reeds", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ef/House-Reed-Main-Shield.png/revision/latest?cb=20170321190122"},
    {"name": "The Manderlys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/67/House-Manderly-Main-Shield.png/revision/latest?cb=20170321190212"},
    {"name": "The Glovers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5a/House-Glover-Main-Shield.png/revision/latest?cb=20170321190302"},
    {"name": "The Tarlys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5d/House-Tarly-Main-Shield.png/revision/latest?cb=20170321190347"},
    {"name": "The Blackwoods", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/21/House-Blackwood-Main-Shield.png/revision/latest?cb=20170321190435"},
    {"name": "The Royces", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a6/House-Royce-Main-Shield.png/revision/latest?cb=20170321190521"},
    {"name": "The Daynes", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/87/House-Dayne-Main-Shield.png/revision/latest?cb=20170321190606"},
    {"name": "The Hightowers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/92/House-Hightower-Main-Shield.png/revision/latest?cb=20170321190649"},
    {"name": "The Redwynes", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/ac/House-Redwyne-Main-Shield.png/revision/latest?cb=20170321190733"},
    {"name": "The Florents", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c5/House-Florent-Main-Shield.png/revision/latest?cb=20170321190817"},
    {"name": "The Conningtons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/91/House-Connington-Main-Shield.png/revision/latest?cb=20170321190903"},
    {"name": "The Yronwoods", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/55/House-Yronwood-Main-Shield.png/revision/latest?cb=20170321190948"},
    {"name": "The Brotherhood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/47/Brotherhood-without-banners-coat.png/revision/latest?cb=20170321191036"},
    {"name": "The Second Sons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/Second_Sons_Banner.jpg/revision/latest?cb=20170321191123"},
    {"name": "The Golden Company", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c9/Golden_Company_S8.png/revision/latest?cb=20190414034906"},
    {"name": "The Faith Militant", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d9/Faith_Militant.png/revision/latest?cb=20170321191211"},
    {"name": "The Brave Companions", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d3/Brave_Companions.png/revision/latest?cb=20170321191257"},
    {"name": "The Burned Men", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/8e/Burned_Men.png/revision/latest?cb=20170321191343"},
    {"name": "The Stone Crows", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5f/Stone_Crows.png/revision/latest?cb=20170321191428"},
    {"name": "The Moon Brothers", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/63/Moon_Brothers.png/revision/latest?cb=20170321191513"},
    {"name": "The Black Ears", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a7/Black_Ears.png/revision/latest?cb=20170321191558"},
    {"name": "The Painted Dogs", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1f/Painted_Dogs.png/revision/latest?cb=20170321191643"},
    {"name": "The Sons of the Harpy", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/68/Sons_of_the_Harpy.png/revision/latest?cb=20170321191728"},
    {"name": "The Wise Masters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c1/Wise_Masters.png/revision/latest?cb=20170321191813"},
    {"name": "The Good Masters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f8/Good_Masters.png/revision/latest?cb=20170321191858"},
    {"name": "The Great Masters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e8/Great_Masters.png/revision/latest?cb=20170321191943"},
    {"name": "The Thirteen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3c/The_Thirteen.png/revision/latest?cb=20170321192028"},
    {"name": "The Warlocks", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/dd/Warlocks.png/revision/latest?cb=20170321192113"},
    {"name": "The Alchemists", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5e/Alchemists.png/revision/latest?cb=20170321192158"},
    {"name": "The Maesters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a3/Maesters.png/revision/latest?cb=20170321192243"},
    {"name": "The Septons", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/69/Septons.png/revision/latest?cb=20170321192328"},
    {"name": "The Silent Sisters", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/cf/Silent_Sisters.png/revision/latest?cb=20170321192413"},
    {"name": "The Maesters' Chain", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/81/Maesters_Chain.png/revision/latest?cb=20170321192458"},
    {"name": "The Kingsroad", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7c/Kingsroad.png/revision/latest?cb=20170321192543"},
    {"name": "The Neck", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/96/The_Neck.png/revision/latest?cb=20170321192628"},
    {"name": "The Twins", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e9/The_Twins.jpg/revision/latest?cb=20170321192713"},
    {"name": "Moat Cailin", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d0/Moat_Cailin.png/revision/latest?cb=20170321192758"},
    {"name": "The Dreadfort", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2c/Dreadfort.png/revision/latest?cb=20170321192843"},
    {"name": "Last Hearth", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a9/Last_Hearth.png/revision/latest?cb=20170321192928"},
    {"name": "Karhold", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/64/Karhold.png/revision/latest?cb=20170321193013"},
    {"name": "The Citadel Library", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1f/Citadel_Library.png/revision/latest?cb=20170321193058"},
    {"name": "The House of Black and White", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c4/House_of_Black_and_White.jpg/revision/latest?cb=20170321193143"},
    {"name": "The Great Pyramid", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/8c/Great_Pyramid.png/revision/latest?cb=20170321193228"},
    {"name": "The Fighting Pits", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ed/Fighting_Pits.png/revision/latest?cb=20170321193313"},
    {"name": "The Temple of the Dosh Khaleen", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f7/Temple_of_Dosh_Khaleen.png/revision/latest?cb=20170321193358"},
    {"name": "Vaes Dothrak", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a2/Vaes_Dothrak.png/revision/latest?cb=20170321193443"},
    {"name": "The Shadow Lands", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/df/Shadow_Lands.png/revision/latest?cb=20170321193528"},
    {"name": "Asshai", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c9/Asshai.png/revision/latest?cb=20170321193613"},
    {"name": "The Fist of the First Men", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5d/Fist_of_First_Men.png/revision/latest?cb=20170321193658"},
    {"name": "Craster's Keep", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/Crasters_Keep.png/revision/latest?cb=20170321193743"},
    {"name": "The Cave of the Three-Eyed Raven", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/29/Cave_Three_Eyed_Raven.png/revision/latest?cb=20170321193828"},
    {"name": "The Land of Always Winter", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/78/Land_Always_Winter.png/revision/latest?cb=20170321193913"},
    {"name": "The God's Eye", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1e/Gods_Eye.png/revision/latest?cb=20170321193958"},
    {"name": "The Isle of Faces", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3f/Isle_of_Faces.png/revision/latest?cb=20170321194043"},
    {"name": "Greywater Watch", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d5/Greywater_Watch.png/revision/latest?cb=20170321194128"},
    {"name": "The Saltpans", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/87/Saltpans.png/revision/latest?cb=20170321194213"},
    {"name": "Seagard", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2d/Seagard.png/revision/latest?cb=20170321194258"},
    {"name": "Maidenpool", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f4/Maidenpool.png/revision/latest?cb=20170321194343"},
    {"name": "Darry", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a8/Darry.png/revision/latest?cb=20170321194428"},
    {"name": "Raventree Hall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/36/Raventree_Hall.png/revision/latest?cb=20170321194513"},
    {"name": "Stone Hedge", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e5/Stone_Hedge.png/revision/latest?cb=20170321194558"},
    {"name": "Acorn Hall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/72/Acorn_Hall.png/revision/latest?cb=20170321194643"},
    {"name": "The Inn at the Crossroads", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1c/Inn_Crossroads.png/revision/latest?cb=20170321194728"},
    {"name": "The Peach", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/dc/The_Peach.png/revision/latest?cb=20170321194813"},
    {"name": "Griffin's Roost", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/aa/Griffins_Roost.png/revision/latest?cb=20170321194858"},
    {"name": "Rain House", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/6f/Rain_House.png/revision/latest?cb=20170321194943"},
    {"name": "Bronzegate", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4e/Bronzegate.png/revision/latest?cb=20170321195028"},
    {"name": "Greenstone", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c7/Greenstone.png/revision/latest?cb=20170321195113"},
    {"name": "Tarth", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/9/9f/Tarth.png/revision/latest?cb=20170321195158"},
    {"name": "The Sapphire Isle", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2c/Sapphire_Isle.png/revision/latest?cb=20170321195243"},
    {"name": "The Arbor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5d/The_Arbor.png/revision/latest?cb=20170321195328"},
    {"name": "Horn Hill", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/31/Horn_Hill.png/revision/latest?cb=20170321195413"},
    {"name": "Brightwater Keep", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a1/Brightwater_Keep.png/revision/latest?cb=20170321195458"},
    {"name": "Bitterbridge", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7f/Bitterbridge.png/revision/latest?cb=20170321195543"},
    {"name": "Longtable", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/eb/Longtable.png/revision/latest?cb=20170321195628"},
    {"name": "Cider Hall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/29/Cider_Hall.png/revision/latest?cb=20170321195713"},
    {"name": "Ashford", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d0/Ashford.png/revision/latest?cb=20170321195758"},
    {"name": "The Water Gardens", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/4/4c/Water_Gardens.png/revision/latest?cb=20170321195843"},
    {"name": "Yronwood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/89/Yronwood.png/revision/latest?cb=20170321195928"},
    {"name": "Starfall", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/0/0e/Starfall.png/revision/latest?cb=20170321200013"},
    {"name": "High Hermitage", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f7/High_Hermitage.png/revision/latest?cb=20170321200058"},
    {"name": "Skyreach", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/ce/Skyreach.png/revision/latest?cb=20170321200143"},
    {"name": "Vaith", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a5/Vaith.png/revision/latest?cb=20170321200228"},
    {"name": "Godsgrace", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/1/1f/Godsgrace.png/revision/latest?cb=20170321200313"},
    {"name": "Lemonwood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3c/Lemonwood.png/revision/latest?cb=20170321200358"},
    {"name": "Spottswood", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/e7/Spottswood.png/revision/latest?cb=20170321200443"},
    {"name": "Salt Shore", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d9/Salt_Shore.png/revision/latest?cb=20170321200528"},
    {"name": "The Tor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f0/The_Tor.png/revision/latest?cb=20170321200613"},
    {"name": "Ghost Hill", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2c/Ghost_Hill.png/revision/latest?cb=20170321200658"},
    {"name": "Hellholt", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/Hellholt.png/revision/latest?cb=20170321200743"},
    {"name": "The Boneway", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/6/67/Boneway.png/revision/latest?cb=20170321200828"},
    {"name": "The Prince's Pass", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3f/Princes_Pass.png/revision/latest?cb=20170321200913"},
    {"name": "The Red Mountains", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ec/Red_Mountains.png/revision/latest?cb=20170321200958"},
    {"name": "The Tower of Joy", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/7c/Tower_of_Joy.png/revision/latest?cb=20170321201043"},
    {"name": "The Stepstones", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d4/Stepstones.png/revision/latest?cb=20170321201128"},
    {"name": "The Summer Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f5/Summer_Sea.png/revision/latest?cb=20170321201213"},
    {"name": "The Smoking Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/8/86/Smoking_Sea.png/revision/latest?cb=20170321201258"},
    {"name": "The Shivering Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5a/Shivering_Sea.png/revision/latest?cb=20170321201343"},
    {"name": "The Sunset Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/39/Sunset_Sea.png/revision/latest?cb=20170321201428"},
    {"name": "The Jade Sea", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/e/ea/Jade_Sea.png/revision/latest?cb=20170321201513"},
    {"name": "Yi Ti", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c0/Yi_Ti.png/revision/latest?cb=20170321201558"},
    {"name": "Leng", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f8/Leng.png/revision/latest?cb=20170321201643"},
    {"name": "Qarth (City)", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/d/d7/Qarth_City.png/revision/latest?cb=20170321201728"},
    {"name": "New Ghis", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a7/New_Ghis.png/revision/latest?cb=20170321201813"},
    {"name": "Volantis", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/2/2e/Volantis.png/revision/latest?cb=20170321201858"},
    {"name": "Pentos", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/f/f4/Pentos.png/revision/latest?cb=20170321201943"},
    {"name": "Myr", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/5/5e/Myr.png/revision/latest?cb=20170321202028"},
    {"name": "Lys", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a0/Lys.png/revision/latest?cb=20170321202113"},
    {"name": "Tyrosh", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/b/b2/Tyrosh.png/revision/latest?cb=20170321202158"},
    {"name": "Norvos", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/3/3d/Norvos.png/revision/latest?cb=20170321202243"},
    {"name": "Qohor", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/c/c5/Qohor.png/revision/latest?cb=20170321202328"},
    {"name": "Lorath", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/7/79/Lorath.png/revision/latest?cb=20170321202413"},
    {"name": "Yunkai", "logo": "https://static.wikia.nocookie.net/gameofthrones/images/a/a9/Yunkai.png/revision/latest?cb=20170321202458"}
]


# --- HELPER FUNCTIONS ---

def create_session():
    """Create a requests session with retry logic"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_DELAY,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_domain(url):
    """Extract domain from URL"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]
        return domain.replace('www.', '')
    except:
        return "unknown"


def extract_m3u_links(html_content):
    """Extract M3U playlist URLs from HTML content"""
    links = []
    
    # Pattern 1: Direct http/https URLs ending with common IPTV parameters
    pattern1 = r'https?://[^\s<>"]+?(?:get\.php|player_api\.php)[^\s<>"]*'
    
    # Pattern 2: M3U file URLs
    pattern2 = r'https?://[^\s<>"]+?\.m3u[^\s<>"]*'
    
    # Pattern 3: URLs with username and password parameters
    pattern3 = r'https?://[^\s<>"]+?(?:username|user)=[^\s<>"&]+(?:&|&amp;)(?:password|pass)=[^\s<>"&]+'
    
    for pattern in [pattern1, pattern2, pattern3]:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        links.extend(matches)
    
    # Clean up HTML entities and duplicates
    cleaned_links = []
    for link in links:
        # Decode HTML entities
        link = link.replace('&amp;', '&')
        # Remove trailing punctuation
        link = re.sub(r'[.,;:)\]]+$', '', link)
        if link not in cleaned_links:
            cleaned_links.append(link)
    
    return cleaned_links


def extract_date_from_title(title):
    """Extract date from article title or URL (e.g., '26-01-2026')"""
    # Common date patterns
    patterns = [
        r'(\d{2})[-/.](\d{2})[-/.](\d{4})',  # DD-MM-YYYY
        r'(\d{4})[-/.](\d{2})[-/.](\d{2})',  # YYYY-MM-DD
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title)
        if match:
            try:
                g1, g2, g3 = match.groups()
                # Determine basic format based on year position (4 digits)
                if len(g1) == 4:
                    # YYYY-MM-DD
                    return dt.strptime(f"{g1}-{g2}-{g3}", "%Y-%m-%d")
                else:
                    # DD-MM-YYYY
                    return dt.strptime(f"{g3}-{g2}-{g1}", "%Y-%m-%d")
            except:
                continue
                
    return None


def extract_version_from_title(title):
    """Extract version (V1, V2, etc.) from article title"""
    match = re.search(r'V(\d+)', title, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 1  # Default to V1 if no version specified


def validate_playlist(url, session):
    """Validate if playlist is live and count channels"""
    try:
        response = session.get(url, timeout=PLAYLIST_TIMEOUT, headers=HEADERS, allow_redirects=True)
        
        # Check response size
        content_length = len(response.content)
        if content_length < MIN_PLAYLIST_SIZE:
            return False, f"Too small ({content_length} bytes)", 0
        
        # Count streams in M3U
        content = response.text
        stream_count = content.count('#EXTINF')
        
        if stream_count < MIN_STREAM_COUNT:
            return False, f"Too few streams ({stream_count})", stream_count
        
        return True, "Valid", stream_count
        
    except requests.Timeout:
        return False, "Timeout", 0
    except Exception as e:
        return False, f"Error: {str(e)[:50]}", 0


def load_scraper_log():
    """Load scraping history"""
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"sources": {}}


def save_scraper_log(log_data):
    """Save scraping history"""
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False)


def get_name_for_slot(slot_id, domain):
    """Get name for slot from GOT_SLOTS or generate Untitled name"""
    if slot_id < len(GOT_SLOTS):
        return GOT_SLOTS[slot_id]["name"]
    # For slots beyond GOT_SLOTS, use Untitled
    untitled_number = slot_id - len(GOT_SLOTS) + 1
    return f"Untitled {untitled_number}"


def get_logo_for_slot(slot_id):
    """Get logo for slot from GOT_SLOTS or use fallback"""
    if slot_id < len(GOT_SLOTS):
        return GOT_SLOTS[slot_id]["logo"]
    return FALLBACK_ICON


def find_available_slot(slot_registry):
    """
    Find an available slot ID, prioritizing:
    1. Empty slots within GOT_SLOTS range (for named slots)
    2. Next available slot after all existing slots
    """
    # First, try to find an empty slot within GOT_SLOTS range
    for i in range(len(GOT_SLOTS)):
        if i not in slot_registry:
            return i, "named"
    
    # If all GOT_SLOTS are taken, find the next available slot
    max_id = max(slot_registry.keys()) if slot_registry else -1
    return max_id + 1, "untitled"


def rename_untitled_playlists(slot_registry):
    """
    Find Untitled playlists and move them to available named slots if possible
    Returns the number of playlists renamed
    """
    renamed_count = 0
    
    # Find all untitled playlists
    untitled_playlists = []
    for slot_id, item in slot_registry.items():
        if item.get("name", "").startswith("Untitled "):
            untitled_playlists.append((slot_id, item))
    
    if not untitled_playlists:
        return 0
    
    # Find available named slots
    available_named_slots = []
    for i in range(len(GOT_SLOTS)):
        if i not in slot_registry:
            available_named_slots.append(i)
    
    if not available_named_slots:
        return 0
    
    print(f"\n[RENAMING] Found {len(untitled_playlists)} Untitled playlists and {len(available_named_slots)} available named slots")
    
    # Move untitled playlists to named slots
    for i, (old_slot_id, item) in enumerate(untitled_playlists):
        if i >= len(available_named_slots):
            break
        
        new_slot_id = available_named_slots[i]
        
        # Update the item
        old_name = item["name"]
        new_name = GOT_SLOTS[new_slot_id]["name"]
        new_logo = GOT_SLOTS[new_slot_id]["logo"]
        
        item["slot_id"] = new_slot_id
        item["name"] = new_name
        item["logo_url"] = new_logo
        item["id"] = f"slot_{str(new_slot_id).zfill(3)}"
        item["last_changed"] = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        item["change_log"] = f"Renamed from '{old_name}' to '{new_name}' on {item['last_changed']}"
        
        # Move in registry
        del slot_registry[old_slot_id]
        slot_registry[new_slot_id] = item
        
        renamed_count += 1
        print(f"  ✓ Renamed: '{old_name}' → '{new_name}' (Slot {old_slot_id} → {new_slot_id})")
    
    return renamed_count


# --- SCRAPER FUNCTIONS ---

def scrape_ninoiptv(session, log_data):
    """Scrape ninoiptv.com with date and version tracking"""
    print("\n[SCRAPING] ninoiptv.com...")
    
    source_name = "ninoiptv"
    source_log = log_data["sources"].get(source_name, {"scraped_articles": {}})
    
    try:
        response = session.get(SOURCES["ninoiptv"], headers=HEADERS, timeout=15)
        print(f"  Response status: {response.status_code}")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Debug: Check what we're finding
        all_articles = soup.find_all('article')
        print(f"  DEBUG: Found {len(all_articles)} <article> tags")
        
        # Find all article titles and links
        articles = []
        for idx, article in enumerate(all_articles):
            # Try multiple ways to find the title
            title_elem = article.find('h2', class_='entry-title')
            if not title_elem:
                title_elem = article.find('h1', class_='entry-title')
            if not title_elem:
                # Try without class
                title_elem = article.find('h2')
            if not title_elem:
                title_elem = article.find('h1')
            
            if title_elem:
                link_elem = title_elem.find('a')
                if link_elem:
                    title = link_elem.get_text(strip=True)
                    url = link_elem.get('href')
                    
                    print(f"  DEBUG Article {idx+1}: {title[:80]}")
                    
                    # Try extracting date from title first, then URL
                    article_date = extract_date_from_title(title)
                    if not article_date:
                        article_date = extract_date_from_title(url)
                    
                    version = extract_version_from_title(title)
                    
                    if article_date:
                        articles.append({
                            'title': title,
                            'url': url,
                            'date': article_date,
                            'version': version
                        })
                        print(f"    → Date: {article_date.strftime('%Y-%m-%d')}, Version: V{version}")
                    else:
                        print(f"    → No date found in title or URL")
                else:
                    print(f"  DEBUG Article {idx+1}: No link found in title")
            else:
                print(f"  DEBUG Article {idx+1}: No title element found")
        
        # Sort by date (newest first) then by version (highest first)
        articles.sort(key=lambda x: (x['date'], x['version']), reverse=True)
        
        print(f"\n  Found {len(articles)} dated articles")
        
        # Get the latest date
        if not articles:
            print("  ERROR: No articles with dates found!")
            return []
        
        latest_date = articles[0]['date']
        latest_date_str = latest_date.strftime("%Y-%m-%d")
        
        # Get all versions for the latest date
        latest_articles = [a for a in articles if a['date'] == latest_date]
        
        print(f"  Latest date: {latest_date_str}")
        versions_str = ', '.join([f'V{a["version"]}' for a in latest_articles])
        print(f"  Versions available: {versions_str}")
        
        all_links = []
        
        for article in latest_articles:
            article_key = f"{latest_date_str}_V{article['version']}"
            
            # Check if already scraped
            if article_key in source_log["scraped_articles"]:
                print(f"  ✓ Already scraped: {article['title']}")
                continue
            
            print(f"  → Scraping: {article['title']}")
            
            try:
                article_response = session.get(article['url'], headers=HEADERS, timeout=15)
                article_soup = BeautifulSoup(article_response.content, 'html.parser')
                
                # Find entry content
                content = article_soup.find('div', class_='entry-content')
                if content:
                    html_text = str(content)
                    links = extract_m3u_links(html_text)
                    
                    print(f"    Found {len(links)} links")
                    all_links.extend(links)
                    
                    # Mark as scraped
                    source_log["scraped_articles"][article_key] = {
                        "title": article['title'],
                        "url": article['url'],
                        "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "links_found": len(links)
                    }
                else:
                    print(f"    ERROR: No entry-content div found")
                    
            except Exception as e:
                print(f"    ERROR scraping article: {type(e).__name__}: {str(e)}")
        
        # Update log
        log_data["sources"][source_name] = source_log
        
        print(f"  Total links extracted: {len(all_links)}")
        return all_links
        
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return []


def scrape_iptvcodes(session, log_data):
    """Scrape iptvcodes.online"""
    print("\n[SCRAPING] iptvcodes.online...")
    
    source_name = "iptvcodes"
    source_log = log_data["sources"].get(source_name, {"scraped_articles": {}})
    
    try:
        response = session.get(SOURCES["iptvcodes"], headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find article links - only latest 1
        article_links = []
        for article in soup.find_all('article')[:1]:  # Only first/latest article
            link_elem = article.find('a', href=True)
            if link_elem:
                url = link_elem['href']
                title = link_elem.get_text(strip=True)
                article_links.append({'title': title, 'url': url})
        
        print(f"  Found {len(article_links)} recent articles")
        
        all_links = []
        
        for article in article_links:
            article_key = article['url']
            
            if article_key in source_log["scraped_articles"]:
                print(f"  ✓ Already scraped: {article['title'][:50]}...")
                continue
            
            print(f"  → Scraping: {article['title'][:50]}...")
            
            try:
                article_response = session.get(article['url'], headers=HEADERS, timeout=15)
                article_html = article_response.text
                
                links = extract_m3u_links(article_html)
                print(f"    Found {len(links)} links")
                all_links.extend(links)
                
                # Mark as scraped
                source_log["scraped_articles"][article_key] = {
                    "title": article['title'],
                    "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "links_found": len(links)
                }
                
            except Exception as e:
                print(f"    Error: {str(e)}")
        
        # Update log
        log_data["sources"][source_name] = source_log
        
        print(f"  Total links extracted: {len(all_links)}")
        return all_links
        
    except Exception as e:
        print(f"  Error: {str(e)}")
        return []


def scrape_m3umax(session, log_data):
    """Scrape m3umax.blogspot.com"""
    print("\n[SCRAPING] m3umax.blogspot.com...")
    
    source_name = "m3umax"
    source_log = log_data["sources"].get(source_name, {"scraped_articles": {}})
    
    try:
        response = session.get(SOURCES["m3umax"], headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find article links - only latest 1
        article_links = []
        for article in soup.find_all('h3', class_='post-title')[:1]:  # Only first/latest article
            link_elem = article.find('a', href=True)
            if link_elem:
                url = link_elem['href']
                title = link_elem.get_text(strip=True)
                article_links.append({'title': title, 'url': url})
        
        print(f"  Found {len(article_links)} recent articles")
        
        all_links = []
        
        for article in article_links:
            article_key = article['url']
            
            if article_key in source_log["scraped_articles"]:
                print(f"  ✓ Already scraped: {article['title'][:50]}...")
                continue
            
            print(f"  → Scraping: {article['title'][:50]}...")
            
            try:
                article_response = session.get(article['url'], headers=HEADERS, timeout=15)
                article_html = article_response.text
                
                links = extract_m3u_links(article_html)
                print(f"    Found {len(links)} links")
                all_links.extend(links)
                
                # Mark as scraped
                source_log["scraped_articles"][article_key] = {
                    "title": article['title'],
                    "scraped_at": dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "links_found": len(links)
                }
                
            except Exception as e:
                print(f"    Error: {str(e)}")
        
        # Update log
        log_data["sources"][source_name] = source_log
        
        print(f"  Total links extracted: {len(all_links)}")
        return all_links
        
    except Exception as e:
        print(f"  Error: {str(e)}")
        return []


# --- MAIN FUNCTION ---

def main():
    print("="*70)
    print("  IPTV PLAYLIST UPDATER")
    print("="*70)
    
    session = create_session()
    
    # Load existing data
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"featured_content": []}
    
    # Load scraper log
    log_data = load_scraper_log()
    
    existing_slots = data.get("featured_content", [])
    slot_registry = {item["slot_id"]: item for item in existing_slots}
    validated_domains = set()
    
    # --- DISCOVERY PHASE (SCRAPE NEW) ---
    print("\n" + "="*70)
    print("  DISCOVERY PHASE")
    print("="*70)
    
    new_links = []
    new_links.extend(scrape_ninoiptv(session, log_data))
    new_links.extend(scrape_iptvcodes(session, log_data))
    new_links.extend(scrape_m3umax(session, log_data))
    
    # Save updated log
    save_scraper_log(log_data)
    
    # Remove duplicates
    new_links = list(dict.fromkeys(new_links))
    
    print(f"\n[TESTING] Found {len(new_links)} unique new links")
    
    added_count = 0
    tested_count = 0
    
    # Process New Links
    for link in new_links:
        domain = get_domain(link)
        if domain in validated_domains:
            continue
        
        tested_count += 1
        print(f"[NEW {tested_count}/{len(new_links)}] TESTING: {domain}")
        
        is_valid, reason, stream_count = validate_playlist(link, session)
        
        if is_valid:
            # Find available slot (prioritizes named slots)
            target_id, slot_type = find_available_slot(slot_registry)
            
            name = get_name_for_slot(target_id, domain)
            logo = get_logo_for_slot(target_id)
            
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
            
            slot_info = f"Slot {target_id} ({slot_type})"
            print(f"  ✓✓✓ SUCCESS! Added to {slot_info} | Channels: {stream_count:,}\n")
        else:
            print(f"  ✗ Failed: {reason}\n")
    
    # Rename untitled playlists if there are available named slots
    renamed_count = rename_untitled_playlists(slot_registry)

    # --- VALIDATION PHASE (EXISTING) ---
    print("\n" + "="*70)
    print("  VALIDATION PHASE (EXISTING)")
    print("="*70)
    print(f"\n[VALIDATION] Checking {len(existing_slots)} existing playlists...")
    
    timestamp_now = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    valid_count = 0
    invalid_count = 0
    
    # We iterate over a COPY because we might unknowingly modify slot_registry structure if we were doing deletions (we aren't here, but safety first)
    # Actually, we are just updating metadata in place.
    
    for idx, item in enumerate(existing_slots, 1):
        slot_id = item.get("slot_id")
        url = item.get("url")
        domain = get_domain(url)
        name = item.get("name")
        
        # If we just added this domain in the discovery phase, no need to re-validate immediately
        # But discovery adds to 'slot_registry' and 'validated_domains', while 'existing_slots' is the old list.
        # So we check against validated_domains.
        
        if domain in validated_domains:
            print(f"[{idx}/{len(existing_slots)}] SKIP: {name} - Updated/Checked recently")
            # Update the existing record in registry with the one that might have been just added/updated?
            # Actually, if it's in existing_slots, it's an old one.
            # If it's in validated_domains, it implies we either just added it OR we already processed it.
            # But wait, we haven't processed existing_slots yet. 
            # So if it is in validated_domains, it MUST be because we just added a DUPLICATE of an existing one in the discovery phase?
            # If so, we should probably ensure the 'existing' one is the one kept or updated.
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

    # Finalize List
    final_list = [slot_registry[sid] for sid in sorted(slot_registry.keys())]
    data["featured_content"] = final_list
    
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    total_channels = sum(x['channel_count'] for x in final_list)
    
    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    print(f"  Active Playlists: {len(final_list)}")
    print(f"  Newly Added: {added_count}")
    if renamed_count > 0:
        print(f"  Renamed (Untitled → Named): {renamed_count}")
    print(f"  Total Channels: {total_channels:,}")
    print(f"  Updated: {timestamp_now}")
    print("="*70)


if __name__ == "__main__":
    main()
